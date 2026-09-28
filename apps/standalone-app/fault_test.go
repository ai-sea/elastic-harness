package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	kvsqlite "github.com/ai-sea/elastic-harness/adapters/kv-sqlite"
	dispatcher "github.com/ai-sea/elastic-harness/components/effect-dispatcher"
	registry "github.com/ai-sea/elastic-harness/components/handler-registry"
	definition "github.com/ai-sea/elastic-harness/components/harness-definition"
	runtime "github.com/ai-sea/elastic-harness/components/onstate-runtime"
	reconciler "github.com/ai-sea/elastic-harness/components/timer-reconciler"
	toolhandler "github.com/ai-sea/elastic-harness/components/tool-handler"
	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// 本文件覆盖 §17 / phase1-tasks.md P6 的端到端故障注入场景：
// 重复消息、Worker 中断、丢失回调、完成/超时竞争、取消/成功竞争与崩溃恢复。
// 与 e2e_test.go 的 happy path 不同，这里绕过 HTTP 层直接驱动
// 存储/内核/派发器/Reconciler，以便确定性注入故障。

// 重复消息：同一 dedupeKey 的 Signal 在 Inbox 入队阶段即被折叠（§5.3.1）。
func TestDuplicateSignalsAreFoldedAtInbox(t *testing.T) {
	fixture := newFaultFixture(t, openMemoryStore(t), oneStepDefinition(), succeedHandler())
	run := fixture.createRun(t, nil)

	inserted, err := fixture.store.PutSignal(context.Background(), cancelSignal(run.RunID), stateHint(run.RunID, "cancel-1"))
	if err != nil || !inserted {
		t.Fatalf("首次取消应被接受：inserted=%v err=%v", inserted, err)
	}
	inserted, err = fixture.store.PutSignal(context.Background(), cancelSignal(run.RunID), stateHint(run.RunID, "cancel-2"))
	if err != nil || inserted {
		t.Fatalf("重复取消必须被折叠：inserted=%v err=%v", inserted, err)
	}

	fixture.process(t, run.RunID)
	assertTerminal(t, fixture, run.RunID, domain.TerminalCancelled)
	if inbox := fixture.inbox(t, run.RunID); len(inbox) != 2 {
		t.Fatalf("Inbox 应只有 entered + 1 条取消（重复已折叠），实际 %d 条", len(inbox))
	}
	if steps := fixture.steps(t, run.RunID); len(steps) != 1 {
		t.Fatalf("折叠后只允许消费一次，步骤数 = %d", len(steps))
	}
}

// Worker 中断：Worker A 领取后在提交前崩溃（租约到期），Reconciler 补发提示，
// Worker B 完成迁移；A 迟到提交必须被 fencing token + stateVersion 双重 CAS 拒绝
// （不变量 #1，§11.1「Worker 执行中崩溃」）。
func TestWorkerCrashMidTransitionRecoversAndRejectsLateCommit(t *testing.T) {
	fixture := newFaultFixture(t, openMemoryStore(t), oneStepDefinition(), succeedHandler())
	run := fixture.createRun(t, nil)

	load, err := fixture.store.AcquireExecution(context.Background(), run.RunID, "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃：不提交、不释放，租约自然到期。
	time.Sleep(80 * time.Millisecond)

	reawakened, err := fixture.reconciler.ReawakenStalled(context.Background(), 10)
	if err != nil || reawakened != 1 {
		t.Fatalf("崩溃 Worker 的 Run 应被识别为停滞并重唤醒：%d/%v", reawakened, err)
	}
	fixture.processAs(t, run.RunID, "worker-b")
	assertTerminal(t, fixture, run.RunID, domain.TerminalCompleted)

	stale := load.Snapshot
	stale.StateVersion++
	commit := ports.TransitionCommit{
		ExpectedStateVersion: load.Snapshot.StateVersion, FencingToken: load.FencingToken,
		Snapshot: stale, SignalID: load.Signal.SignalID,
		Step: domain.Step{RunID: run.RunID, StepSeq: stale.StateVersion, Attempt: 1, State: load.Snapshot.CurrentState},
	}
	if err := fixture.store.CommitTransition(context.Background(), commit); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("崩溃 Worker 的迟到提交必须被 CAS 拒绝，得到：%v", err)
	}
	if steps := fixture.steps(t, run.RunID); len(steps) != 1 {
		t.Fatalf("迟到提交不得产生重复步骤，步骤数 = %d", len(steps))
	}
}

// 丢失回调：工具调用进入 WAITING 后回调永远不到达，
// Callback Timer 到期把 Run 收敛到 timed-out（§11.1「回调丢失」）。
func TestLostCallbackConvergesViaCallbackTimer(t *testing.T) {
	fixture := newWaitingFixture(t, 50*time.Millisecond)
	run := fixture.createRun(t, pendingToolCallContext(t))
	fixture.process(t, run.RunID)
	assertWaiting(t, fixture, run.RunID)

	time.Sleep(80 * time.Millisecond)
	fired, err := fixture.reconciler.FireDueTimers(context.Background(), 10)
	if err != nil || fired != 1 {
		t.Fatalf("到期 Callback Timer 应触发：%d/%v", fired, err)
	}
	fixture.process(t, run.RunID)
	assertTerminal(t, fixture, run.RunID, domain.TerminalTimedOut)
	if entries := fixture.effects(t, run.RunID); len(entries) != 1 || entries[0].Status != effects.EffectPending {
		t.Fatalf("回调丢失时 Effect 不得被重放，应保持 PENDING：%+v", entries)
	}
}

// 完成/超时竞争（完成先提交）：Effect 完成先落地，Run 离开等待态后
// Timer 因 stateEnterCounter 失配而作废（§8.7，先提交者定义事实）。
func TestCompletionBeatsCallbackTimeout(t *testing.T) {
	fixture := newWaitingFixture(t, 50*time.Millisecond)
	run := fixture.createRun(t, pendingToolCallContext(t))
	fixture.process(t, run.RunID)
	fixture.dispatchOnce(t)

	fixture.process(t, run.RunID)
	assertTerminal(t, fixture, run.RunID, domain.TerminalCompleted)

	time.Sleep(80 * time.Millisecond)
	fired, err := fixture.reconciler.FireDueTimers(context.Background(), 10)
	if err != nil || fired != 0 {
		t.Fatalf("Run 已离开等待态，过期 Timer 必须作废而非触发：fired=%d err=%v", fired, err)
	}
	if steps := fixture.steps(t, run.RunID); len(steps) != 2 {
		t.Fatalf("步骤数 = %d，期望 2（entered + effect.completed）", len(steps))
	}
}

// 完成/超时竞争（超时先提交）：timer.fired 先进入 Inbox 先被消费，
// Run 进入终态后迟到的 effect.completed 只记录、不生效（§5.3.1 乱序裁决）。
func TestCallbackTimeoutBeatsCompletion(t *testing.T) {
	fixture := newWaitingFixture(t, 50*time.Millisecond)
	run := fixture.createRun(t, pendingToolCallContext(t))
	fixture.process(t, run.RunID)

	time.Sleep(80 * time.Millisecond)
	fired, err := fixture.reconciler.FireDueTimers(context.Background(), 10)
	if err != nil || fired != 1 {
		t.Fatalf("到期 Callback Timer 应触发：%d/%v", fired, err)
	}
	// 确保完成 Signal 的 occurredAt 严格晚于 timer.fired，消除时钟精度歧义。
	time.Sleep(30 * time.Millisecond)
	fixture.dispatchOnce(t)

	fixture.process(t, run.RunID)
	assertTerminal(t, fixture, run.RunID, domain.TerminalTimedOut)

	err = fixture.engine.ProcessRun(context.Background(), run.RunID, "worker-1")
	if !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("终态 Run 不得再被推进，得到：%v", err)
	}
	if inbox := fixture.inbox(t, run.RunID); len(inbox) != 3 {
		t.Fatalf("迟到回调只记录不生效：Inbox 应有 entered+timer.fired+effect.completed 共 3 条，实际 %d", len(inbox))
	}
}

// 取消/成功竞争：cancel 优先级最高，先提交者胜；终态不可逆（不变量 #2/#10）。
func TestCancelBeatsCompletionAndTerminalIsFinal(t *testing.T) {
	fixture := newWaitingFixture(t, time.Minute)
	run := fixture.createRun(t, pendingToolCallContext(t))
	fixture.process(t, run.RunID)
	fixture.dispatchOnce(t) // effect.completed 已在 Inbox，但优先级低于取消

	inserted, err := fixture.store.PutSignal(context.Background(), cancelSignal(run.RunID), stateHint(run.RunID, "cancel"))
	if err != nil || !inserted {
		t.Fatalf("取消应被接受：inserted=%v err=%v", inserted, err)
	}
	fixture.process(t, run.RunID)
	assertTerminal(t, fixture, run.RunID, domain.TerminalCancelled)

	if _, err := fixture.store.PutSignal(context.Background(), cancelSignal(run.RunID), stateHint(run.RunID, "cancel-late")); !errors.Is(err, ports.ErrTerminal) {
		t.Fatalf("终态 Run 必须拒绝新 Signal（不变量 #10），得到：%v", err)
	}
	if steps := fixture.steps(t, run.RunID); len(steps) != 2 {
		t.Fatalf("取消后不得再消费任何 Signal，步骤数 = %d", len(steps))
	}
}

// 恢复：进程崩溃（SQLite 关闭）后重开同一数据库，Run 从持久化状态继续推进到终态。
func TestRunResumesAfterStoreReopen(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "harness.sqlite")
	first := newFaultFixture(t, openFileStore(t, databasePath), twoStepDefinition(), scriptedHandler{
		onState: func(execution ports.StateExecutionContext, _ effects.StateSignal) (effects.StateOutcome, error) {
			return effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "succeeded"}, nil
		},
	})
	run := first.createRun(t, nil)
	first.process(t, run.RunID) // 完成第一步，崩溃在第二步之前
	if err := first.store.Close(); err != nil {
		t.Fatal(err)
	}

	second := newFaultFixture(t, openFileStore(t, databasePath), twoStepDefinition(), scriptedHandler{
		onState: func(execution ports.StateExecutionContext, _ effects.StateSignal) (effects.StateOutcome, error) {
			return effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "succeeded"}, nil
		},
	})
	// 不挂在 t.Cleanup 上：openFileStore 不自动关闭，显式关闭以释放 Windows 文件锁。
	defer func() { _ = second.store.Close() }()
	second.process(t, run.RunID)
	assertTerminal(t, second, run.RunID, domain.TerminalCompleted)

	events, err := second.store.Events(context.Background(), run.RunID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for index, event := range events {
		if event.Sequence != int64(index+1) {
			t.Fatalf("崩溃恢复后事件序列必须严格单调（不变量 #5）：%+v", events)
		}
	}
}

// 自续跑预算收敛：自环定义在 MaxSteps 处被预算闸终止（不变量 #6）。
func TestSelfContinuationStopsAtStepBudget(t *testing.T) {
	fixture := newFaultFixture(t, openMemoryStore(t), selfLoopDefinition(), succeedHandler())
	snapshot, err := fixture.engine.CreateRun(context.Background(), runtime.CreateRunRequest{
		TenantID: "tenant-1", ChatID: "chat-1",
		Harness: domain.HarnessRef{ID: selfLoopDefinition().ID, Version: selfLoopDefinition().Version},
		Budget:  domain.Budget{MaxSteps: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.process(t, snapshot.RunID)
	fixture.process(t, snapshot.RunID)
	fixture.process(t, snapshot.RunID) // 第 3 次推进撞上预算闸

	assertTerminal(t, fixture, snapshot.RunID, domain.TerminalFailed)
	if steps := fixture.steps(t, snapshot.RunID); len(steps) != 3 {
		t.Fatalf("预算闸触发的那一步也必须落账，步骤数 = %d", len(steps))
	}
	run, err := fixture.store.GetRun(context.Background(), snapshot.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Budget.UsedSteps != 3 {
		t.Fatalf("预算消耗 = %d，期望 3", run.Budget.UsedSteps)
	}
}

// 不变量 #7：Run 创建时固定的 Handler 绑定在生命周期内不变——
// 即使注册中心随后出现了更新版本，进行中的 Run 仍按快照里的旧版本执行。
func TestPinnedBindingsDoNotFollowNewHandlerVersions(t *testing.T) {
	store := openMemoryStore(t)
	def := twoStepDefinition()
	definitions := definition.NewRepository()
	if err := definitions.PutDraft(context.Background(), def); err != nil {
		t.Fatal(err)
	}
	if _, err := definitions.Publish(context.Background(), def.ID, def.Version); err != nil {
		t.Fatal(err)
	}
	handlerRegistry, err := registry.New(registry.Options{
		Issuer: "fault-test", Audience: "handlers", TokenTTL: time.Minute, HeartbeatTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	capability := qname.MustParse("harness/test.step")
	registerVersion(t, handlerRegistry, capability, "1.0.0", succeedHandler())
	engine := runtime.New(store, definitions, handlerRegistry, runtime.Options{})
	snapshot, err := engine.CreateRun(context.Background(), runtime.CreateRunRequest{
		TenantID: "tenant-1", ChatID: "chat-1",
		Harness: domain.HarnessRef{ID: def.ID, Version: def.Version}, Budget: domain.Budget{MaxSteps: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Run 创建后才出现的新版本：若被它接管，Run 会走向 failed 而非 completed。
	registerVersion(t, handlerRegistry, capability, "2.0.0", scriptedHandler{
		onState: func(ports.StateExecutionContext, effects.StateSignal) (effects.StateOutcome, error) {
			return effects.StateOutcome{Kind: effects.OutcomeTerminalFailure, Result: "failed"}, nil
		},
	})

	if err := engine.ProcessRun(context.Background(), snapshot.RunID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	steps, err := store.Steps(context.Background(), snapshot.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Handler.HandlerVersion != "1.0.0" {
		t.Fatalf("进行中的 Run 必须沿用创建时固定的 1.0.0 绑定：%+v", steps)
	}
	if snapshot.ResolvedBindings["first"].HandlerVersion != "1.0.0" {
		t.Fatalf("快照绑定被改写：%+v", snapshot.ResolvedBindings)
	}
}

func registerVersion(t *testing.T, handlerRegistry *registry.Registry, capability qname.QName, version string, handler ports.StateHandler) {
	t.Helper()
	descriptor := ports.HandlerDescriptor{
		HandlerID: capability, Version: version, Deployment: "in-process",
		Capabilities: []ports.CapabilityDescriptor{{Capability: capability, Version: version}},
	}
	if _, err := handlerRegistry.Register(descriptor, handler, true); err != nil {
		t.Fatal(err)
	}
}

type faultFixture struct {
	store      *kvsqlite.Store
	engine     *runtime.Engine
	reconciler *reconciler.Reconciler
	dispatcher *dispatcher.Dispatcher
	definition domain.HarnessDefinition
}

func newFaultFixture(t *testing.T, store *kvsqlite.Store, def domain.HarnessDefinition, handler ports.StateHandler) *faultFixture {
	t.Helper()
	fixture := &faultFixture{store: store, definition: def}
	definitions := definition.NewRepository()
	if err := definitions.PutDraft(context.Background(), def); err != nil {
		t.Fatal(err)
	}
	if _, err := definitions.Publish(context.Background(), def.ID, def.Version); err != nil {
		t.Fatal(err)
	}
	handlerRegistry, err := registry.New(registry.Options{
		Issuer: "fault-test", Audience: "handlers", TokenTTL: time.Minute, HeartbeatTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range requiredCapabilities(def) {
		descriptor := ports.HandlerDescriptor{
			HandlerID: capability, Version: "1.0.0", Deployment: "in-process",
			Capabilities: []ports.CapabilityDescriptor{{Capability: capability, Version: "1.0.0"}},
		}
		if _, err := handlerRegistry.Register(descriptor, handler, true); err != nil {
			t.Fatal(err)
		}
	}
	fixture.engine = runtime.New(store, definitions, handlerRegistry, runtime.Options{LeaseDuration: 50 * time.Millisecond})
	fixture.reconciler = reconciler.New(store, runtime.RandomIDGenerator{}, nil)
	fixture.dispatcher = dispatcher.New(store, stubExecutor{}, "fault-dispatcher")
	return fixture
}

func newWaitingFixture(t *testing.T, callback time.Duration) *faultFixture {
	t.Helper()
	return newFaultFixture(t, openMemoryStore(t), waitToolDefinition(callback), toolhandler.New(stubToolCatalog{}, nil))
}

func requiredCapabilities(def domain.HarnessDefinition) []qname.QName {
	seen := map[qname.QName]bool{}
	var capabilities []qname.QName
	for _, node := range def.States {
		if node.Type == domain.StateTerminal || seen[node.Requires.Capability] {
			continue
		}
		seen[node.Requires.Capability] = true
		capabilities = append(capabilities, node.Requires.Capability)
	}
	return capabilities
}

func (f *faultFixture) createRun(t *testing.T, initialContext json.RawMessage) domain.RunSnapshot {
	t.Helper()
	snapshot, err := f.engine.CreateRun(context.Background(), runtime.CreateRunRequest{
		TenantID: "tenant-1", ChatID: "chat-1",
		Harness:        domain.HarnessRef{ID: f.definition.ID, Version: f.definition.Version},
		Budget:         domain.Budget{MaxSteps: 32},
		InitialContext: initialContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (f *faultFixture) process(t *testing.T, runID string) {
	t.Helper()
	f.processAs(t, runID, "worker-1")
}

func (f *faultFixture) processAs(t *testing.T, runID, workerID string) {
	t.Helper()
	if err := f.engine.ProcessRun(context.Background(), runID, workerID); err != nil {
		t.Fatal(err)
	}
}

func (f *faultFixture) dispatchOnce(t *testing.T) {
	t.Helper()
	dispatched, err := f.dispatcher.DispatchOnce(context.Background(), 10)
	if err != nil || dispatched != 1 {
		t.Fatalf("DispatchOnce = %d/%v，期望 1/nil", dispatched, err)
	}
}

func (f *faultFixture) steps(t *testing.T, runID string) []domain.Step {
	t.Helper()
	steps, err := f.store.Steps(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return steps
}

func (f *faultFixture) inbox(t *testing.T, runID string) []effects.StateSignal {
	t.Helper()
	signals, err := f.store.Inbox(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return signals
}

func (f *faultFixture) effects(t *testing.T, runID string) []effects.EffectLedgerEntry {
	t.Helper()
	entries, err := f.store.Effects(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func assertTerminal(t *testing.T, fixture *faultFixture, runID string, reason domain.TerminalReason) {
	t.Helper()
	run, err := fixture.store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.LifecycleStatus != domain.LifecycleTerminal {
		t.Fatalf("Run 应为终态，实际 %s", run.LifecycleStatus)
	}
	if run.TerminalReason == nil || *run.TerminalReason != reason {
		t.Fatalf("终态原因 = %v，期望 %s", run.TerminalReason, reason)
	}
}

func assertWaiting(t *testing.T, fixture *faultFixture, runID string) {
	t.Helper()
	run, err := fixture.store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.LifecycleStatus != domain.LifecycleWaiting {
		t.Fatalf("Run 应为 WAITING，实际 %s", run.LifecycleStatus)
	}
}

func openMemoryStore(t *testing.T) *kvsqlite.Store {
	t.Helper()
	store, err := kvsqlite.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func openFileStore(t *testing.T, path string) *kvsqlite.Store {
	t.Helper()
	store, err := kvsqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func pendingToolCallContext(t *testing.T) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"pendingToolCalls": []ports.ToolCall{{
		CallID: "call-1", Tool: qname.MustParse("harness/echo"), Arguments: json.RawMessage(`{"text":"hi"}`),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func cancelSignal(runID string) effects.StateSignal {
	return effects.StateSignal{
		SignalID: "sig-cancel-" + runID, RunID: runID, Type: qname.MustParse("harness/run.cancel.requested"),
		DedupeKey: runID + "/cancel", Priority: 1000, OccurredAt: time.Now().UTC(),
	}
}

func stateHint(runID, id string) ports.OutboxRecord {
	payload, _ := json.Marshal(map[string]string{"runId": runID})
	return ports.OutboxRecord{
		ID: "out-" + id + "-" + runID, Channel: "state", Key: runID, Payload: payload, CreatedAt: time.Now().UTC(),
	}
}

type scriptedHandler struct {
	onState func(ports.StateExecutionContext, effects.StateSignal) (effects.StateOutcome, error)
}

func (h scriptedHandler) OnState(_ context.Context, execution ports.StateExecutionContext, signal effects.StateSignal) (effects.StateOutcome, error) {
	return h.onState(execution, signal)
}

func succeedHandler() scriptedHandler {
	return scriptedHandler{onState: func(ports.StateExecutionContext, effects.StateSignal) (effects.StateOutcome, error) {
		return effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "succeeded"}, nil
	}}
}

type stubExecutor struct{}

type stubToolCatalog struct{}

func (stubToolCatalog) ResolveTool(context.Context, string, qname.QName) (ports.ToolRegistration, error) {
	return ports.ToolRegistration{
		RegistrationID: "tool-registration-1", Revision: 1,
		Name: qname.MustParse("harness/echo"), Endpoint: "http://tool.invalid",
	}, nil
}

func (stubExecutor) Execute(context.Context, effects.EffectLedgerEntry) (string, string, error) {
	return "ext-1", "artifact://result", nil
}

func (stubExecutor) Recover(context.Context, effects.EffectLedgerEntry) (string, string, bool, error) {
	return "", "", false, nil
}

func oneStepDefinition() domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "fault-one-step", Version: "1.0.0", Initial: "work",
		States: map[string]domain.StateNode{
			"work": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.step"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: 30 * time.Second},
				Transitions: map[string]string{"succeeded": "done"},
			},
			"done": {Type: domain.StateTerminal},
		},
	}
}

func twoStepDefinition() domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "fault-two-step", Version: "1.0.0", Initial: "first",
		States: map[string]domain.StateNode{
			"first": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.step"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: 30 * time.Second},
				Transitions: map[string]string{"succeeded": "second"},
			},
			"second": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.step"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: 30 * time.Second},
				Transitions: map[string]string{"succeeded": "done"},
			},
			"done": {Type: domain.StateTerminal},
		},
	}
}

// 自续跑循环必须经过 WAIT 状态，否则发布闸门会以「无界自动循环」拒绝——
// 预算闸（MaxSteps）正是这类循环的收敛保障（不变量 #6）。
func selfLoopDefinition() domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "fault-self-loop", Version: "1.0.0", Initial: "loop",
		States: map[string]domain.StateNode{
			"loop": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.step"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: 30 * time.Second},
				Transitions: map[string]string{"succeeded": "gate", "budgetExceeded": "failed"},
			},
			"gate": {
				Type: domain.StateWait, SideEffect: domain.SideEffectIdempotent,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.step"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: 30 * time.Second},
				Transitions: map[string]string{"succeeded": "loop", "budgetExceeded": "failed"},
			},
			"failed": {Type: domain.StateTerminal},
		},
	}
}

func waitToolDefinition(callback time.Duration) domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "fault-wait-tool", Version: "1.0.0", Initial: "wait-tool",
		States: map[string]domain.StateNode{
			"wait-tool": {
				Type: domain.StateWait, SideEffect: domain.SideEffectIdempotent,
				Requires: domain.RequirementSpec{Capability: qname.MustParse("harness/tools.invoke-all"), Version: "^1"},
				Timeouts: domain.Timeouts{StartToClose: 30 * time.Second, Callback: callback},
				Transitions: map[string]string{
					"succeeded": "done", "timedOut": "timed-out",
				},
			},
			"done":      {Type: domain.StateTerminal},
			"timed-out": {Type: domain.StateTerminal},
		},
	}
}
