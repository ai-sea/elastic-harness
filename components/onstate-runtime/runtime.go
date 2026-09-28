package runtime

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

var (
	ErrMissingBinding    = errors.New("当前状态缺少固定 Handler 绑定")
	ErrInvalidOutcome    = errors.New("Handler 返回值违反运行时契约")
	ErrMissingTransition = errors.New("Outcome 没有对应迁移规则")
)

type IDGenerator interface {
	New(prefix string) string
}

type RandomIDGenerator struct{}

func (RandomIDGenerator) New(prefix string) string {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("生成随机 ID：%v", err))
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value)
}

type Options struct {
	LeaseDuration time.Duration
	Now           func() time.Time
	IDs           IDGenerator
}

type Engine struct {
	store       ports.StateStore
	definitions ports.DefinitionRepository
	resolver    ports.HandlerResolver
	options     Options
}

func New(store ports.StateStore, definitions ports.DefinitionRepository, resolver ports.HandlerResolver, options Options) *Engine {
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = 30 * time.Second
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	if options.IDs == nil {
		options.IDs = RandomIDGenerator{}
	}
	return &Engine{store: store, definitions: definitions, resolver: resolver, options: options}
}

type CreateRunRequest struct {
	TenantID       string
	ChatID         string
	RunID          string
	Harness        domain.HarnessRef
	Profile        domain.ProfileRef
	Budget         domain.Budget
	InitialContext json.RawMessage
}

// CreateRun 在启动时解析并固定全部 Handler 绑定，同时写入权威 entered Signal。
func (e *Engine) CreateRun(ctx context.Context, request CreateRunRequest) (domain.RunSnapshot, error) {
	definition, err := e.definitions.Get(ctx, request.Harness.ID, request.Harness.Version)
	if err != nil {
		return domain.RunSnapshot{}, err
	}
	bindings := make(map[string]domain.HandlerBinding, len(definition.States))
	for name, node := range definition.States {
		if node.Type == domain.StateTerminal {
			continue
		}
		binding, err := e.resolver.Resolve(ctx, request.TenantID, node.Requires)
		if err != nil {
			return domain.RunSnapshot{}, fmt.Errorf("解析状态 %q 的 Handler：%w", name, err)
		}
		bindings[name] = binding
	}
	now := e.options.Now()
	if request.RunID == "" {
		request.RunID = e.options.IDs.New("run_")
	}
	snapshot := domain.RunSnapshot{
		TenantID: request.TenantID, ChatID: request.ChatID, RunID: request.RunID,
		LifecycleStatus: domain.LifecycleRunnable, CurrentState: definition.Initial,
		StateVersion: 1, StateEnterCounter: 1, StateAttempt: 1, Harness: request.Harness,
		ExecutionProfile: request.Profile, ResolvedBindings: bindings,
		PendingCount: 1, Context: cloneJSON(request.InitialContext), Budget: request.Budget, UpdatedAt: now,
	}
	snapshot.OldestPendingAt = timePointer(now)
	signal := e.enteredSignal(snapshot.RunID, snapshot.StateVersion, now)
	outbox := e.stateWakeupOutbox(snapshot.RunID, signal, now)
	if err := e.store.CreateRun(ctx, snapshot, signal, []ports.OutboxRecord{outbox}); err != nil {
		return domain.RunSnapshot{}, err
	}
	return snapshot, nil
}

// ProcessRun 最多消费一个权威 Inbox Signal 并尝试一次状态迁移。
func (e *Engine) ProcessRun(ctx context.Context, runID, workerID string) error {
	load, err := e.store.AcquireExecution(ctx, runID, workerID, e.options.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() { _ = e.store.ReleaseLease(context.Background(), runID, load.FencingToken) }()

	definition, err := e.definitions.Get(ctx, load.Snapshot.Harness.ID, load.Snapshot.Harness.Version)
	if err != nil {
		return err
	}
	node, exists := definition.States[load.Snapshot.CurrentState]
	if !exists {
		return fmt.Errorf("定义中不存在当前状态 %q", load.Snapshot.CurrentState)
	}
	binding, exists := load.Snapshot.ResolvedBindings[load.Snapshot.CurrentState]
	if !exists && node.Type != domain.StateTerminal {
		return ErrMissingBinding
	}
	now := e.options.Now()
	startedAt := now
	outcome, err := e.execute(ctx, load, definition, node, binding, now)
	if err != nil {
		return err
	}
	commit, err := e.buildCommit(load, definition, node, binding, outcome, startedAt, e.options.Now())
	if err != nil {
		return err
	}
	return e.store.CommitTransition(ctx, commit)
}

func (e *Engine) execute(
	ctx context.Context,
	load ports.ExecutionLoad,
	definition domain.HarnessDefinition,
	node domain.StateNode,
	binding domain.HandlerBinding,
	now time.Time,
) (effects.StateOutcome, error) {
	if load.Signal.Type.Equal(qname.MustParse("harness/run.cancel.requested")) {
		return effects.StateOutcome{Kind: effects.OutcomeCancelled, Result: "cancelled"}, nil
	}
	if load.Snapshot.Budget.Exhausted(now) {
		result := "budgetExceeded"
		if !load.Snapshot.Budget.Deadline.IsZero() && !now.Before(load.Snapshot.Budget.Deadline) {
			result = "timedOut"
		}
		return effects.StateOutcome{Kind: effects.OutcomeTerminalFailure, Result: result, ErrorCode: result}, nil
	}
	handler, err := e.resolver.Handler(ctx, binding)
	if err != nil {
		return effects.StateOutcome{}, err
	}
	attempt := load.Snapshot.StateAttempt
	if attempt <= 0 {
		attempt = 1
	}
	outcome, err := handler.OnState(ctx, ports.StateExecutionContext{
		Definition: definition, Snapshot: load.Snapshot, Node: node, Attempt: attempt,
	}, load.Signal)
	if err != nil {
		return effects.StateOutcome{}, err
	}
	if node.SideEffect == domain.SideEffectPure && len(outcome.Effects) > 0 {
		return effects.StateOutcome{}, fmt.Errorf("%w：pure 状态不得创建 Effect Ledger 意图", ErrInvalidOutcome)
	}
	if node.SideEffect != domain.SideEffectPure {
		for _, intent := range outcome.Effects {
			if intent.SideEffect == domain.SideEffectPure {
				return effects.StateOutcome{}, fmt.Errorf("%w：改变世界的状态不得声明 pure Effect", ErrInvalidOutcome)
			}
		}
	}
	return outcome, nil
}

func (e *Engine) buildCommit(
	load ports.ExecutionLoad,
	definition domain.HarnessDefinition,
	node domain.StateNode,
	binding domain.HandlerBinding,
	outcome effects.StateOutcome,
	startedAt, finishedAt time.Time,
) (ports.TransitionCommit, error) {
	snapshot := load.Snapshot
	before := snapshot.StateVersion
	snapshot.StateVersion++
	snapshot.PendingCount--
	if snapshot.PendingCount < 0 {
		return ports.TransitionCommit{}, errors.New("pendingCount 不得为负数")
	}
	snapshot.ActiveHandler = &binding
	snapshot.UpdatedAt = finishedAt
	snapshot.Budget.UsedSteps++
	snapshot.Budget.UsedTokens += outcome.Usage.Tokens
	snapshot.Budget.UsedCostMicros += outcome.Usage.CostMicros
	patched, err := mergeJSON(snapshot.Context, outcome.ContextPatch)
	if err != nil {
		return ports.TransitionCommit{}, err
	}
	snapshot.Context = patched

	attempt := load.Snapshot.StateAttempt
	if attempt <= 0 {
		attempt = 1
	}
	retrying := outcome.Kind == effects.OutcomeRetryableFailure && attempt < maxAttempts(node.Retry)
	if retrying {
		snapshot.LifecycleStatus = domain.LifecycleWaiting
		snapshot.StateAttempt = attempt + 1
		outcome.Timers = append(outcome.Timers, effects.TimerIntent{
			Kind: "retry", DueAt: finishedAt.Add(retryDelay(node.Retry, attempt)),
			SignalType: qname.MustParse("harness/internal.retry"),
		})
	} else {
		if err := applyTransition(&snapshot, definition, outcome); err != nil {
			return ports.TransitionCommit{}, err
		}
	}
	if snapshot.CurrentState != load.Snapshot.CurrentState {
		snapshot.StateEnterCounter++
		snapshot.StateAttempt = 1
	}

	ledgerEntries := e.ledgerEntries(snapshot, node, outcome.Effects, finishedAt)
	timers := e.timers(snapshot, outcome.Timers)
	events := e.events(snapshot, load.Signal, outcome.Events, finishedAt)
	snapshot.LastAcceptedSequence += int64(len(events))

	step := domain.Step{
		RunID: snapshot.RunID, StepSeq: snapshot.StateVersion, Attempt: attempt,
		State: load.Snapshot.CurrentState, Handler: binding, SignalIDs: []string{load.Signal.SignalID},
		StartedAt: startedAt, FinishedAt: finishedAt, Outcome: string(outcome.Kind),
		StateVersionBefore: before, StateVersionAfter: snapshot.StateVersion,
		Usage: outcome.Usage, Invocations: append([]domain.InvocationTrace(nil), outcome.Invocations...),
	}

	commit := ports.TransitionCommit{
		ExpectedStateVersion: before, FencingToken: load.FencingToken, Snapshot: snapshot,
		SignalID: load.Signal.SignalID, Step: step, Effects: ledgerEntries,
		Invocations: append([]effects.ToolInvocation(nil), outcome.ToolInvocations...),
		Timers:      timers, Events: events,
	}
	for _, event := range events {
		commit.Outbox = append(commit.Outbox, e.chatEventOutbox(snapshot.RunID, event, finishedAt))
	}
	if snapshot.LifecycleStatus == domain.LifecycleRunnable {
		continuation := e.enteredSignal(snapshot.RunID, snapshot.StateVersion, finishedAt)
		commit.Continuation = &continuation
		commit.Snapshot.PendingCount++
		if commit.Snapshot.OldestPendingAt == nil || finishedAt.Before(*commit.Snapshot.OldestPendingAt) {
			commit.Snapshot.OldestPendingAt = timePointer(finishedAt)
		}
		commit.Outbox = append(commit.Outbox, e.stateWakeupOutbox(snapshot.RunID, continuation, finishedAt))
	}
	if commit.Snapshot.PendingCount == 0 {
		commit.Snapshot.OldestPendingAt = nil
	}
	return commit, nil
}

func applyTransition(snapshot *domain.RunSnapshot, definition domain.HarnessDefinition, outcome effects.StateOutcome) error {
	if outcome.Kind == effects.OutcomeWaiting {
		snapshot.LifecycleStatus = domain.LifecycleWaiting
		return nil
	}
	current := definition.States[snapshot.CurrentState]
	key := outcome.Result
	if key == "" {
		switch outcome.Kind {
		case effects.OutcomeSucceeded:
			key = "succeeded"
		case effects.OutcomeRetryableFailure, effects.OutcomeTerminalFailure:
			key = "failed"
		case effects.OutcomeCancelled:
			key = "cancelled"
		default:
			return fmt.Errorf("%w：未知 Outcome %q", ErrInvalidOutcome, outcome.Kind)
		}
	}
	target, exists := current.Transitions[key]
	if !exists {
		if outcome.Kind == effects.OutcomeCancelled || outcome.Kind == effects.OutcomeTerminalFailure {
			snapshot.LifecycleStatus = domain.LifecycleTerminal
			reason := terminalReason(outcome, key)
			snapshot.TerminalReason = &reason
			return nil
		}
		return fmt.Errorf("%w：状态 %q 没有 %q 迁移", ErrMissingTransition, snapshot.CurrentState, key)
	}
	targetNode, exists := definition.States[target]
	if !exists {
		return fmt.Errorf("%w：目标状态 %q 不存在", ErrMissingTransition, target)
	}
	snapshot.CurrentState = target
	if targetNode.Type == domain.StateTerminal {
		snapshot.LifecycleStatus = domain.LifecycleTerminal
		reason := terminalReason(outcome, target)
		snapshot.TerminalReason = &reason
		return nil
	}
	snapshot.LifecycleStatus = domain.LifecycleRunnable
	snapshot.TerminalReason = nil
	return nil
}

func terminalReason(outcome effects.StateOutcome, transition string) domain.TerminalReason {
	lower := transition
	switch {
	case outcome.Kind == effects.OutcomeCancelled || lower == "cancelled":
		return domain.TerminalCancelled
	case lower == "timedOut" || outcome.ErrorCode == "timedOut":
		return domain.TerminalTimedOut
	case outcome.Kind == effects.OutcomeSucceeded || lower == "completed" || lower == "done":
		return domain.TerminalCompleted
	default:
		return domain.TerminalFailed
	}
}

func (e *Engine) ledgerEntries(snapshot domain.RunSnapshot, node domain.StateNode, intents []effects.EffectIntent, now time.Time) []effects.EffectLedgerEntry {
	entries := make([]effects.EffectLedgerEntry, 0, len(intents))
	for _, intent := range intents {
		entry := effects.EffectLedgerEntry{
			EffectID: intent.EffectID, InvocationID: intent.EffectID,
			TenantID: snapshot.TenantID, RunID: snapshot.RunID, Kind: intent.Kind,
			Status: effects.EffectPending, LedgerVersion: 1,
			Intent: cloneJSON(intent.Intent), IntentRef: intent.IntentRef,
			IdempotencyKey: intent.IdempotencyKey, SideEffect: node.SideEffect,
			Deadline: intent.Deadline, CreatedAt: now,
		}
		if entry.EffectID == "" {
			entry.EffectID = e.options.IDs.New("eff_")
		}
		if entry.IdempotencyKey == "" {
			entry.IdempotencyKey = snapshot.RunID + "/" + entry.EffectID
		}
		entries = append(entries, entry)
	}
	return entries
}

func (e *Engine) timers(snapshot domain.RunSnapshot, intents []effects.TimerIntent) []effects.Timer {
	timers := make([]effects.Timer, 0, len(intents))
	for _, intent := range intents {
		timerID := intent.TimerID
		if timerID == "" {
			timerID = e.options.IDs.New("timer_")
		}
		timers = append(timers, effects.Timer{
			TimerID: timerID, RunID: snapshot.RunID, Kind: intent.Kind, DueAt: intent.DueAt,
			SignalType: intent.SignalType, StateVersionAtSchedule: snapshot.StateVersion,
			EnteredAtCounter: snapshot.StateEnterCounter, Status: effects.TimerScheduled,
		})
	}
	return timers
}

func (e *Engine) events(snapshot domain.RunSnapshot, signal effects.StateSignal, supplied []domain.EventEnvelope, now time.Time) []domain.EventEnvelope {
	events := append([]domain.EventEnvelope(nil), supplied...)
	payload, _ := json.Marshal(map[string]any{
		"state": snapshot.CurrentState, "lifecycleStatus": snapshot.LifecycleStatus,
		"stateVersion": snapshot.StateVersion,
	})
	events = append(events, domain.EventEnvelope{
		EventID: e.options.IDs.New("evt_"), EventType: qname.MustParse("harness/state.transitioned"),
		TenantID: snapshot.TenantID, StreamID: "run/" + snapshot.RunID, OccurredAt: now,
		CorrelationID: snapshot.RunID, CausationID: signal.SignalID,
		Producer: "harness/onstate-runtime", SchemaVersion: 1, Payload: payload,
	})
	sequence := snapshot.LastAcceptedSequence
	for index := range events {
		sequence++
		if events[index].EventID == "" {
			events[index].EventID = e.options.IDs.New("evt_")
		}
		events[index].TenantID = snapshot.TenantID
		events[index].StreamID = "run/" + snapshot.RunID
		events[index].Sequence = sequence
		if events[index].OccurredAt.IsZero() {
			events[index].OccurredAt = now
		}
		if events[index].CorrelationID == "" {
			events[index].CorrelationID = snapshot.RunID
		}
		if events[index].SchemaVersion == 0 {
			events[index].SchemaVersion = 1
		}
	}
	return events
}

func (e *Engine) enteredSignal(runID string, stateVersion int64, now time.Time) effects.StateSignal {
	return effects.StateSignal{
		SignalID: e.options.IDs.New("sig_"), RunID: runID,
		Type:      qname.MustParse("harness/state.entered"),
		DedupeKey: fmt.Sprintf("%s/%d", runID, stateVersion), OccurredAt: now,
	}
}

func (e *Engine) stateWakeupOutbox(runID string, signal effects.StateSignal, now time.Time) ports.OutboxRecord {
	payload, _ := json.Marshal(map[string]string{"runId": runID, "dedupeKey": signal.DedupeKey})
	return ports.OutboxRecord{
		ID: e.options.IDs.New("out_"), Channel: "state", Key: runID, Payload: payload, CreatedAt: now,
	}
}

func (e *Engine) chatEventOutbox(runID string, event domain.EventEnvelope, now time.Time) ports.OutboxRecord {
	payload, _ := json.Marshal(event)
	return ports.OutboxRecord{
		ID: e.options.IDs.New("out_"), Channel: "chat", Key: runID, Payload: payload, CreatedAt: now,
	}
}

func mergeJSON(base, patch json.RawMessage) (json.RawMessage, error) {
	if len(patch) == 0 {
		return cloneJSON(base), nil
	}
	var baseObject map[string]any
	if len(base) == 0 {
		baseObject = map[string]any{}
	} else if err := json.Unmarshal(base, &baseObject); err != nil {
		return nil, fmt.Errorf("Run Context 不是 JSON 对象：%w", err)
	}
	var patchObject map[string]any
	if err := json.Unmarshal(patch, &patchObject); err != nil {
		return nil, fmt.Errorf("Context Patch 不是 JSON 对象：%w", err)
	}
	for key, value := range patchObject {
		if value == nil {
			delete(baseObject, key)
			continue
		}
		baseObject[key] = value
	}
	return json.Marshal(baseObject)
}

func cloneJSON(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func timePointer(value time.Time) *time.Time { return &value }

func maxAttempts(policy domain.RetryPolicy) int {
	if policy.MaxAttempts <= 0 {
		return 1
	}
	return policy.MaxAttempts
}

func retryDelay(policy domain.RetryPolicy, attempt int) time.Duration {
	delay := policy.InitialBackoff
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	for current := 1; current < attempt; current++ {
		if policy.MaxBackoff > 0 && delay >= policy.MaxBackoff/2 {
			return policy.MaxBackoff
		}
		delay *= 2
	}
	if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return delay
}
