package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	ceqembedded "github.com/ai-sea/elastic-harness/adapters/ceq-embedded"
	kvsqlite "github.com/ai-sea/elastic-harness/adapters/kv-sqlite"
	objectstorelocal "github.com/ai-sea/elastic-harness/adapters/objectstore-local"
	seqembedded "github.com/ai-sea/elastic-harness/adapters/seq-embedded"
	toolexecutorhttp "github.com/ai-sea/elastic-harness/adapters/toolexecutor-http"
	api "github.com/ai-sea/elastic-harness/components/api"
	dispatcher "github.com/ai-sea/elastic-harness/components/effect-dispatcher"
	projector "github.com/ai-sea/elastic-harness/components/event-projector"
	executionprofile "github.com/ai-sea/elastic-harness/components/execution-profile"
	registry "github.com/ai-sea/elastic-harness/components/handler-registry"
	definition "github.com/ai-sea/elastic-harness/components/harness-definition"
	llmhandler "github.com/ai-sea/elastic-harness/components/llm-handler"
	runtime "github.com/ai-sea/elastic-harness/components/onstate-runtime"
	reconciler "github.com/ai-sea/elastic-harness/components/timer-reconciler"
	toolexecutor "github.com/ai-sea/elastic-harness/components/tool-executor"
	toolhandler "github.com/ai-sea/elastic-harness/components/tool-handler"
	toolregistry "github.com/ai-sea/elastic-harness/components/tool-registry"
	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type application struct {
	store    *kvsqlite.Store
	handler  http.Handler
	cancel   context.CancelFunc
	wait     sync.WaitGroup
	registry *registry.Registry
	tokens   []string
	views    *projector.MemorySink
}

type applicationOptions struct {
	databasePath string
	artifactPath string
	toolEndpoint string
	provider     ports.ModelProvider
	logger       *log.Logger
}

func newApplication(options applicationOptions) (*application, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	store, err := kvsqlite.Open(options.databasePath)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = store.Close()
		}
	}()
	artifacts, err := objectstorelocal.New(options.artifactPath)
	if err != nil {
		return nil, err
	}
	definitions := definition.NewRepository()
	tools := toolregistry.New()
	if err := tools.Publish(toolregistry.Registration{
		RegistrationID: "builtin-echo", Revision: 1,
		Manifest: toolregistry.Manifest{
			Name: qname.MustParse("harness/echo"), Description: "返回输入参数",
			InputSchema: json.RawMessage(`{"type":"object","additionalProperties":true}`),
			SideEffect:  string(domain.SideEffectIdempotent), Endpoint: options.toolEndpoint,
		},
	}, true); err != nil {
		return nil, err
	}
	profiles := executionprofile.New(artifacts)
	profile, err := profiles.Publish(context.Background(), "system", domain.ExecutionProfile{
		ID: "standalone-default", Revision: 1, AllowedNamespaces: []string{"harness"},
		Budget: domain.Budget{MaxSteps: 32, MaxTokens: 100000, MaxCostMicros: 1000000},
	})
	if err != nil {
		return nil, err
	}
	definitionValue := defaultDefinition()
	if err := definitions.PutDraft(context.Background(), definitionValue); err != nil {
		return nil, err
	}
	if _, err := definitions.Publish(context.Background(), definitionValue.ID, definitionValue.Version); err != nil {
		return nil, err
	}
	handlerRegistry, err := registry.New(registry.Options{
		Issuer: "elastic-harness-standalone", Audience: "state-handlers",
		TokenTTL: time.Minute, HeartbeatTTL: time.Minute,
	})
	if err != nil {
		return nil, err
	}
	if options.provider == nil {
		options.provider = &demoProvider{}
	}
	llm := llmhandler.New(options.provider, tools, "standalone-demo")
	tool := toolhandler.New(tools, nil)
	tokens := make([]string, 0, 2)
	for _, registration := range []struct {
		descriptor ports.HandlerDescriptor
		handler    ports.StateHandler
	}{
		{handlerDescriptor("harness/llm.invoke", []string{"tool-calling"}), llm},
		{handlerDescriptor("harness/tools.invoke-all", nil), tool},
	} {
		token, err := handlerRegistry.Register(registration.descriptor, registration.handler, true)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	ids := runtime.RandomIDGenerator{}
	engine := runtime.New(store, definitions, handlerRegistry, runtime.Options{IDs: ids})
	stateQueue := seqembedded.New(1024)
	chatQueue := ceqembedded.New(1024)
	relay := projector.NewOutboxRelay(store, map[string]ports.Queue{"state": stateQueue, "chat": chatQueue})
	views := projector.NewMemorySink()
	chatProjection := projector.NewProjection(store, views)
	httpTools := toolexecutorhttp.New(nil)
	effectExecutor := toolexecutor.New(httpTools, artifacts)
	effectDispatcher := dispatcher.New(store, effectExecutor, "standalone-dispatcher")
	timerReconciler := reconciler.New(store, ids, nil)
	apiServer := api.New(store, engine, ids, api.Defaults{
		Harness: domain.HarnessRef{ID: definitionValue.ID, Version: definitionValue.Version},
		Profile: profile, Budget: domain.Budget{MaxSteps: 32, MaxTokens: 100000, MaxCostMicros: 1000000}, RunTTL: time.Hour,
	}, nil)
	root := http.NewServeMux()
	root.HandleFunc("POST /internal/tools/echo", echoTool)
	root.Handle("/", apiServer.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	app := &application{store: store, handler: root, cancel: cancel, registry: handlerRegistry, tokens: tokens, views: views}
	app.start(ctx, engine, stateQueue, chatQueue, chatProjection, relay, effectDispatcher, timerReconciler, options.logger)
	failed = false
	return app, nil
}

func (a *application) Handler() http.Handler { return a.handler }

func (a *application) Close() error {
	a.cancel()
	a.wait.Wait()
	return a.store.Close()
}

func (a *application) start(
	ctx context.Context,
	engine *runtime.Engine,
	stateQueue ports.Queue,
	chatQueue ports.Queue,
	chatProjection *projector.Projection,
	relay *projector.OutboxRelay,
	effectDispatcher *dispatcher.Dispatcher,
	timerReconciler *reconciler.Reconciler,
	logger *log.Logger,
) {
	if logger == nil {
		logger = log.Default()
	}
	stateMessages, _ := stateQueue.Subscribe(ctx)
	chatMessages, _ := chatQueue.Subscribe(ctx)
	a.runLoop(ctx, 10*time.Millisecond, func() error {
		_, err := relay.RelayOnce(ctx, 100)
		return err
	}, logger)
	a.wait.Add(1)
	go func() {
		defer a.wait.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-stateMessages:
				var wakeup struct {
					RunID string `json:"runId"`
				}
				if err := json.Unmarshal(message.Body, &wakeup); err != nil {
					logger.Printf("解析状态提示失败：%v", err)
					continue
				}
				if err := engine.ProcessRun(ctx, wakeup.RunID, "standalone-worker"); err != nil && !errors.Is(err, ports.ErrConflict) && !errors.Is(err, ports.ErrNotFound) {
					logger.Printf("处理 Run %s 失败：%v", wakeup.RunID, err)
				}
			}
		}
	}()
	a.wait.Add(1)
	go func() {
		defer a.wait.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-chatMessages:
				if _, err := chatProjection.ApplyMessage(ctx, message); err != nil {
					logger.Printf("投影 Chat 事件失败：%v", err)
				}
			}
		}
	}()
	a.runLoop(ctx, 10*time.Millisecond, func() error {
		_, err := effectDispatcher.DispatchOnce(ctx, 100)
		return err
	}, logger)
	a.runLoop(ctx, 50*time.Millisecond, func() error {
		if _, err := timerReconciler.FireDueTimers(ctx, 100); err != nil {
			return err
		}
		if _, err := timerReconciler.RepairRunnable(ctx, 100); err != nil {
			return err
		}
		// Worker 崩溃/提示丢失的兜底：为租约失效但 Inbox 仍有未消费 Signal 的 Run 补发提示（§11.1）。
		_, err := timerReconciler.ReawakenStalled(ctx, 100)
		return err
	}, logger)
	a.runLoop(ctx, 20*time.Second, func() error {
		for index, token := range a.tokens {
			refreshed, err := a.registry.Heartbeat(token)
			if err != nil {
				return err
			}
			a.tokens[index] = refreshed
		}
		return nil
	}, logger)
}

func (a *application) runLoop(ctx context.Context, interval time.Duration, operation func() error, logger *log.Logger) {
	a.wait.Add(1)
	go func() {
		defer a.wait.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := operation(); err != nil && !errors.Is(err, context.Canceled) {
					logger.Printf("后台循环失败：%v", err)
				}
			}
		}
	}()
}

func handlerDescriptor(name string, features []string) ports.HandlerDescriptor {
	identifier := qname.MustParse(name)
	return ports.HandlerDescriptor{
		HandlerID: identifier, Version: "1.0.0", Deployment: "in-process",
		Capabilities: []ports.CapabilityDescriptor{{Capability: identifier, Version: "1.0.0", Features: features}},
	}
}

func defaultDefinition() domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "default-agent-loop", Version: "1.0.0", Initial: "call-model",
		States: map[string]domain.StateNode{
			"call-model": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires: domain.RequirementSpec{
					Capability: qname.MustParse("harness/llm.invoke"), Version: "^1", Features: []string{"tool-calling"},
				},
				Timeouts: domain.Timeouts{StartToClose: 2 * time.Minute}, Retry: domain.RetryPolicy{MaxAttempts: 3},
				Transitions: map[string]string{
					"toolRequested": "call-tools", "completed": "completed", "failed": "failed",
					"budgetExceeded": "failed", "timedOut": "timed-out", "cancelled": "cancelled",
				},
			},
			"call-tools": {
				Type: domain.StateWait, SideEffect: domain.SideEffectIdempotent,
				Requires: domain.RequirementSpec{Capability: qname.MustParse("harness/tools.invoke-all"), Version: "^1"},
				Timeouts: domain.Timeouts{StartToClose: 30 * time.Second, Callback: time.Minute, State: 2 * time.Minute},
				Retry:    domain.RetryPolicy{MaxAttempts: 3},
				Transitions: map[string]string{
					"succeeded": "call-model", "failed": "failed", "timedOut": "timed-out", "cancelled": "cancelled",
				},
			},
			"completed": {Type: domain.StateTerminal},
			"failed":    {Type: domain.StateTerminal},
			"timed-out": {Type: domain.StateTerminal},
			"cancelled": {Type: domain.StateTerminal},
		},
	}
}

func echoTool(response http.ResponseWriter, request *http.Request) {
	var input any
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("X-Execution-Ref", request.Header.Get("Idempotency-Key"))
	_ = json.NewEncoder(response).Encode(map[string]any{"echo": input})
}

type demoProvider struct{}

func (*demoProvider) Invoke(_ context.Context, request ports.ModelRequest) (ports.ModelResponse, error) {
	if strings.Contains(request.Prompt, "toolResult:") {
		return ports.ModelResponse{
			Content: "工具调用已完成。", Usage: domain.Usage{Tokens: 12},
			Provider: "standalone-demo", Model: request.Model, ModelVersion: "1",
		}, nil
	}
	arguments, _ := json.Marshal(map[string]string{"text": request.Prompt})
	return ports.ModelResponse{
		ToolCalls: []ports.ToolCall{{CallID: "call_" + request.RunID, Tool: qname.MustParse("harness/echo"), Arguments: arguments}},
		Usage:     domain.Usage{Tokens: 8}, Provider: "standalone-demo", Model: request.Model, ModelVersion: "1",
	}, nil
}

func validateOptions(options applicationOptions) error {
	if options.databasePath == "" || options.artifactPath == "" || options.toolEndpoint == "" {
		return fmt.Errorf("databasePath、artifactPath 与 toolEndpoint 不能为空")
	}
	return nil
}
