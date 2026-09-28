package ports

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/qname"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conditional write conflict")
	ErrTerminal     = errors.New("run is terminal")
	ErrUnauthorized = errors.New("unauthorized")
)

type Clock interface{ Now() time.Time }

type StateExecutionContext struct {
	Definition domain.HarnessDefinition
	Snapshot   domain.RunSnapshot
	Node       domain.StateNode
	Attempt    int
	Invocations []effects.ToolInvocation
}

// StateHandler 有意不暴露生命周期和终局字段；只有运行时可由 Outcome 与 IR 推导它们。
type StateHandler interface {
	OnState(context.Context, StateExecutionContext, effects.StateSignal) (effects.StateOutcome, error)
}

type HandlerDescriptor struct {
	HandlerID     qname.QName            `json:"handlerId"`
	Version       string                 `json:"version"`
	Capabilities  []CapabilityDescriptor `json:"capabilities"`
	Deployment    string                 `json:"deployment"`
	Endpoint      string                 `json:"endpoint,omitempty"`
	Features      []string               `json:"features,omitempty"`
	Status        string                 `json:"status"`
	LastHeartbeat time.Time              `json:"lastHeartbeat"`
	ExpiresAt     time.Time              `json:"expiresAt"`
}

type CapabilityDescriptor struct {
	Capability qname.QName `json:"capability"`
	Version    string      `json:"version"`
	Features   []string    `json:"features,omitempty"`
}

type HandlerResolver interface {
	Resolve(context.Context, string, domain.RequirementSpec) (domain.HandlerBinding, error)
	Handler(context.Context, domain.HandlerBinding) (StateHandler, error)
}

type DefinitionRepository interface {
	PutDraft(context.Context, domain.HarnessDefinition) error
	Publish(context.Context, string, string) (domain.HarnessDefinition, error)
	Get(context.Context, string, string) (domain.HarnessDefinition, error)
}

type ExecutionLoad struct {
	Snapshot     domain.RunSnapshot
	Signal       effects.StateSignal
	FencingToken int64
}

type TransitionCommit struct {
	ExpectedStateVersion int64
	FencingToken         int64
	Snapshot             domain.RunSnapshot
	SignalID             string
	Step                 domain.Step
	Effects              []effects.EffectLedgerEntry
	Invocations          []effects.ToolInvocation
	InvocationUpdates    []effects.InvocationUpdate
	Timers               []effects.Timer
	Events               []domain.EventEnvelope
	Outbox               []OutboxRecord
	Continuation         *effects.StateSignal
}

type OutboxRecord struct {
	ID          string          `json:"id"`
	Channel     string          `json:"channel"`
	Key         string          `json:"key"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"createdAt"`
	PublishedAt *time.Time      `json:"publishedAt,omitempty"`
}

type StateStore interface {
	CreateChat(context.Context, domain.Chat) error
	GetChat(context.Context, string) (domain.Chat, error)
	AppendMessage(context.Context, domain.Message) error
	CreateRun(context.Context, domain.RunSnapshot, effects.StateSignal, []OutboxRecord) error
	GetRun(context.Context, string) (domain.RunSnapshot, error)
	PutSignal(context.Context, effects.StateSignal, OutboxRecord) (bool, error)
	AcquireExecution(context.Context, string, string, time.Duration) (ExecutionLoad, error)
	CommitTransition(context.Context, TransitionCommit) error
	ReleaseLease(context.Context, string, int64) error
	Steps(context.Context, string) ([]domain.Step, error)
	Events(context.Context, string, int64, int) ([]domain.EventEnvelope, error)
	Inbox(context.Context, string) ([]effects.StateSignal, error)
	Effects(context.Context, string) ([]effects.EffectLedgerEntry, error)
	Invocations(context.Context, string) ([]effects.ToolInvocation, error)
	PendingEffects(context.Context, int) ([]effects.EffectLedgerEntry, error)
	ClaimEffect(context.Context, string, int64, string) (effects.EffectLedgerEntry, error)
	DispatchedEffects(context.Context, time.Time, int) ([]effects.EffectLedgerEntry, error)
	ReclaimEffect(context.Context, string, int64, string, time.Time) (effects.EffectLedgerEntry, error)
	CommitEffectResult(context.Context, EffectResultCommit) error
	MarkEffectManual(context.Context, string, int64) error
	DueTimers(context.Context, time.Time, int) ([]effects.Timer, error)
	FireTimer(context.Context, string, int64, effects.StateSignal, OutboxRecord) (bool, error)
	PendingOutbox(context.Context, int) ([]OutboxRecord, error)
	MarkOutboxPublished(context.Context, string) error
	RunnableWithoutSignal(context.Context, int) ([]domain.RunSnapshot, error)
	// StalledRuns 扫描「Inbox 仍有未消费 Signal、但租约已失效」的非终态 Run。
	// 这是 Worker 崩溃、唤醒提示丢失后的兜底发现手段（§11.1）——队列只是提示，
	// Inbox 才是事实来源，必须有第二条不依赖提示的唤醒路径。
	StalledRuns(context.Context, time.Time, int) ([]domain.RunSnapshot, error)
	// EnqueueHint 只写入一条 Outbox 唤醒提示，不触碰任何业务状态。
	// Reconciler 用它为停滞 Run 补发提示；最终迁移仍由 onState 经 CAS 提交。
	EnqueueHint(context.Context, OutboxRecord) error
}

type EffectResultCommit struct {
	EffectID              string
	ExpectedLedgerVersion int64
	ExternalRef           string
	ResultRef             string
	Signal                effects.StateSignal
	Outbox                OutboxRecord
}

type Queue interface {
	Publish(context.Context, string, []byte) error
	Subscribe(context.Context) (<-chan Message, error)
}

type Message struct {
	Key  string
	Body []byte
}

type ArtifactStore interface {
	Put(context.Context, string, io.Reader) (string, error)
	Get(context.Context, string) (io.ReadCloser, error)
}

type ModelRequest struct {
	RunID  string           `json:"runId"`
	Prompt string           `json:"prompt"`
	Model  string           `json:"model"`
	Tools  []ToolDefinition `json:"tools,omitempty"`
}

type ToolDefinition struct {
	Name        qname.QName     `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type ModelResponse struct {
	Content      string       `json:"content,omitempty"`
	ToolCalls    []ToolCall   `json:"toolCalls,omitempty"`
	Usage        domain.Usage `json:"usage"`
	Provider     string       `json:"provider"`
	Model        string       `json:"model"`
	ModelVersion string       `json:"modelVersion,omitempty"`
}

type ToolCall struct {
	CallID    string          `json:"callId"`
	Tool      qname.QName     `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

type ModelProvider interface {
	Invoke(context.Context, ModelRequest) (ModelResponse, error)
}

type ToolRequest struct {
	EffectID             string
	IdempotencyKey       string
	TenantID             string
	RegistrationID       string
	RegistrationRevision int64
	Endpoint             string
	Tool                 qname.QName
	Input                json.RawMessage
}

type ToolResult struct {
	ExternalExecutionRef string
	Result               []byte
	Async                bool
}

type ToolExecutor interface {
	Invoke(context.Context, ToolRequest) (ToolResult, error)
	Recover(context.Context, string) (ToolResult, bool, error)
}

type ToolRegistration struct {
	RegistrationID string
	Revision       int64
	Name           qname.QName
	Endpoint       string
}

type ToolCatalog interface {
	ResolveTool(context.Context, string, qname.QName) (ToolRegistration, error)
}

type EffectExecutor interface {
	Execute(context.Context, effects.EffectLedgerEntry) (externalRef, resultRef string, err error)
	Recover(context.Context, effects.EffectLedgerEntry) (externalRef, resultRef string, completed bool, err error)
}
