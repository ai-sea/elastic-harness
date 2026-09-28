package domain

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ai-sea/elastic-harness/core/qname"
)

type LifecycleStatus string

const (
	LifecycleRunnable LifecycleStatus = "RUNNABLE"
	LifecycleWaiting  LifecycleStatus = "WAITING"
	LifecycleBlocked  LifecycleStatus = "BLOCKED"
	LifecycleTerminal LifecycleStatus = "TERMINAL"
)

type TerminalReason string

const (
	TerminalCompleted TerminalReason = "COMPLETED"
	TerminalFailed    TerminalReason = "FAILED"
	TerminalCancelled TerminalReason = "CANCELLED"
	TerminalTimedOut  TerminalReason = "TIMED_OUT"
)

type SideEffect string

const (
	SideEffectPure          SideEffect = "pure"
	SideEffectIdempotent    SideEffect = "idempotent"
	SideEffectNonIdempotent SideEffect = "non-idempotent"
)

type StateType string

const (
	StateNormal   StateType = "normal"
	StateWait     StateType = "wait"
	StateBranch   StateType = "branch"
	StateTerminal StateType = "terminal"
)

type HarnessRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type ProfileRef struct {
	ID          string `json:"id"`
	Revision    int64  `json:"revision"`
	SnapshotRef string `json:"snapshotRef,omitempty"`
}

type HandlerBinding struct {
	HandlerID      qname.QName `json:"handlerId"`
	HandlerVersion string      `json:"handlerVersion"`
	Deployment     string      `json:"deployment"`
}

type Budget struct {
	MaxSteps       int64     `json:"maxSteps"`
	MaxTokens      int64     `json:"maxTokens"`
	MaxCostMicros  int64     `json:"maxCostMicros"`
	Deadline       time.Time `json:"deadline"`
	UsedSteps      int64     `json:"usedSteps"`
	UsedTokens     int64     `json:"usedTokens"`
	UsedCostMicros int64     `json:"usedCostMicros"`
}

func (b Budget) Exhausted(now time.Time) bool {
	return (!b.Deadline.IsZero() && !now.Before(b.Deadline)) ||
		(b.MaxSteps > 0 && b.UsedSteps >= b.MaxSteps) ||
		(b.MaxTokens > 0 && b.UsedTokens >= b.MaxTokens) ||
		(b.MaxCostMicros > 0 && b.UsedCostMicros >= b.MaxCostMicros)
}

type RunSnapshot struct {
	TenantID             string                    `json:"tenantId"`
	ChatID               string                    `json:"chatId"`
	RunID                string                    `json:"runId"`
	LifecycleStatus      LifecycleStatus           `json:"lifecycleStatus"`
	TerminalReason       *TerminalReason           `json:"terminalReason,omitempty"`
	CurrentState         string                    `json:"currentState"`
	StateVersion         int64                     `json:"stateVersion"`
	Harness              HarnessRef                `json:"harness"`
	ExecutionProfile     ProfileRef                `json:"executionProfile"`
	ResolvedBindings     map[string]HandlerBinding `json:"resolvedBindings"`
	ActiveHandler        *HandlerBinding           `json:"activeHandler,omitempty"`
	LastAcceptedSequence int64                     `json:"lastAcceptedSequence"`
	StateEnterCounter    int64                     `json:"stateEnterCounter"`
	StateAttempt         int                       `json:"stateAttempt"`
	PendingCount         int64                     `json:"pendingCount"`
	OldestPendingAt      *time.Time                `json:"oldestPendingAt,omitempty"`
	CheckpointRef        string                    `json:"checkpointRef,omitempty"`
	Context              json.RawMessage           `json:"context,omitempty"`
	Budget               Budget                    `json:"budget"`
	UpdatedAt            time.Time                 `json:"updatedAt"`
}

func (r RunSnapshot) Validate() error {
	if r.TenantID == "" || r.ChatID == "" || r.RunID == "" {
		return errors.New("run snapshot: tenantId, chatId and runId are required")
	}
	if r.CurrentState == "" || r.Harness.ID == "" || r.Harness.Version == "" {
		return errors.New("run snapshot: currentState and immutable harness reference are required")
	}
	if r.LifecycleStatus == LifecycleTerminal && r.TerminalReason == nil {
		return errors.New("run snapshot: terminal run requires terminalReason")
	}
	if r.LifecycleStatus != LifecycleTerminal && r.TerminalReason != nil {
		return errors.New("run snapshot: non-terminal run cannot have terminalReason")
	}
	return nil
}

type Usage struct {
	Tokens     int64 `json:"tokens"`
	CostMicros int64 `json:"costMicros"`
}

type InvocationTrace struct {
	Kind                 string `json:"kind"`
	Provider             string `json:"provider,omitempty"`
	Model                string `json:"model,omitempty"`
	ModelVersion         string `json:"modelVersion,omitempty"`
	ProfileRevision      int64  `json:"profileRevision,omitempty"`
	RouteReason          string `json:"routeReason,omitempty"`
	CallPath             string `json:"callPath"`
	ExternalExecutionRef string `json:"externalExecutionRef,omitempty"`
}

type Step struct {
	RunID              string            `json:"runId"`
	StepSeq            int64             `json:"stepSeq"`
	Attempt            int               `json:"attempt"`
	State              string            `json:"state"`
	Handler            HandlerBinding    `json:"handler"`
	SignalIDs          []string          `json:"signalIds"`
	StartedAt          time.Time         `json:"startedAt"`
	FinishedAt         time.Time         `json:"finishedAt"`
	Outcome            string            `json:"outcome"`
	StateVersionBefore int64             `json:"stateVersionBefore"`
	StateVersionAfter  int64             `json:"stateVersionAfter,omitempty"`
	Usage              Usage             `json:"usage"`
	Invocations        []InvocationTrace `json:"invocations,omitempty"`
	TraceID            string            `json:"traceId,omitempty"`
}

type EventEnvelope struct {
	EventID         string            `json:"eventId"`
	EventType       qname.QName       `json:"eventType"`
	TenantID        string            `json:"tenantId"`
	StreamID        string            `json:"streamId"`
	Sequence        int64             `json:"sequence"`
	OccurredAt      time.Time         `json:"occurredAt"`
	CorrelationID   string            `json:"correlationId"`
	CausationID     string            `json:"causationId,omitempty"`
	Producer        string            `json:"producer"`
	SchemaVersion   int               `json:"schemaVersion"`
	Payload         json.RawMessage   `json:"payload,omitempty"`
	ArtifactRefs    []string          `json:"artifactRefs,omitempty"`
	TraceContext    map[string]string `json:"traceContext,omitempty"`
	SecurityContext map[string]string `json:"securityContext,omitempty"`
}

type Chat struct {
	TenantID  string    `json:"tenantId"`
	ChatID    string    `json:"chatId"`
	CreatedAt time.Time `json:"createdAt"`
}

type Message struct {
	TenantID  string    `json:"tenantId"`
	ChatID    string    `json:"chatId"`
	MessageID string    `json:"messageId"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt"`
}
