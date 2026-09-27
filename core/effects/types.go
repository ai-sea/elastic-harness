package effects

import (
	"encoding/json"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type OutcomeKind string

const (
	OutcomeSucceeded        OutcomeKind = "succeeded"
	OutcomeWaiting          OutcomeKind = "waiting"
	OutcomeRetryableFailure OutcomeKind = "retryableFailure"
	OutcomeTerminalFailure  OutcomeKind = "terminalFailure"
	OutcomeCancelled        OutcomeKind = "cancelled"
)

type StateSignal struct {
	SignalID       string          `json:"signalId"`
	RunID          string          `json:"runId"`
	Sequence       int64           `json:"sequence"`
	Type           qname.QName     `json:"type"`
	DedupeKey      string          `json:"dedupeKey"`
	Priority       int             `json:"priority"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	PayloadRef     string          `json:"payloadRef,omitempty"`
	OccurredAt     time.Time       `json:"occurredAt"`
	SourceSeq      int64           `json:"sourceSequence,omitempty"`
	ConsumedByStep *int64          `json:"consumedByStep,omitempty"`
}

type EffectIntent struct {
	EffectID       string            `json:"effectId"`
	Kind           qname.QName       `json:"kind"`
	Intent         json.RawMessage   `json:"intent,omitempty"`
	IntentRef      string            `json:"intentRef,omitempty"`
	IdempotencyKey string            `json:"idempotencyKey"`
	Deadline       time.Time         `json:"deadline"`
	SideEffect     domain.SideEffect `json:"sideEffect"`
}

type TimerIntent struct {
	TimerID    string      `json:"timerId"`
	Kind       string      `json:"kind"`
	DueAt      time.Time   `json:"dueAt"`
	SignalType qname.QName `json:"signalType"`
}

type StateOutcome struct {
	Kind         OutcomeKind              `json:"kind"`
	Result       string                   `json:"result,omitempty"`
	ContextPatch json.RawMessage          `json:"contextPatch,omitempty"`
	Events       []domain.EventEnvelope   `json:"events,omitempty"`
	Effects      []EffectIntent           `json:"effects,omitempty"`
	Timers       []TimerIntent            `json:"timers,omitempty"`
	Usage        domain.Usage             `json:"usage"`
	Invocations  []domain.InvocationTrace `json:"invocations,omitempty"`
	ErrorCode    string                   `json:"errorCode,omitempty"`
}

type EffectStatus string

const (
	EffectPending     EffectStatus = "PENDING"
	EffectDispatched  EffectStatus = "DISPATCHED"
	EffectCommitted   EffectStatus = "COMMITTED"
	EffectCompensated EffectStatus = "COMPENSATED"
	EffectManual      EffectStatus = "NEEDS_MANUAL"
)

type EffectLedgerEntry struct {
	EffectID       string            `json:"effectId"`
	TenantID       string            `json:"tenantId"`
	RunID          string            `json:"runId"`
	Kind           qname.QName       `json:"kind"`
	Status         EffectStatus      `json:"status"`
	LedgerVersion  int64             `json:"ledgerVersion"`
	Intent         json.RawMessage   `json:"intent,omitempty"`
	IntentRef      string            `json:"intentRef,omitempty"`
	IdempotencyKey string            `json:"idempotencyKey"`
	SideEffect     domain.SideEffect `json:"sideEffect"`
	Deadline       time.Time         `json:"deadline"`
	DispatcherRef  string            `json:"dispatcherRef,omitempty"`
	ExternalRef    string            `json:"externalExecutionRef,omitempty"`
	ResultRef      string            `json:"resultRef,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	ResolvedAt     *time.Time        `json:"resolvedAt,omitempty"`
}

type TimerStatus string

const (
	TimerScheduled TimerStatus = "SCHEDULED"
	TimerFired     TimerStatus = "FIRED"
	TimerCancelled TimerStatus = "CANCELLED"
)

type Timer struct {
	TimerID                string      `json:"timerId"`
	RunID                  string      `json:"runId"`
	Kind                   string      `json:"kind"`
	DueAt                  time.Time   `json:"dueAt"`
	SignalType             qname.QName `json:"signalType"`
	StateVersionAtSchedule int64       `json:"stateVersionAtSchedule"`
	EnteredAtCounter       int64       `json:"enteredAtCounter"`
	Status                 TimerStatus `json:"status"`
}

type InvocationStatus string

const (
	InvocationPending         InvocationStatus = "PENDING"
	InvocationDispatched      InvocationStatus = "DISPATCHED"
	InvocationRunning         InvocationStatus = "RUNNING"
	InvocationSucceeded       InvocationStatus = "SUCCEEDED"
	InvocationFailed          InvocationStatus = "FAILED"
	InvocationTimedOut        InvocationStatus = "TIMED_OUT"
	InvocationCancelRequested InvocationStatus = "CANCEL_REQUESTED"
	InvocationCancelled       InvocationStatus = "CANCELLED"
)

type ToolInvocation struct {
	InvocationID         string           `json:"invocationId"`
	TenantID             string           `json:"tenantId"`
	RunID                string           `json:"runId"`
	ToolID               qname.QName      `json:"toolId"`
	RegistrationID       string           `json:"registrationId"`
	RegistrationRevision int64            `json:"registrationRevision"`
	Status               InvocationStatus `json:"status"`
	InvocationVersion    int64            `json:"invocationVersion"`
	Attempt              int              `json:"attempt"`
	IdempotencyKey       string           `json:"idempotencyKey"`
	ExternalExecutionRef string           `json:"externalExecutionRef,omitempty"`
	InputRef             string           `json:"inputRef,omitempty"`
	ResultRef            string           `json:"resultRef,omitempty"`
	HeartbeatDeadline    time.Time        `json:"heartbeatDeadline,omitempty"`
	CallbackDeadline     time.Time        `json:"callbackDeadline,omitempty"`
}
