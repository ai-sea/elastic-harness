package domain

import (
	"encoding/json"
	"time"

	"github.com/ai-sea/elastic-harness/core/qname"
)

type RequirementSpec struct {
	Capability qname.QName `json:"capability"`
	Version    string      `json:"version"`
	Features   []string    `json:"features,omitempty"`
	Deployment string      `json:"deployment,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts    int           `json:"maxAttempts"`
	InitialBackoff time.Duration `json:"initialBackoff,omitempty"`
	MaxBackoff     time.Duration `json:"maxBackoff,omitempty"`
}

type Timeouts struct {
	ScheduleToStart time.Duration `json:"scheduleToStart,omitempty"`
	StartToClose    time.Duration `json:"startToClose,omitempty"`
	Heartbeat       time.Duration `json:"heartbeat,omitempty"`
	Callback        time.Duration `json:"callback,omitempty"`
	State           time.Duration `json:"state,omitempty"`
}

type StateNode struct {
	Type        StateType         `json:"type"`
	SideEffect  SideEffect        `json:"sideEffect"`
	OnFailure   string            `json:"onFailure,omitempty"`
	Requires    RequirementSpec   `json:"requires"`
	Config      json.RawMessage   `json:"config,omitempty"`
	Transitions map[string]string `json:"on"`
	Timeouts    Timeouts          `json:"timeouts"`
	Retry       RetryPolicy       `json:"retry"`
}

type HarnessDefinition struct {
	ID        string               `json:"id"`
	Version   string               `json:"version"`
	Initial   string               `json:"initial"`
	States    map[string]StateNode `json:"states"`
	Published bool                 `json:"published"`
	CreatedAt time.Time            `json:"createdAt"`
}

type ExecutionProfile struct {
	ID                string              `json:"id"`
	Revision          int64               `json:"revision"`
	AllowedNamespaces []string            `json:"allowedNamespaces"`
	RequiredFeatures  map[string][]string `json:"requiredFeatures,omitempty"`
	Budget            Budget              `json:"budget"`
	Published         bool                `json:"published"`
}
