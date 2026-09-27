package toolregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type Manifest struct {
	Name        qname.QName     `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
	SideEffect  string          `json:"sideEffect"`
	Endpoint    string          `json:"endpoint"`
}

type Registration struct {
	RegistrationID string    `json:"registrationId"`
	Revision       int64     `json:"revision"`
	TenantID       string    `json:"tenantId"`
	Manifest       Manifest  `json:"manifest"`
	Status         string    `json:"status"`
	PublishedAt    time.Time `json:"publishedAt"`
}

// Registry 保存显式发布的工具 Manifest，并以 Revision 固定执行语义。
type Registry struct {
	mu      sync.RWMutex
	entries map[string]Registration
}

func New() *Registry { return &Registry{entries: make(map[string]Registration)} }

func (r *Registry) Publish(registration Registration, platform bool) error {
	if registration.RegistrationID == "" || registration.Revision <= 0 || registration.Manifest.Name.IsZero() {
		return errors.New("工具注册缺少 ID、Revision 或 QName")
	}
	if registration.Manifest.Name.IsPlatform() && !platform {
		return errors.New("非平台来源不得注册 harness/* 工具")
	}
	if !json.Valid(registration.Manifest.InputSchema) {
		return errors.New("工具 inputSchema 不是有效 JSON")
	}
	registration.Status = "active"
	if registration.PublishedAt.IsZero() {
		registration.PublishedAt = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := registrationKey(registration.RegistrationID, registration.Revision)
	if _, exists := r.entries[key]; exists {
		return ports.ErrConflict
	}
	r.entries[key] = clone(registration)
	return nil
}

func (r *Registry) Disable(registrationID string, revision int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := registrationKey(registrationID, revision)
	registration, exists := r.entries[key]
	if !exists {
		return ports.ErrNotFound
	}
	registration.Status = "disabled"
	r.entries[key] = registration
	return nil
}

func (r *Registry) Resolve(_ context.Context, tenantID string, name qname.QName) (Registration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var selected Registration
	found := false
	for _, registration := range r.entries {
		if registration.Status != "active" || !registration.Manifest.Name.Equal(name) {
			continue
		}
		if registration.TenantID != "" && registration.TenantID != tenantID {
			continue
		}
		if !found || registration.Revision > selected.Revision {
			selected, found = registration, true
		}
	}
	if !found {
		return Registration{}, ports.ErrNotFound
	}
	return clone(selected), nil
}

func (r *Registry) ResolveTool(ctx context.Context, tenantID string, name qname.QName) (ports.ToolRegistration, error) {
	registration, err := r.Resolve(ctx, tenantID, name)
	if err != nil {
		return ports.ToolRegistration{}, err
	}
	return ports.ToolRegistration{
		RegistrationID: registration.RegistrationID, Revision: registration.Revision,
		Name: registration.Manifest.Name, Endpoint: registration.Manifest.Endpoint,
	}, nil
}

func (r *Registry) Definitions(_ context.Context, tenantID string) []ports.ToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	definitions := make([]ports.ToolDefinition, 0)
	for _, registration := range r.entries {
		if registration.Status != "active" || (registration.TenantID != "" && registration.TenantID != tenantID) {
			continue
		}
		definitions = append(definitions, ports.ToolDefinition{
			Name: registration.Manifest.Name, Description: registration.Manifest.Description,
			InputSchema: append(json.RawMessage(nil), registration.Manifest.InputSchema...),
		})
	}
	return definitions
}

func registrationKey(id string, revision int64) string {
	return fmt.Sprintf("%s@%d", id, revision)
}

func clone(value Registration) Registration {
	value.Manifest.InputSchema = append(json.RawMessage(nil), value.Manifest.InputSchema...)
	return value
}
