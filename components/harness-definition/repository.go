package definition

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
)

// Repository 是线程安全的不可变版本仓库，适用于 standalone 组合。
type Repository struct {
	mu        sync.RWMutex
	drafts    map[string]domain.HarnessDefinition
	published map[string]domain.HarnessDefinition
	validator Validator
}

func NewRepository() *Repository {
	return &Repository{
		drafts:    make(map[string]domain.HarnessDefinition),
		published: make(map[string]domain.HarnessDefinition),
	}
}

func (r *Repository) PutDraft(_ context.Context, value domain.HarnessDefinition) error {
	if value.ID == "" || value.Version == "" {
		return fmt.Errorf("%w：草稿缺少 id 或 version", ErrInvalidDefinition)
	}
	key := definitionKey(value.ID, value.Version)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.published[key]; exists {
		return fmt.Errorf("%w：已发布版本不可修改", ports.ErrConflict)
	}
	value.Published = false
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	r.drafts[key] = cloneDefinition(value)
	return nil
}

func (r *Repository) Publish(_ context.Context, id, version string) (domain.HarnessDefinition, error) {
	key := definitionKey(id, version)
	r.mu.Lock()
	defer r.mu.Unlock()
	if value, exists := r.published[key]; exists {
		return cloneDefinition(value), nil
	}
	value, exists := r.drafts[key]
	if !exists {
		return domain.HarnessDefinition{}, ports.ErrNotFound
	}
	if err := r.validator.Validate(value); err != nil {
		return domain.HarnessDefinition{}, err
	}
	value.Published = true
	r.published[key] = cloneDefinition(value)
	delete(r.drafts, key)
	return cloneDefinition(value), nil
}

func (r *Repository) Get(_ context.Context, id, version string) (domain.HarnessDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, exists := r.published[definitionKey(id, version)]
	if !exists {
		return domain.HarnessDefinition{}, ports.ErrNotFound
	}
	return cloneDefinition(value), nil
}

func definitionKey(id, version string) string { return id + "@" + version }

func cloneDefinition(value domain.HarnessDefinition) domain.HarnessDefinition {
	copyValue := value
	copyValue.States = make(map[string]domain.StateNode, len(value.States))
	for name, node := range value.States {
		copyNode := node
		copyNode.Requires.Features = append([]string(nil), node.Requires.Features...)
		copyNode.Transitions = make(map[string]string, len(node.Transitions))
		for outcome, target := range node.Transitions {
			copyNode.Transitions[outcome] = target
		}
		copyNode.Config = append([]byte(nil), node.Config...)
		copyValue.States[name] = copyNode
	}
	return copyValue
}
