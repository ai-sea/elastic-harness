package executionprofile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
)

type Repository struct {
	mu        sync.RWMutex
	profiles  map[string]domain.ExecutionProfile
	artifacts ports.ArtifactStore
}

func New(artifacts ports.ArtifactStore) *Repository {
	return &Repository{profiles: make(map[string]domain.ExecutionProfile), artifacts: artifacts}
}

func (r *Repository) Publish(ctx context.Context, tenantID string, profile domain.ExecutionProfile) (domain.ProfileRef, error) {
	if profile.ID == "" || profile.Revision <= 0 || len(profile.AllowedNamespaces) == 0 {
		return domain.ProfileRef{}, errors.New("Execution Profile 缺少 ID、Revision 或允许的命名空间")
	}
	if profile.Budget.MaxSteps <= 0 {
		return domain.ProfileRef{}, errors.New("Execution Profile 必须设置正数 maxSteps")
	}
	key := fmt.Sprintf("%s@%d", profile.ID, profile.Revision)
	r.mu.Lock()
	if _, exists := r.profiles[key]; exists {
		r.mu.Unlock()
		return domain.ProfileRef{}, ports.ErrConflict
	}
	profile.Published = true
	r.profiles[key] = profile
	r.mu.Unlock()
	body, err := json.Marshal(profile)
	if err != nil {
		return domain.ProfileRef{}, err
	}
	reference, err := r.artifacts.Put(ctx, tenantID, bytes.NewReader(body))
	if err != nil {
		return domain.ProfileRef{}, err
	}
	return domain.ProfileRef{ID: profile.ID, Revision: profile.Revision, SnapshotRef: reference}, nil
}

func (r *Repository) Get(id string, revision int64) (domain.ExecutionProfile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	profile, exists := r.profiles[fmt.Sprintf("%s@%d", id, revision)]
	if !exists {
		return domain.ExecutionProfile{}, ports.ErrNotFound
	}
	return profile, nil
}
