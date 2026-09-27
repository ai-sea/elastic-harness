package toolexecutor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
)

type Executor struct {
	tools     ports.ToolExecutor
	artifacts ports.ArtifactStore
}

func New(tools ports.ToolExecutor, artifacts ports.ArtifactStore) *Executor {
	return &Executor{tools: tools, artifacts: artifacts}
}

func (e *Executor) Execute(ctx context.Context, entry effects.EffectLedgerEntry) (string, string, error) {
	if entry.Kind.String() != "harness/tool.invoke" {
		return "", "", errors.New("不支持的 Effect kind")
	}
	var call ports.ToolCall
	if err := json.Unmarshal(entry.Intent, &call); err != nil {
		return "", "", err
	}
	result, err := e.tools.Invoke(ctx, ports.ToolRequest{
		EffectID: entry.EffectID, IdempotencyKey: entry.IdempotencyKey,
		TenantID: entry.TenantID, Tool: call.Tool, Input: call.Arguments,
	})
	if err != nil {
		return "", "", err
	}
	if result.Async {
		return result.ExternalExecutionRef, "", errors.New("Phase 1 standalone 不接受未完成的异步工具结果")
	}
	resultRef, err := e.artifacts.Put(ctx, entry.TenantID, bytes.NewReader(result.Result))
	if err != nil {
		return result.ExternalExecutionRef, "", err
	}
	return result.ExternalExecutionRef, resultRef, nil
}

func (e *Executor) Recover(ctx context.Context, entry effects.EffectLedgerEntry) (string, string, bool, error) {
	if entry.ExternalRef == "" {
		return "", "", false, nil
	}
	result, completed, err := e.tools.Recover(ctx, entry.ExternalRef)
	if err != nil || !completed {
		return entry.ExternalRef, "", completed, err
	}
	resultRef, err := e.artifacts.Put(ctx, entry.TenantID, bytes.NewReader(result.Result))
	if err != nil {
		return entry.ExternalRef, "", false, err
	}
	return entry.ExternalRef, resultRef, true, nil
}
