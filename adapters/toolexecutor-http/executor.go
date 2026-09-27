package toolexecutorhttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ai-sea/elastic-harness/core/ports"
)

const maxResultBytes = 4 << 20

type Executor struct {
	catalog ports.ToolCatalog
	client  *http.Client
}

func New(catalog ports.ToolCatalog, client *http.Client) *Executor {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Executor{catalog: catalog, client: client}
}

func (e *Executor) Invoke(ctx context.Context, request ports.ToolRequest) (ports.ToolResult, error) {
	registration, err := e.catalog.ResolveTool(ctx, request.TenantID, request.Tool)
	if err != nil {
		return ports.ToolResult{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, registration.Endpoint, bytes.NewReader(request.Input))
	if err != nil {
		return ports.ToolResult{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Idempotency-Key", request.IdempotencyKey)
	response, err := e.client.Do(httpRequest)
	if err != nil {
		return ports.ToolResult{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResultBytes+1))
	if err != nil {
		return ports.ToolResult{}, err
	}
	if len(body) > maxResultBytes {
		return ports.ToolResult{}, fmt.Errorf("工具结果超过 %d 字节", maxResultBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ports.ToolResult{}, fmt.Errorf("工具返回 HTTP %d：%s", response.StatusCode, body)
	}
	externalRef := response.Header.Get("X-Execution-Ref")
	if externalRef == "" {
		externalRef = request.EffectID
	}
	return ports.ToolResult{ExternalExecutionRef: externalRef, Result: body}, nil
}

func (e *Executor) Recover(context.Context, string) (ports.ToolResult, bool, error) {
	return ports.ToolResult{}, false, nil
}
