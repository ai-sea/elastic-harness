package modelprovideropenai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

const maxResponseBytes = 8 << 20

type Options struct {
	Endpoint string
	APIKey   string
	Model    string
	Client   *http.Client
}

type Provider struct {
	options Options
}

func New(options Options) (*Provider, error) {
	if options.Endpoint == "" || options.Model == "" {
		return nil, errors.New("OpenAI-compatible Endpoint 和 Model 不能为空")
	}
	if options.Client == nil {
		options.Client = &http.Client{Timeout: 120 * time.Second}
	}
	return &Provider{options: options}, nil
}

func (p *Provider) Invoke(ctx context.Context, request ports.ModelRequest) (ports.ModelResponse, error) {
	model := request.Model
	if model == "" {
		model = p.options.Model
	}
	toolNames := make(map[string]qname.QName, len(request.Tools))
	tools := make([]map[string]any, 0, len(request.Tools))
	for _, tool := range request.Tools {
		providerName := encodeToolName(tool.Name.String())
		toolNames[providerName] = tool.Name
		var schema any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
			return ports.ModelResponse{}, fmt.Errorf("工具 %s schema 无效：%w", tool.Name, err)
		}
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{
			"name": providerName, "description": tool.Description, "parameters": schema,
		}})
	}
	payload := map[string]any{
		"model": model, "messages": []map[string]string{{"role": "user", "content": request.Prompt}},
	}
	if len(tools) > 0 {
		payload["tools"] = tools
	}
	body, _ := json.Marshal(payload)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, p.options.Endpoint, bytes.NewReader(body))
	if err != nil {
		return ports.ModelResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if p.options.APIKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+p.options.APIKey)
	}
	response, err := p.options.Client.Do(httpRequest)
	if err != nil {
		return ports.ModelResponse{}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return ports.ModelResponse{}, err
	}
	if len(responseBody) > maxResponseBytes {
		return ports.ModelResponse{}, errors.New("模型响应超过大小限制")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ports.ModelResponse{}, fmt.Errorf("模型返回 HTTP %d：%s", response.StatusCode, responseBody)
	}
	return decodeResponse(responseBody, toolNames, model)
}

func decodeResponse(body []byte, toolNames map[string]qname.QName, requestedModel string) (ports.ModelResponse, error) {
	var envelope struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string          `json:"name"`
						Arguments json.RawMessage `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ports.ModelResponse{}, err
	}
	if len(envelope.Choices) == 0 {
		return ports.ModelResponse{}, errors.New("模型响应缺少 choices")
	}
	message := envelope.Choices[0].Message
	result := ports.ModelResponse{
		Content: message.Content, Provider: "openai-compatible", Model: requestedModel,
		ModelVersion: envelope.Model, Usage: domain.Usage{Tokens: envelope.Usage.TotalTokens},
	}
	for _, call := range message.ToolCalls {
		name, exists := toolNames[call.Function.Name]
		if !exists {
			return ports.ModelResponse{}, fmt.Errorf("模型请求了未注册工具 %q", call.Function.Name)
		}
		result.ToolCalls = append(result.ToolCalls, ports.ToolCall{
			CallID: call.ID, Tool: name, Arguments: append(json.RawMessage(nil), call.Function.Arguments...),
		})
	}
	return result, nil
}

func encodeToolName(name string) string {
	name = strings.ReplaceAll(name, "/", "__")
	return strings.ReplaceAll(name, ".", "_dot_")
}
