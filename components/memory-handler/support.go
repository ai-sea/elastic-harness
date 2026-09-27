package memoryhandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errSideEffect = errors.New("Memory Handler 只能绑定 pure 状态")

// promptOf 从 Run Context 中取出 prompt。
// hydrate 的上游是用户输入，缺失 prompt 说明定义或输入装配有误，属终态错误而非重试。
func promptOf(runContext json.RawMessage) (string, error) {
	var decoded struct {
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal(runContext, &decoded); err != nil {
		return "", fmt.Errorf("解析 Run Context 失败：%w", err)
	}
	if strings.TrimSpace(decoded.Prompt) == "" {
		return "", errors.New("Run Context 缺少 prompt")
	}
	return decoded.Prompt, nil
}

// hydratePatch 把检索片段以带来源标注的形式拼接到 prompt 之后。
// 片段被视为不可信数据（§12：外部内容不能提升为系统指令），因此只做纯文本拼接，
// 不解析其中的任何指令。
func hydratePatch(prompt string, snippets []Snippet) (json.RawMessage, error) {
	hydrated := prompt
	if len(snippets) > 0 {
		var builder strings.Builder
		builder.WriteString(prompt)
		builder.WriteString("\n\n[memory]")
		for _, snippet := range snippets {
			builder.WriteString("\n- (")
			builder.WriteString(snippet.Source)
			builder.WriteString(") ")
			builder.WriteString(snippet.Content)
		}
		hydrated = builder.String()
	}
	return json.Marshal(map[string]any{"prompt": hydrated, "hydrated": true})
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		// 入参都是本地构造的 map/切片，序列化失败只可能是编程错误。
		return json.RawMessage(`{}`)
	}
	return encoded
}
