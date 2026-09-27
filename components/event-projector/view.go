// Package projector 提供 Phase 1 的最小投影：
// 把 ChatEventQueue 上的领域事件折叠为 §6.1 的 ChatView 视图。
//
// 投影是**可重建**的派生数据：EventIndex 才是权威来源（§6.2 丢失影响一栏），
// 因此这里的每一次折叠都必须是重复投递安全的，否则一次重投就会污染视图。
package projector

import (
	"encoding/json"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// 事件类型在投影中被反复比对，提前解析一次以避免每次投递都走正则解析。
var (
	eventMessageCompleted = qname.MustParse("harness/message.completed")
	eventStateTransition  = qname.MustParse("harness/state.transitioned")
)

// ChatView 是 §6.1 `ChatView/{tenantId}/{chatId}` 的逻辑视图：
// 消息索引、活跃 Run 与每个 Run 流的最新游标。
type ChatView struct {
	TenantID   string           `json:"tenantId"`
	ChatID     string           `json:"chatId"`
	Messages   []ViewMessage    `json:"messages"`
	ActiveRuns []string         `json:"activeRuns"`
	RunCursors map[string]int64 `json:"runCursors"`
	LastActive string           `json:"lastActiveAt,omitempty"`
}

// ViewMessage 是投影后的消息，不带 Run 内部状态。
type ViewMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	RunID   string `json:"runId,omitempty"`
}

func NewChatView(tenantID, chatID string) ChatView {
	return ChatView{TenantID: tenantID, ChatID: chatID, RunCursors: map[string]int64{}}
}

// Apply 把一条事件折叠进视图，返回视图是否发生变化。
//
// 幂等性来源有二，缺一不可（§17 风险 5）：
//  1. `sequence` 必须严格大于该 Run 流已见水位，否则视为乱序/回放直接丢弃；
//  2. 只有 `assistant` 角色的最终消息才写入 Messages，中间增量不进视图。
//
// 顺序上先解析、后落状态：载荷非法时返回错误且**不推进水位**，
// 这样重复投递可以安全重试，不会把一条读不懂的事件永久跳过。
func (v *ChatView) Apply(event domain.EventEnvelope) (bool, error) {
	if v.RunCursors == nil {
		v.RunCursors = map[string]int64{}
	}
	if !v.accepts(event) {
		return false, nil
	}
	message, hasMessage, err := foldMessage(event)
	if err != nil {
		return false, err
	}
	lifecycle, err := foldLifecycle(event)
	if err != nil {
		return false, err
	}
	v.commit(event, message, hasMessage, lifecycle)
	return true, nil
}

// accepts 实现不变量 #5：单个 Run 流内 sequence 严格单调，已提交事件不得乱序或被改写。
// 迟到或重复事件只被忽略，不报错——它们本来就可能是至少一次投递的产物。
func (v *ChatView) accepts(event domain.EventEnvelope) bool {
	if event.CorrelationID == "" {
		return false
	}
	return event.Sequence > v.RunCursors[event.CorrelationID]
}

func (v *ChatView) commit(event domain.EventEnvelope, message ViewMessage, hasMessage bool, lifecycle string) {
	v.RunCursors[event.CorrelationID] = event.Sequence
	v.LastActive = event.OccurredAt.UTC().Format("2006-01-02T15:04:05Z")
	if hasMessage {
		v.Messages = append(v.Messages, message)
	}
	switch lifecycle {
	case "":
		return
	case string(domain.LifecycleTerminal):
		v.ActiveRuns = remove(v.ActiveRuns, event.CorrelationID)
	default:
		v.ActiveRuns = appendUnique(v.ActiveRuns, event.CorrelationID)
	}
}

// foldMessage 从 `message.completed` 事件中提取可入视图的 assistant 消息。
func foldMessage(event domain.EventEnvelope) (ViewMessage, bool, error) {
	if !event.EventType.Equal(eventMessageCompleted) {
		return ViewMessage{}, false, nil
	}
	var payload struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ViewMessage{}, false, err
	}
	if payload.Role != "assistant" {
		return ViewMessage{}, false, nil
	}
	return ViewMessage{Role: payload.Role, Content: payload.Content, RunID: event.CorrelationID}, true, nil
}

// foldLifecycle 从内核派生的 `state.transitioned` 事件中读出生命周期。
// 投影只读不推断——"Run 是否活跃"由内核裁决，不由投影猜测。
func foldLifecycle(event domain.EventEnvelope) (string, error) {
	if !event.EventType.Equal(eventStateTransition) {
		return "", nil
	}
	var payload struct {
		LifecycleStatus string `json:"lifecycleStatus"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return "", err
	}
	return payload.LifecycleStatus, nil
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// remove 返回新切片而不复用底层数组：view 可能是被复制的值，
// 原地裁剪会改写共享的 backing array。
func remove(values []string, value string) []string {
	result := make([]string, 0, len(values))
	for _, existing := range values {
		if existing != value {
			result = append(result, existing)
		}
	}
	return result
}
