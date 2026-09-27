package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
)

// RunLocator 把事件里的 Run 身份解析为它所属的 Chat。
//
// 领域事件只携带 `correlationId = runId`（§8.2），而 ChatView 的键是
// `tenantId/chatId`。Phase 1 直接查权威状态完成这次解析——投影是派生数据，
// 但它的键空间来自权威状态，不维护第二份映射表。
type RunLocator interface {
	GetRun(context.Context, string) (domain.RunSnapshot, error)
}

// ViewSink 是投影结果的落点，以"原子读改写"为契约。
//
// 不暴露 Load/Save 两个方法是有意的：拆开会让调用方可能漏掉写回，
// 或在两次调用之间交叠另一次折叠而丢事件。把折叠函数交给落点执行，
// 锁的粒度与一致性由落点一处负责。
type ViewSink interface {
	Update(ctx context.Context, tenantID, chatID string, fold func(*ChatView) (bool, error)) (bool, error)
}

// Projection 把 ChatEventQueue 的事件折叠进 ChatView。
type Projection struct {
	locator RunLocator
	sink    ViewSink
}

func NewProjection(locator RunLocator, sink ViewSink) *Projection {
	return &Projection{locator: locator, sink: sink}
}

// ConsumeOnce 处理至多一批消息，返回已应用（视图发生变化）的条数。
//
// 单条消息处理失败不中断整批：队列是至少一次投递，卡在一条毒消息上会让
// 整个投影停摆。失败经 errors.Join 汇总上报，由调用方记录并告警——
// 已成功的部分不回滚，因为投影本身是可重建的派生数据。
func (p *Projection) ConsumeOnce(ctx context.Context, messages []ports.Message) (int, error) {
	applied := 0
	var failures []error
	for _, message := range messages {
		changed, err := p.ApplyMessage(ctx, message)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if changed {
			applied++
		}
	}
	return applied, errors.Join(failures...)
}

// ApplyMessage 处理单条消息并返回本次是否有变更。
func (p *Projection) ApplyMessage(ctx context.Context, message ports.Message) (bool, error) {
	var event domain.EventEnvelope
	if err := json.Unmarshal(message.Body, &event); err != nil {
		return false, fmt.Errorf("解析事件 %s 失败：%w", message.Key, err)
	}
	return p.applyEvent(ctx, event)
}

func (p *Projection) applyEvent(ctx context.Context, event domain.EventEnvelope) (bool, error) {
	if event.CorrelationID == "" {
		return false, errors.New("事件缺少 correlationId，无法归属到 Run")
	}
	run, err := p.locator.GetRun(ctx, event.CorrelationID)
	if err != nil {
		return false, fmt.Errorf("解析 Run %s 归属失败：%w", event.CorrelationID, err)
	}
	return p.sink.Update(ctx, run.TenantID, run.ChatID, func(view *ChatView) (bool, error) {
		return view.Apply(event)
	})
}

// MemorySink 是 standalone 的进程内视图存储，按 chat 串行化读改写。
// 它不承诺持久化——Phase 1 的投影是可重建的派生数据（§6.2）。
type MemorySink struct {
	mutex sync.Mutex
	views map[string]ChatView
}

func NewMemorySink() *MemorySink { return &MemorySink{views: map[string]ChatView{}} }

func (s *MemorySink) Update(_ context.Context, tenantID, chatID string, fold func(*ChatView) (bool, error)) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	key := tenantID + "/" + chatID
	view, exists := s.views[key]
	if !exists {
		view = NewChatView(tenantID, chatID)
	}
	changed, err := fold(&view)
	if err != nil || !changed {
		return false, err
	}
	s.views[key] = view
	return true, nil
}

// View 读取当前视图快照，仅供查询与测试。返回副本，避免调用方
// 直接改写内部 map 与切片而与折叠竞争。
func (s *MemorySink) View(tenantID, chatID string) (ChatView, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	view, exists := s.views[tenantID+"/"+chatID]
	if !exists {
		return ChatView{}, false
	}
	return cloneView(view), true
}

func cloneView(view ChatView) ChatView {
	cloned := view
	cloned.Messages = append([]ViewMessage(nil), view.Messages...)
	cloned.ActiveRuns = append([]string(nil), view.ActiveRuns...)
	cloned.RunCursors = make(map[string]int64, len(view.RunCursors))
	for runID, sequence := range view.RunCursors {
		cloned.RunCursors[runID] = sequence
	}
	return cloned
}
