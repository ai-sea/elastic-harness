package projector

import (
	"context"
	"fmt"

	"github.com/ai-sea/elastic-harness/core/ports"
)

// OutboxRelay 只在发布成功后标记记录；崩溃窗口最多造成重复提示。
type OutboxRelay struct {
	store  ports.StateStore
	queues map[string]ports.Queue
}

func NewOutboxRelay(store ports.StateStore, queues map[string]ports.Queue) *OutboxRelay {
	return &OutboxRelay{store: store, queues: queues}
}

func (r *OutboxRelay) RelayOnce(ctx context.Context, limit int) (int, error) {
	records, err := r.store.PendingOutbox(ctx, limit)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, record := range records {
		queue, exists := r.queues[record.Channel]
		if !exists {
			return published, fmt.Errorf("Outbox channel %q 未绑定队列", record.Channel)
		}
		if err := queue.Publish(ctx, record.Key, record.Payload); err != nil {
			return published, err
		}
		if err := r.store.MarkOutboxPublished(ctx, record.ID); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}
