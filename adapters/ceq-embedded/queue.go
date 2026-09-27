package ceqembedded

import (
	"context"

	"github.com/ai-sea/elastic-harness/core/ports"
)

// Queue 是 standalone 使用的有界 Chat Event 队列。
type Queue struct {
	messages chan ports.Message
}

func New(capacity int) *Queue {
	if capacity <= 0 {
		capacity = 256
	}
	return &Queue{messages: make(chan ports.Message, capacity)}
}

func (q *Queue) Publish(ctx context.Context, key string, body []byte) error {
	message := ports.Message{Key: key, Body: append([]byte(nil), body...)}
	select {
	case q.messages <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) Subscribe(context.Context) (<-chan ports.Message, error) {
	return q.messages, nil
}
