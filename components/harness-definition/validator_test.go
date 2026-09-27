package definition

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func validDefinition() domain.HarnessDefinition {
	return domain.HarnessDefinition{
		ID: "test-agent", Version: "1.0.0", Initial: "model",
		States: map[string]domain.StateNode{
			"model": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/llm.invoke"), Version: "^1"},
				Timeouts:    domain.Timeouts{StartToClose: time.Minute},
				Retry:       domain.RetryPolicy{MaxAttempts: 2},
				Transitions: map[string]string{"completed": "done"},
			},
			"done": {Type: domain.StateTerminal},
		},
	}
}

func TestValidatorAcceptsValidDefinition(t *testing.T) {
	if err := (Validator{}).Validate(validDefinition()); err != nil {
		t.Fatalf("有效定义发布校验失败：%v", err)
	}
}

func TestValidatorRejectsAutomaticCycle(t *testing.T) {
	value := validDefinition()
	value.States["model"] = domain.StateNode{
		Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
		Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/llm.invoke"), Version: "^1"},
		Timeouts:    domain.Timeouts{StartToClose: time.Minute},
		Transitions: map[string]string{"completed": "model"},
	}
	delete(value.States, "done")
	if err := (Validator{}).Validate(value); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("无界循环应被拒绝，得到：%v", err)
	}
}

func TestPublishedVersionIsImmutable(t *testing.T) {
	repository := NewRepository()
	value := validDefinition()
	if err := repository.PutDraft(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Publish(context.Background(), value.ID, value.Version); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutDraft(context.Background(), value); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("覆盖已发布版本应冲突，得到：%v", err)
	}
}
