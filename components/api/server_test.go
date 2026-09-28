package api

import (
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
)

func TestBudgetDeadlineIsCalculatedWhenRunIsCreated(t *testing.T) {
	now := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	server := &Server{
		defaults: Defaults{
			Budget: domain.Budget{MaxSteps: 32, Deadline: now.Add(-time.Hour)},
			RunTTL: time.Hour,
		},
		now: func() time.Time { return now },
	}

	budget := server.budgetForRun()
	if want := now.Add(time.Hour); !budget.Deadline.Equal(want) {
		t.Fatalf("Run 截止时间 = %s，期望按创建时刻计算为 %s", budget.Deadline, want)
	}
	if budget.MaxSteps != 32 {
		t.Fatalf("计算截止时间不得丢失其他预算：%+v", budget)
	}
}
