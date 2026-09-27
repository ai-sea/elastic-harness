package domain

import (
	"testing"
	"time"
)

func TestRunSnapshotValidateTerminalInvariant(t *testing.T) {
	base := RunSnapshot{
		TenantID:        "tenant-1",
		ChatID:          "chat-1",
		RunID:           "run-1",
		LifecycleStatus: LifecycleRunnable,
		CurrentState:    "start",
		Harness:         HarnessRef{ID: "agent", Version: "1.0.0"},
	}

	if err := base.Validate(); err != nil {
		t.Fatalf("有效快照不应校验失败：%v", err)
	}

	reason := TerminalCompleted
	base.TerminalReason = &reason
	if err := base.Validate(); err == nil {
		t.Fatal("非终态快照不得携带 terminalReason")
	}

	base.LifecycleStatus = LifecycleTerminal
	if err := base.Validate(); err != nil {
		t.Fatalf("终态快照携带 terminalReason 时应有效：%v", err)
	}
}

func TestBudgetExhausted(t *testing.T) {
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		budget Budget
		want   bool
	}{
		{name: "仍有预算", budget: Budget{MaxSteps: 2, UsedSteps: 1, Deadline: now.Add(time.Minute)}},
		{name: "步骤耗尽", budget: Budget{MaxSteps: 2, UsedSteps: 2}, want: true},
		{name: "令牌耗尽", budget: Budget{MaxTokens: 5, UsedTokens: 5}, want: true},
		{name: "费用耗尽", budget: Budget{MaxCostMicros: 5, UsedCostMicros: 5}, want: true},
		{name: "截止时间到达", budget: Budget{Deadline: now}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.budget.Exhausted(now); got != test.want {
				t.Fatalf("Exhausted() = %v，期望 %v", got, test.want)
			}
		})
	}
}
