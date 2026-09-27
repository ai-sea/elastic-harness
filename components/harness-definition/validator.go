package definition

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ai-sea/elastic-harness/core/domain"
)

var ErrInvalidDefinition = errors.New("Harness Definition 无效")

// ValidationError 汇总全部发布错误，使调用方一次即可修复完整定义。
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%v：%s", ErrInvalidDefinition, strings.Join(e.Problems, "；"))
}

func (e *ValidationError) Unwrap() error { return ErrInvalidDefinition }

// Validator 执行与运行环境无关的发布校验，不检查当前是否存在 Handler。
type Validator struct{}

func (Validator) Validate(value domain.HarnessDefinition) error {
	problems := make([]string, 0)
	if value.ID == "" {
		problems = append(problems, "id 不能为空")
	}
	if value.Version == "" {
		problems = append(problems, "version 不能为空")
	}
	if value.Initial == "" {
		problems = append(problems, "initial 不能为空")
	}
	if len(value.States) == 0 {
		problems = append(problems, "states 不能为空")
	}
	if _, ok := value.States[value.Initial]; value.Initial != "" && !ok {
		problems = append(problems, fmt.Sprintf("入口状态 %q 不存在", value.Initial))
	}

	terminalCount := 0
	stateNames := make([]string, 0, len(value.States))
	for name := range value.States {
		stateNames = append(stateNames, name)
	}
	sort.Strings(stateNames)
	for _, name := range stateNames {
		node := value.States[name]
		if node.Type == domain.StateTerminal {
			terminalCount++
			if len(node.Transitions) != 0 {
				problems = append(problems, fmt.Sprintf("终态 %q 不得声明迁移", name))
			}
			continue
		}
		if node.Requires.Capability.IsZero() {
			problems = append(problems, fmt.Sprintf("状态 %q 缺少 capability", name))
		} else if err := node.Requires.Capability.Validate(); err != nil {
			problems = append(problems, fmt.Sprintf("状态 %q capability 无效：%v", name, err))
		}
		if node.Requires.Version == "" {
			problems = append(problems, fmt.Sprintf("状态 %q 缺少 capability 版本范围", name))
		}
		if node.SideEffect == "" {
			problems = append(problems, fmt.Sprintf("状态 %q 必须显式声明 sideEffect", name))
		}
		if node.Timeouts.State <= 0 && node.Timeouts.StartToClose <= 0 {
			problems = append(problems, fmt.Sprintf("状态 %q 缺少超时覆盖", name))
		}
		if node.Retry.MaxAttempts < 0 {
			problems = append(problems, fmt.Sprintf("状态 %q 的 maxAttempts 不得为负数", name))
		}
		for outcome, target := range node.Transitions {
			if outcome == "" || target == "" {
				problems = append(problems, fmt.Sprintf("状态 %q 存在空迁移", name))
				continue
			}
			if _, ok := value.States[target]; !ok {
				problems = append(problems, fmt.Sprintf("状态 %q 的迁移目标 %q 不存在", name, target))
			}
		}
	}
	if terminalCount == 0 {
		problems = append(problems, "至少需要一个终态")
	}
	problems = append(problems, unreachableStates(value)...)
	problems = append(problems, automaticCycles(value)...)
	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

func unreachableStates(value domain.HarnessDefinition) []string {
	if _, ok := value.States[value.Initial]; !ok {
		return nil
	}
	visited := map[string]bool{}
	queue := []string{value.Initial}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if visited[name] {
			continue
		}
		visited[name] = true
		for _, target := range value.States[name].Transitions {
			queue = append(queue, target)
		}
	}
	problems := make([]string, 0)
	for name := range value.States {
		if !visited[name] {
			problems = append(problems, fmt.Sprintf("状态 %q 不可达", name))
		}
	}
	sort.Strings(problems)
	return problems
}

func automaticCycles(value domain.HarnessDefinition) []string {
	visiting := map[string]bool{}
	visited := map[string]bool{}
	problems := make([]string, 0)
	var visit func(string)
	visit = func(name string) {
		if visiting[name] {
			problems = append(problems, fmt.Sprintf("检测到无界自动循环，包含状态 %q", name))
			return
		}
		if visited[name] {
			return
		}
		node, ok := value.States[name]
		if !ok || node.Type == domain.StateWait || node.Type == domain.StateTerminal {
			visited[name] = true
			return
		}
		visiting[name] = true
		for _, target := range node.Transitions {
			visit(target)
		}
		delete(visiting, name)
		visited[name] = true
	}
	visit(value.Initial)
	return problems
}
