module github.com/ai-sea/elastic-harness/apps/standalone-app

// SPDX-License-Identifier: MIT

go 1.24

require (
	github.com/ai-sea/elastic-harness/adapters/ceq-embedded v0.0.0
	github.com/ai-sea/elastic-harness/adapters/kv-sqlite v0.0.0
	github.com/ai-sea/elastic-harness/adapters/objectstore-local v0.0.0
	github.com/ai-sea/elastic-harness/adapters/seq-embedded v0.0.0
	github.com/ai-sea/elastic-harness/adapters/toolexecutor-http v0.0.0
	github.com/ai-sea/elastic-harness/components/api v0.0.0
	github.com/ai-sea/elastic-harness/components/effect-dispatcher v0.0.0
	github.com/ai-sea/elastic-harness/components/event-projector v0.0.0
	github.com/ai-sea/elastic-harness/components/execution-profile v0.0.0
	github.com/ai-sea/elastic-harness/components/handler-registry v0.0.0
	github.com/ai-sea/elastic-harness/components/harness-definition v0.0.0
	github.com/ai-sea/elastic-harness/components/llm-handler v0.0.0
	github.com/ai-sea/elastic-harness/components/onstate-runtime v0.0.0
	github.com/ai-sea/elastic-harness/components/timer-reconciler v0.0.0
	github.com/ai-sea/elastic-harness/components/tool-executor v0.0.0
	github.com/ai-sea/elastic-harness/components/tool-handler v0.0.0
	github.com/ai-sea/elastic-harness/components/tool-registry v0.0.0
	github.com/ai-sea/elastic-harness/core v0.0.0
)
