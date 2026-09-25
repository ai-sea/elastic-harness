# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 仓库当前状态

**架构已定稿，代码尚未生成。** 根目录无 `go.mod` / `package.json` / `Makefile`，仅有 `README.md` 与 `docs/arch.md`（v1.0，Phase 1 基线）。所有架构结论以 `docs/arch.md` 为单一事实源；架构变更**先改文档再改代码**。

Phase 1 = 最小可恢复闭环（Chat/Run/Harness Definition IR/`onState` 内核/Handler Registry+JWT/双队列/LLM+Tool Handler/Effect Dispatcher/Outbox/SSE/对象产物/取消与基础重试/端到端故障注入），范围见 `docs/arch.md` §16。Phase 1 已确认按本期文档**全量实施**，非极简切片。

## 架构核心入口（按需查阅，不通读）

未来修改前先确认是否触及相应章节，再读章节定位决策。

- **执行模型与内核契约**：`docs/arch.md` §5（持久化状态机、`onState`、Effect 两相派发、sideEffect 三档）
- **数据模型与一致性边界**：`docs/arch.md` §6 + §8（KV 权威状态、双队列职责分离、Run Snapshot、Inbox、Effect Ledger、Timer）
- **可插拔端口与适配器**：`docs/arch.md` §7（Port 契约 + 中心 Registry + JWT 鉴权 + 调度式解析）
- **系统不变量**：`docs/arch.md` §11.2 — 13 条不变量是契约测试的验证目标，**任何实现变更不得违反**
- **风险与故障注入清单**：`docs/arch.md` §17 — Phase 1 验收的故障场景来源
- **决策摘要与术语表**：`docs/arch.md` §18 / §19 — 改动前自查"是否已偏离既有 ADR"

## 代码组织（Phase 1 编码时遵循）

实现语言为 Go，单仓多 module（`docs/arch.md` §15）。规划布局：

- `core/` — 零基础设施依赖（不 import 任何 adapter、云 SDK、Web 框架、数据库客户端）
- `components/<name>/` — 每能力组件一个 module；只依赖 `core` 与更底层组件
- `adapters/<name>/` — 基础设施扩展（`kv-sqlite`、`kv-foundationdb`、`ceq-kafka`、`objectstore-s3` 等）；实现 `core/ports`
- `apps/standalone-app/` 与 `apps/cloudnative-app/` — 官方组合根范本（仅这两个允许 import adapter）
- `deploy/` — 第三层部署拓扑

**关键约束**（§15）：
1. 依赖单向：`core` 不 import 任何 `adapters/*`；只有组合根允许 import adapter
2. 不使用 Go `.so` plugin — 扩展点在编译期由组合根决定
3. 跨 module 依赖固定到已发布版本，本地用 `go.work` + `replace` 联调
4. 组件测试必须含 §14.1 三类：领域单测 + Port 契约测试 + 消息 schema 兼容性测试；声明 `idempotent: true` 的组件必须附重复调用测试

## 实现期必须遵守的硬性约束（从 arch.md 抽取）

这些规则如果违反，从代码表面看不明显，但会破坏正确性：

- **CAS 是唯一裁决点**：Run 状态以 `stateVersion` 为条件，Effect 派发以 `effectId + ledgerVersion` 为条件，两种 CAS 相互独立（不变量 #1, #4, #11）
- **改变世界必须两相派发**：意图先随事务落账为 `EffectLedger(PENDING)`，提交成功后才调 Provider；**仅** `sideEffect: pure` 可内联直调；`idempotent` 同样必须两相（不变量 #9 + §5.3.2）
- **内部推进不脱管**：`RUNNABLE` 状态必须同事务写入 `entered` Signal 到 Inbox，**不允许绕过 Inbox 自唤醒**（不变量 #13 + §5.3 自续跑）
- **Inbox 是权威来源**：StateEventQueue 只是提示，可丢；Reconciler 按 Inbox 兜底（§5.3.1）
- **定时器判据用 `stateEnterCounter` 而非 `stateVersion`**——同状态内迁移也会推进 `stateVersion`，会误杀有效 Timer（§8.7）
- **JWT 必须非对称**：仅 EdDSA/ES256，拒 `none` 与 HMAC；JWKS 离线验签（§7.2 + §12）
- **Run Snapshot 不内嵌待处理列表**：Invocation / Timer / Effect 各自成键，Snapshot 只保留计数与最老水位；长 Run 上无界膨胀会破坏 CAS 性能（§6.1）
- **Handler 不得设置 `lifecycleStatus` / `terminalReason`**：内核由 `StateOutcome` + Transition Rule 派生，Handler 绕过即破坏控制流（§5.2）

## 变更约定

- 文档是单一事实源（README 底部脚注）；架构/语义/不变量变更必须**先改 `docs/arch.md` 再改代码**
- 中文是仓库母语；commit message 沿用中文+简要英文术语（如 `docs:`、`fix:`、`feat:`、`refactor:` 前缀），按改动范围给一句话要点
- 单分支 `main`；本仓库直接推送不强制 PR（规模与评审机制尚未建立）