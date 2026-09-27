# Phase 1 实施与验收账本

> 唯一需求来源：`docs/arch.md` v1.0。本文只跟踪实施，不修改架构结论。

## 完成定义

Phase 1 只有同时满足以下条件才算完成：

1. `go test` 覆盖全部 workspace module 并通过；✅（20 个 module 全绿）
2. standalone 应用可生成单一二进制；✅（`scripts/build.ps1` → `bin/elastic-harness.exe`，已验证编译通过）
3. E2E 自动化测试走通 Chat → Message → Run → LLM → Tool → 最终消息；✅（`apps/standalone-app/e2e_test.go: TestAgentLoopEndToEnd`）
4. 故障注入覆盖重复消息、Worker 中断、丢失提示/回调、完成与超时竞争、Reconciler 恢复；✅（`apps/standalone-app/fault_test.go`，7 个场景）
5. `docs/arch.md` §11.2 的 13 条不变量均有契约测试或 E2E 断言；✅（见下方不变量覆盖表）
6. 示例配置、启动方式与 API 调用方法已写入 README。✅（README「快速开始（standalone）」一节）

## 不变量覆盖表（§11.2）

| # | 不变量 | 验证位置 |
|---|--------|----------|
| 1 | Run CAS 唯一裁决 | onstate-runtime `TestRunCASAndFencingTokenAreTheOnlyCommitAuthority` + fault 迟到提交测试 |
| 2 | 终态不可逆 | onstate-runtime `TestTerminalStateCannotBeRevived` + fault `TestCancelBeatsCompletionAndTerminalIsFinal` |
| 3 | Inbox 消费保证 | onstate-runtime `TestInboxDeduplicatesAndEffectCASIsIndependent` + fault 重复 Signal 折叠测试 |
| 4 | Effect 至多一次 | effect-dispatcher 并发 CAS 测试 |
| 5 | 事件序号单调 | event-projector 测试 + fault `TestRunResumesAfterStoreReopen`（重开后 Sequence 连续） |
| 6 | 预算闸门 | onstate-runtime 预算耗尽测试 + fault `TestSelfContinuationStopsAtStepBudget` |
| 7 | 绑定版本固定 | fault `TestPinnedBindingsDoNotFollowNewHandlerVersions` + definition `TestPublishedVersionIsImmutable` |
| 8 | 不落明文 Secret | handler-registry `TestJWKSAndTokensCarryNoSecretMaterial` |
| 9 | 两相派发 | dispatcher claim-before-execute 测试 + runtime `TestPureStateCannotCreateEffectIntent` |
| 10 | 终态拒新 Signal | fault 测试（终态后 `PutSignal` → `ErrTerminal`） |
| 11 | Ledger CAS 独立 | dispatcher 并发测试 + runtime `TestInboxDeduplicatesAndEffectCASIsIndependent` |
| 12 | 多键原子迁移 | runtime `TestRunnableTransitionWritesContinuationInSameCommit` |
| 13 | RUNNABLE 必有 Signal | reconciler `RepairRunnable` 测试 + runtime 同事写 entered 测试 |

## 工作分解

- [x] P0 仓库与契约基线
  - [x] 修正 Go multi-module workspace，统一 `core` module 边界
  - [x] 建立领域类型、QName、Effect/Signal/Outcome 与 Port 契约
  - [x] 建立全仓测试与二进制构建脚本（`scripts/test.ps1`、`scripts/build.ps1`）
- [x] P1 定义与控制面
  - [x] Harness Definition IR、校验、不可变版本仓库
  - [x] Execution Profile 快照与绑定约束
  - [x] Handler Registry：注册、能力解析、心跳、EdDSA 短期 JWT/JWKS
  - [x] Tool Registry：显式 Manifest、Revision 固定、禁用
- [x] P2 权威状态与迁移内核
  - [x] SQLite WAL 状态存储、多键事务、Run CAS 与 fencing token
  - [x] Inbox 去重/优先级/领取，RUNNABLE 同事务写 `entered`
  - [x] Step、EventIndex、Outbox、Invocation、Timer、Effect 独立记录（工具调用以 Effect Ledger 记录，Phase 1 不单独建 Invocation 表）
  - [x] 通用 `onState` 内核与 Transition Rule 推导
- [x] P3 两相派发与恢复
  - [x] Effect `PENDING → DISPATCHED → COMMITTED` 独立 ledgerVersion CAS
  - [x] 非幂等 Effect 的 recover/manual 路径
  - [x] 持久 Timer 使用 `stateEnterCounter` 判定
  - [x] Outbox Relay 与 Reconciler（含 `FireDueTimers` / `RepairRunnable` / `ReawakenStalled` 三路兜底）
- [x] P4 Handler 与适配器
  - [x] LLM Handler 与单模型 Provider Adapter（pure 内联）
  - [x] Tool Handler、单协议 Tool Executor（非 pure 两相）
  - [x] 本地 Object Store 与 ArtifactRef
  - [x] 内嵌 ChatEventQueue / StateEventQueue
- [x] P5 API、SSE 与 standalone
  - [x] Chat/Message/Run/Cancel Command API
  - [x] Run/Event/Step/Effect/Inbox Query API
  - [x] SSE cursor 断线续传与 eventId 去重语义（HMAC 签名 cursor）
  - [x] standalone 组合根与可执行配置
- [x] P6 E2E 与故障注入验收
  - [x] 正常 Agent Loop E2E
  - [x] 重复/乱序消息与 CAS 冲突
  - [x] Worker 中断、Outbox 提示丢失与恢复（租约过期 + `ReawakenStalled` 重唤醒）
  - [x] 回调丢失、Timer/完成竞争（双向竞争各一例）
  - [x] 取消/成功竞争与终态不可逆
  - [x] 自续跑步骤预算与 runDeadline 收敛

## 架构红线（每次实现评审必查）

- Run CAS 与 Effect Ledger CAS 独立，且分别是唯一裁决点。
- 只有 `sideEffect: pure` 可以内联；其余 Effect 先落账再派发。
- `RUNNABLE` 必须在同一事务写入 `entered` Inbox Signal。
- 队列只是提示，Inbox 才是事实来源。
- Timer 有效性比较 `stateEnterCounter`，不比较 `stateVersion`。
- JWT 只允许 EdDSA/ES256；拒绝 HMAC 与 `none`。
- Snapshot 不嵌入 Invocation/Timer/Effect 待处理列表。
- Handler 不得设置 `lifecycleStatus` 或 `terminalReason`。

## 执行记录

- 2026-09-27：完成现状盘点；仓库仅有未接入 workspace 的 QName 初稿，其余目录为空。
- 2026-09-27：确认交付要求为 E2E 完成、生成二进制并运行自动化测试。
- 2026-09-27：完成 T0–T24 全部实现（core 契约、6 个 adapter、13 个 component、standalone 组合根），E2E happy-path 走通。
- 2026-09-27：补齐 §11.1 故障矩阵两处缺口——tool-handler 处理 `harness/timer.fired` 回调超时收敛；Reconciler 新增 `ReawakenStalled` 为「租约失效但 Inbox 有未消费 Signal」的 Run 补发提示。
- 2026-09-27：完成 T25/T26——`apps/standalone-app/fault_test.go` 7 个故障注入场景全绿；补齐不变量 #7（绑定版本固定）与 #8（无明文 Secret）契约测试，13 条不变量全部有测试断言。完成定义 1–5 达成，仅剩 README 示例（条件 6）。
- 2026-09-27：README 补 standalone 构建/启动/API 示例并更新仓库结构——**完成定义 6 条全部达成，Phase 1 收官**。
