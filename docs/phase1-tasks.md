# Phase 1 实施与验收账本

> 唯一需求来源：`docs/arch.md` v1.0。本文只跟踪实施，不修改架构结论。

## 完成定义

Phase 1 只有同时满足以下条件才算完成：

1. `go test` 覆盖全部 workspace module 并通过；
2. standalone 应用可生成单一二进制；
3. E2E 自动化测试走通 Chat → Message → Run → LLM → Tool → 最终消息；
4. 故障注入覆盖重复消息、Worker 中断、丢失提示/回调、完成与超时竞争、Reconciler 恢复；
5. `docs/arch.md` §11.2 的 13 条不变量均有契约测试或 E2E 断言；
6. 示例配置、启动方式与 API 调用方法已写入 README。

## 工作分解

- [ ] P0 仓库与契约基线
  - [x] 修正 Go multi-module workspace，统一 `core` module 边界
  - [x] 建立领域类型、QName、Effect/Signal/Outcome 与 Port 契约
  - [ ] 建立全仓测试与二进制构建脚本
- [ ] P1 定义与控制面
  - [x] Harness Definition IR、校验、不可变版本仓库
  - [ ] Execution Profile 快照与绑定约束
  - [x] Handler Registry：注册、能力解析、心跳、EdDSA 短期 JWT/JWKS
  - [ ] Tool Registry：显式 Manifest、Revision 固定、禁用
- [ ] P2 权威状态与迁移内核
  - [x] SQLite WAL 状态存储、多键事务、Run CAS 与 fencing token
  - [x] Inbox 去重/优先级/领取，RUNNABLE 同事务写 `entered`
  - [ ] Step、EventIndex、Outbox、Invocation、Timer、Effect 独立记录
  - [x] 通用 `onState` 内核与 Transition Rule 推导
- [ ] P3 两相派发与恢复
  - [x] Effect `PENDING → DISPATCHED → COMMITTED` 独立 ledgerVersion CAS
  - [x] 非幂等 Effect 的 recover/manual 路径
  - [x] 持久 Timer 使用 `stateEnterCounter` 判定
  - [x] Outbox Relay 与 Reconciler
- [ ] P4 Handler 与适配器
  - [ ] LLM Handler 与单模型 Provider Adapter（pure 内联）
  - [ ] Tool Handler、单协议 Tool Executor（非 pure 两相）
  - [ ] 本地 Object Store 与 ArtifactRef
  - [x] 内嵌 ChatEventQueue / StateEventQueue
- [ ] P5 API、SSE 与 standalone
  - [ ] Chat/Message/Run/Cancel Command API
  - [ ] Run/Event/Step/Effect/Inbox Query API
  - [ ] SSE cursor 断线续传与 eventId 去重语义
  - [ ] standalone 组合根与可执行配置
- [ ] P6 E2E 与故障注入验收
  - [ ] 正常 Agent Loop E2E
  - [ ] 重复/乱序消息与 CAS 冲突
  - [ ] Worker 中断、Outbox 提示丢失与恢复
  - [ ] 回调丢失、Timer/完成竞争
  - [ ] 取消/成功竞争与终态不可逆
  - [ ] 自续跑步骤预算与 runDeadline 收敛

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
