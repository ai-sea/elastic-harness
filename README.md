# ElasticHarness

> 以**持久化状态机**为核心的 Serverless Agent 平台:每次唤醒只推进一个有界状态迁移,空闲不占资源,进程崩溃后可恢复。

---

## 它是什么

**ElasticHarness** 是一个用于承载**长时间运行、可暂停、可恢复、可审计**的 Agent / Harness Chat 的平台。

它把传统「进程内 Agent Loop」改造为「持久化状态机 + 事件驱动」:

- 每次计算只推进有限个状态迁移,随后释放算力;
- 下一步由事件、工具回调、定时器或用户输入再次唤醒任意 Worker;
- 计算层无状态,权威状态全部外置到 KV,事件流通过 Transactional Outbox 发布。

**首要目标**:按需计算、可恢复执行、基础设施可替换、全链路可审计、多租户隔离。

## 当前状态

| | |
|---|---|
| 版本 | 0.1.0（Phase 1 已落地） |
| 代码 | Go multi-module workspace：`core` + 13 个 component + 6 个 adapter + standalone 组合根 |
| 文档 | `docs/arch.md` v1.0（Phase 1 基线） |
| 当前阶段 | **Phase 1 完成** —— 最小可恢复闭环（E2E + 故障注入全绿，见 `docs/phase1-tasks.md`） |

## 快速开始（standalone）

要求：Go 1.24+（go.work 多 module 联调，无需安装额外依赖）。

```powershell
# 构建单一二进制到 bin/elastic-harness.exe
.\scripts\build.ps1

# 运行全部 module 测试
.\scripts\test.ps1

# 启动（默认监听 127.0.0.1:8080，数据落在 .\local-data）
.\bin\elastic-harness.exe
# 自定义：.\bin\elastic-harness.exe -listen 127.0.0.1:9090 -data D:\data\harness
```

standalone 内置演示闭环：本地 SQLite 权威状态 + 内嵌双队列 + 本地对象存储 + 演示 Provider（首轮发起 `echo` 工具调用，次轮收敛最终回复）。

### API 调用示例

```bash
# 1. 创建 Chat
curl -s -X POST http://127.0.0.1:8080/v1/chats \
  -H 'Content-Type: application/json' -d '{"tenantId":"demo"}'
# → {"chatId":"chat_..."}

# 2. 写入用户消息
curl -s -X POST http://127.0.0.1:8080/v1/chats/<chatId>/messages \
  -H 'Content-Type: application/json' \
  -d '{"tenantId":"demo","role":"user","content":"请调用 echo 工具"}'

# 3. 创建 Run（自动按内嵌 Harness Definition 执行：LLM → Tool → LLM）
curl -s -X POST http://127.0.0.1:8080/v1/chats/<chatId>/runs \
  -H 'Content-Type: application/json' \
  -d '{"tenantId":"demo","prompt":"请调用 echo 工具"}'
# → {"runId":"run_...", "lifecycleStatus":"runnable", ...}

# 4. 查询 Run 状态 / 步骤 / Effect Ledger / Inbox
curl -s http://127.0.0.1:8080/v1/runs/<runId>        -H 'X-Tenant-ID: demo'
curl -s http://127.0.0.1:8080/v1/runs/<runId>/steps  -H 'X-Tenant-ID: demo'
curl -s http://127.0.0.1:8080/v1/runs/<runId>/effects -H 'X-Tenant-ID: demo'
curl -s http://127.0.0.1:8080/v1/runs/<runId>/inbox  -H 'X-Tenant-ID: demo'

# 5. SSE 实时事件流（断线后用上次收到的 id: 值续传）
curl -N http://127.0.0.1:8080/v1/runs/<runId>/stream -H 'X-Tenant-ID: demo'
curl -N "http://127.0.0.1:8080/v1/runs/<runId>/stream?after=<cursor>" -H 'X-Tenant-ID: demo'

# 6. 取消进行中的 Run
curl -s -X POST http://127.0.0.1:8080/v1/runs/<runId>/cancel \
  -H 'Content-Type: application/json' -d '{"tenantId":"demo"}'
```

Query/SSE 接口用 `X-Tenant-ID` 头做租户归属校验；SSE 事件 `id` 为 HMAC 签名 cursor（15 分钟有效），重复事件由客户端按 eventId 去重。

## 架构核心

```text
Client / SDK / UI
   ↓
API & Realtime Gateway
   ↓
Command Service ─→ KV (权威状态) ─→ StateEventQueue (唤醒) ─→ Worker
                    │                 ChatEventQueue (事件, 内嵌 KV claim-check)
                    └─ Object Store   ├─→ Scheduler ─→ Stateless Worker
                                      │                  ↓
                                      │              onState Runtime
                                      └─────────────── Versioned Harness Definition
                                                         ↓
                                            Handler Registry (注册/心跳/JWT)
                                            (LLM / Tool / Memory / Approval)
```

详细架构、ADR、数据模型、协议、可靠性设计、三层装配与分阶段落地路线见 [`docs/arch.md`](docs/arch.md)。

## 关键设计决策(摘要)

| 决策 | 选择 |
|---|---|
| 执行模型 | 持久化状态机(单步执行) |
| 执行定义 | Harness Definition：版本化 IR + 逻辑 capability 需求(不绑定具体 Handler) |
| 统一运行入口 | `StateHandler.onState(context, signal) → StateOutcome` |
| Handler 绑定 | 中心注册 + 调度式解析 + Run 时固定(K8s 风格) |
| Handler 凭证 | 短期 JWT(1h)+ 心跳滚动刷新;MQ 写入强制携带(JWT 仅用于写入鉴权,不进入消息体) |
| 一致性边界 | Run 聚合 + CAS(`stateVersion` + fencing token) |
| 消息语义 | 至少一次投递 + 幂等效果 |
| Effect 派发 | 两相派发:意图与 Snapshot/Inbox/Outbox 同事务提交,提交后才调用 Provider;仅 `pure\|idempotent` 允许内联快路径 |
| 事件通道 | 双队列：ChatEventQueue(内嵌 KV claim-check,承载领域事件) + StateEventQueue(仅唤醒提示,可丢) |
| 会话上下文 | ChatContextTree：Git 式不可变单父树 + 分支 |
| 事件发布 | Transactional Outbox |
| 大对象 | 逻辑 Artifact + 不可变引用,物理存储随 Assembly 替换 |
| 工具模型 | 统一异步 Invocation(MCP/A2A/RAG/人工审批) |
| 插件分层 | Handler Plugin + Provider Adapter |
| 组件编排 | 单一组件模型 + Assembly Binding;单机/分布式同构 |
| Registry 可用性 | 无状态多副本 + 共享 KV + JWKS 离线验签(故障不阻断在途 Run) |
| 流推送 | 无状态 Stream Gateway + EventIndex 断线补齐(任意水平扩展) |
| 部署 | 所有计算服务无状态;Serverless Function / Container / K8s 任选 |

完整 ADR 见 `docs/arch.md` §18。

## 仓库结构

```text
.
├── core/                 # 零基础设施依赖：domain / effects / ports / qname
├── components/           # 每能力组件一个 module（只依赖 core 与更底层组件）
│   ├── api/                  # Command/Query/SSE HTTP 服务
│   ├── effect-dispatcher/    # 两相派发（Ledger CAS → Provider → 回执）
│   ├── event-projector/      # Outbox Relay + 事件投影
│   ├── execution-profile/    # 执行预算/命名空间约束
│   ├── handler-registry/     # 注册/能力解析/心跳 + EdDSA JWT/JWKS
│   ├── harness-definition/   # Definition IR、校验、不可变版本仓库
│   ├── llm-handler/          # LLM 状态 Handler（pure 内联）
│   ├── memory-handler/       # Memory Handler（Phase 1 stub）
│   ├── onstate-runtime/      # 通用 onState 内核（CAS + Inbox + 自续跑）
│   ├── timer-reconciler/     # 持久 Timer + 停滞 Run 兜底重唤醒
│   ├── tool-executor/        # 工具执行编排
│   ├── tool-handler/         # 工具状态 Handler（两相派发 + 回调 Timer）
│   └── tool-registry/        # 显式 Manifest + Revision 固定
├── adapters/             # 基础设施扩展（实现 core/ports）
│   ├── kv-sqlite/            # SQLite WAL 权威状态（CAS + fencing token）
│   ├── ceq-embedded/         # 内嵌 ChatEventQueue
│   ├── seq-embedded/         # 内嵌 StateEventQueue
│   ├── objectstore-local/    # 本地文件对象存储
│   ├── modelprovider-openai/ # OpenAI 兼容 Provider
│   └── toolexecutor-http/    # 单协议 HTTP 工具执行器
├── apps/standalone-app/  # 官方组合根范本（唯一允许 import adapter 的应用）
├── deploy/               # 部署拓扑
├── schemas/              # 消息 schema
├── scripts/              # build.ps1 / test.ps1
└── docs/                 # arch.md（单一事实源）+ phase1-tasks.md（实施账本）
```

> 依赖单向：`core` 不 import 任何 adapter；只有 `apps/*` 组合根允许 import adapter（arch.md §15）。

## 路线图

- **Phase 1 — 最小可恢复闭环**（✅ 已完成，验收见 `docs/phase1-tasks.md`）：Chat/Run/Harness Definition IR/`onState` 内核/Handler Registry(注册/心跳/JWT)/LLM+Tool Handler/Outbox/SSE/对象产物/取消与基础重试/端到端故障注入。
- **Phase 2 — 生产可用**:多租户 IAM、MCP/A2A/RAG、远程 Handler、长期 Memory、可重试失败与 `BLOCKED` Run 的运维闭环、限流熔断降级。
- **Phase 3 — 生态规模化**:插件 SDK、可视化编排、Run fork/replay、语义缓存、跨区域主动-主动。

## 贡献

Phase 1 代码已落地。架构/语义/不变量变更遵循「先改 `docs/arch.md` 再改代码」；实施进度与验收状态见 `docs/phase1-tasks.md`。欢迎围绕 `docs/arch.md` 提出 issue 与评审意见。

## 许可证与专利说明

本仓库基于 [**MIT 许可证**](./LICENSE) 开源。你可以自由使用、修改、再分发（包括商业使用），**前提是保留版权与许可声明**。完整决策依据见 `docs/arch.md` 中 ADR-LIC。

MIT 许可证**不包含专利授权条款**。本仓库的贡献者与项目所有者**不承诺本仓库代码不侵犯任何第三方的专利权**。使用者应自行评估并承担与专利相关的全部风险。

如需获得明确的专利授权或侵权赔偿承诺，请通过 issue 或邮件与维护者联系洽谈单独的书面协议。

---

<sub>本 README 与 `docs/arch.md` 配套阅读;架构变更请同步更新 arch.md,以保持单一事实源。</sub>
