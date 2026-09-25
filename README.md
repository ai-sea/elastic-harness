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
| 版本 | 0.0.0(架构设计阶段) |
| 代码 | 尚未生成 |
| 文档 | `docs/arch.md` v0.11（评审稿） |
| 当前阶段 | 待进入 **Phase 1** —— 最小可恢复闭环(MVP) |

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
├── README.md           # 本文件
└── docs/
    └── arch.md         # 总体架构文档(单一事实源,待按主题拆分)
```

> 说明:Phase 1 启动后,代码骨架采用 Go multi-module monorepo——`core`(零基础设施依赖)+ `components/`(每组件一个 module)+ `adapters/`(基础设施扩展,按需引入)+ `assemblies/`(组合根),参见 arch.md §15。

## 路线图

- **Phase 1 — 最小可恢复闭环**:Chat/Run/Harness Definition IR/`onState` 内核/Handler Registry(注册/心跳/JWT)/LLM+Tool Handler/Outbox/SSE/对象产物/取消与基础重试。
- **Phase 2 — 生产可用**:多租户 IAM、MCP/A2A/RAG、远程 Handler、长期 Memory、可重试失败与 `BLOCKED` Run 的运维闭环、限流熔断降级。
- **Phase 3 — 生态规模化**:插件 SDK、可视化编排、Run fork/replay、语义缓存、跨区域主动-主动。

## 贡献

目前项目处于架构设计阶段,欢迎围绕 `docs/arch.md` 提出 issue 与评审意见。代码层尚未开放贡献。

---

<sub>本 README 与 `docs/arch.md` 配套阅读;架构变更请同步更新 arch.md,以保持单一事实源。</sub>
