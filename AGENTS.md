# AGENTS.md

> 本文件为 Codex CLI 及其他遵循 AGENTS.md 约定的 AI 编码工具提供仓库引导。
> 完整架构见 `docs/arch.md` v1.0;Claude Code 实例请阅读 `CLAUDE.md`。
> 本文件与之同步;任何冲突以 `docs/arch.md` 为准。

## 仓库当前状态

**架构已定稿,代码尚未生成。** 根目录无 `go.mod` / `package.json` / `Makefile`,仅有 `README.md` 与 `docs/arch.md` v1.0。所有架构结论以 `docs/arch.md` 为单一事实源;架构变更**先改文档再改代码**。

Phase 1 = 最小可恢复闭环(Chat/Run/Harness Definition IR/`onState` 内核/Handler Registry+JWT/双队列/LLM+Tool Handler/Effect Dispatcher/Outbox/SSE/对象产物/取消与基础重试/端到端故障注入),范围见 `docs/arch.md` §16。

## 必须遵守的硬性约束(从 arch.md 抽取)

违反任一条均会破坏系统正确性,且不易从代码表面看出:

- **CAS 是唯一裁决点**:Run 状态以 `stateVersion` 为条件,Effect 派发以 `effectId + ledgerVersion` 为条件,两种 CAS 相互独立(不变量 #1, #4, #11)
- **改变世界必须两相派发**:意图先随事务落账为 `EffectLedger(PENDING)`,提交成功后才调 Provider;**仅** `sideEffect: pure` 可内联直调;`idempotent` 同样必须两相(不变量 #9 + §5.3.2)
- **内部推进不脱管**:`RUNNABLE` 状态必须同事务写入 `entered` Signal 到 Inbox,**不允许绕过 Inbox 自唤醒**(不变量 #13 + §5.3 自续跑)
- **Inbox 是权威来源**:StateEventQueue 只是提示,可丢;Reconciler 按 Inbox 兜底(§5.3.1)
- **定时器判据用 `stateEnterCounter` 而非 `stateVersion`**——同状态内迁移也会推进 `stateVersion`,会误杀有效 Timer(§8.7)
- **JWT 必须非对称**:仅 EdDSA/ES256,拒 `none` 与 HMAC;JWKS 离线验签(§7.2 + §12)
- **Run Snapshot 不内嵌待处理列表**:Invocation / Timer / Effect 各自成键,Snapshot 只保留计数与最老水位(§6.1)
- **Handler 不得设置 `lifecycleStatus` / `terminalReason`**:内核由 `StateOutcome` + Transition Rule 派生(§5.2)

## 代码组织(Phase 1 编码期)

实现语言为 Go,单仓多 module(`docs/arch.md` §15):

- `core/` — 零基础设施依赖(不 import 任何 adapter / 云 SDK / Web 框架 / DB 客户端)
- `components/<name>/` — 每能力组件一个 module;只依赖 `core` 与更底层组件
- `adapters/<name>/` — 基础设施扩展(`kv-sqlite` / `kv-foundationdb` / `ceq-kafka` / `objectstore-s3` 等);实现 `core/ports`
- `apps/standalone-app/` 与 `apps/cloudnative-app/` — 官方组合根范本;**仅**这两处允许 import adapter
- `deploy/` — 第三层部署拓扑

依赖单向、不使用 `.so` plugin、跨 module 依赖固定到已发布版本——详见 §15。

## 章节定位(按需查阅 arch.md)

- 执行模型与内核契约:§5
- 数据模型与一致性边界:§6 + §8
- 可插拔端口与适配器:§7
- 系统不变量:§11.2
- 风险与故障注入清单:§17
- 决策摘要与术语表:§18 / §19

## 变更约定

- 中文是仓库母语;commit message 沿用中文 + 简要英文术语(`docs:` / `fix:` / `feat:` / `refactor:` 前缀)
- 单分支 `main`;本仓库直接推送不强制 PR

## 编码风格硬性约束

任何后续编码工作(无论由 Claude Code、Codex 或其他 AI 工具执行)必须同时遵守以下三条,缺一不可:

1. **中文注释**:Go 源码中的注释、测试失败信息、错误文案一律中文。架构文档以中文为母语,代码注释保持同一语种,便于跨工具评审。
2. **随时提交 git**:每完成一个有意义单元(一个 module 跑通、一个组件的核心契约落地、一个不变量测试通过)就提交一次;不要等到会话结束前一次性提交,避免历史被淹没。`commit message` 沿用 `docs:` / `feat:` / `fix:` / `refactor:` / `test:` 前缀。
3. **CleanCode(Robert C. Martin)**:
   - 函数短小、单一职责(理想 ≤ 20 行;不超过一屏)
   - 命名揭示意图(无缩写、无 magic number、无误导性命名)
   - 注释解释"为什么"而非"是什么";删除冗余注释
   - DRY:不重复相同逻辑;提取共性到私有函数
   - 错误处理显式:返回 error 而非 panic(除 MustParse 等契约性构造器)
   - 边界条件优先:nil / 零值 / 空集合 / 并发入口都要在测试覆盖内
   - 类/模块高内聚低耦合:每个 package 一个清晰职责
   - 测试先行或紧跟实现,覆盖正常路径与失败路径