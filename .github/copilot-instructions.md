# GitHub Copilot Instructions — ElasticHarness

> 完整架构见 `docs/arch.md` v1.0;本文件与之同步;冲突以 `docs/arch.md` 为准。

## 仓库当前状态

`docs/arch.md` v1.0 是 Phase 1 基线;代码尚未生成。所有架构结论以该文档为单一事实源;架构/语义/不变量变更**先改文档再改代码**。

## 实现期硬性约束(代码生成必须遵守)

1. **CAS 是唯一裁决点**:Run 状态用 `stateVersion` CAS,Effect 派发用 `effectId + ledgerVersion` CAS,两者独立(`arch.md` 不变量 #1, #4, #11)
2. **改变世界必须两相派发**:`sideEffect: pure` 可内联;`idempotent` 与 `non-idempotent` 一律两相,意图先随事务落账为 `EffectLedger(PENDING)`,提交成功后才调 Provider(不变量 #9 + §5.3.2)
3. **RUNNABLE 状态必须同事务写 `entered` Signal 到 Inbox**,不允许绕过 Inbox 自唤醒(不变量 #13)
4. **Inbox 是权威来源**:StateEventQueue 只是提示,可丢;Reconciler 兜底(§5.3.1)
5. **定时器判据用 `stateEnterCounter`**,不用 `stateVersion`(§8.7)
6. **JWT 用 EdDSA/ES256**,拒 `none` 与 HMAC;JWKS 离线验签(§7.2 + §12)
7. **Run Snapshot 不内嵌待处理列表**:Invocation / Timer / Effect 各自成键,Snapshot 只保留计数与水位(§6.1)
8. **Handler 不得设置 `lifecycleStatus` / `terminalReason`**;由 `StateOutcome` + Transition Rule 派生(§5.2)

## 代码组织

- 实现语言:Go;单仓多 module(每个 `components/<name>/` 一个 module)
- `core/` 不 import 任何 adapter / 云 SDK / Web 框架 / DB 客户端
- adapter 仅由 `apps/standalone-app/` 或 `apps/cloudnative-app/` 等组合根 import
- 不使用 Go `.so` plugin

## 命名与风格

- 仓库母语:中文;commit message 中文 + 简要英文术语前缀(`docs:` / `fix:` / `feat:` / `refactor:`)
- 事件类型用 QName(`ns/name`),版本由 `schemaVersion` 字段表达,名称不含版本后缀
- Handler / Provider 分层:`onState` 是状态语义插件,Provider 只负责协议适配,二者不可混为一层
- Port 名 `name/version` 标识;同一组件的 `component.yaml` provides 与 Registry 中可解析的 capability 必须一致
- 单分支 `main`;直接推送不强制 PR

## arch.md 章节定位

- 执行模型:§5(内核契约)、§5.3(自续跑)、§5.3.2(Effect 两相)
- 数据模型:§6(KV/队列/对象存储)、§8(Run Snapshot/Inbox/Effect Ledger)
- 端口与适配器:§7
- 不变量:§11.2(13 条)、§11.3(错误分类)
- 模块与装配:§14(三层装配)、§15(代码仓)