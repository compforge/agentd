# Harness

## 定位

Harness 是 Agentlet 接入 Agent Loop 的执行边界。它负责模型循环、原生上下文和工具调用语义，
但不拥有 Session 生命周期、全局调度策略或沙箱实现。

当前首个实现是 Agentlet 进程内的 AgentGo Adapter。Agentlet 通过稳定的 Harness 契约调用它，
避免让执行 API、Control Plane 和持久化层依赖 AgentGo 类型；接入其它本地或远端 Harness 时，
Session 和 Event 的产品语义保持不变。

## 职责边界

| 组件 | 拥有 | 不拥有 |
| --- | --- | --- |
| agentd Control Plane | 输入持久化、Assignment、全局运行状态与终止决策 | 模型循环、原生上下文格式 |
| Agentlet | Assignment 内的本地执行、runtime 生命周期与 observed state 上报 | 全局 placement、跨实例资源真相 |
| Harness Adapter | Harness runtime、原生状态恢复、模型与工具循环、输出投影 | 长期 Session 身份、执行所有权 |
| Sandbox Engine | 隔离环境及文件、命令等执行能力 | Tool 语义、模型循环 |
| Agent Ledger Adapter | 将原生模型和工具 hook 记录为规范化执行事实 | Harness State、恢复和重放决策 |

具体业务目标、任务拆分和验收方式由 Agent、Skill 或更上层编排提供，不属于 Harness Adapter。

## Agentlet 契约

当前 `Harness` 接口表达四组能力：

| 能力 | 语义 |
| --- | --- |
| `Name` / `Version` | 提供可固定在 Session Control State 中的 Harness 身份 |
| `PrepareSession` | 为新 Session 准备原生状态，并返回对 Agentlet 和 Control Plane 不透明的 `ResumeRef` |
| `Run` | 从 `ResumeRef` 恢复上下文，消费一个已持久化输入，执行至本轮稳定边界并投影输出 |
| `Interrupt` | 请求停止当前 Session 的活跃 runtime；它本身不代表状态已经安全持久化 |

`Run` 的输入带稳定 input ID。Adapter 返回最新 Harness State revision；输出通过 callback 投影为
Managed Event，由 Agentlet 上报并由 agentd 持久化。接口不暴露原生 message、model、tool 或远端
session 类型。

一个 Agentlet 可以支持一个或多个 Harness 实现。agentd 创建 Assignment 时固定 Harness 名称和
版本；Agentlet 必须根据已固定的身份选择实现，不能在恢复时静默切换。

## 单轮执行

```text
durable user Event + Assignment
        │
        ▼
Agentlet receives the current Assignment
        │
        ▼
Harness Adapter restores native state
        │
        ▼
model loop ── tool call ──► Sandbox Engine
     │                          │
     └──── Ledger hooks ◄───────┘
        │
        ▼
commit native state + project Managed Event
        │
        ▼
Agentlet returns the committed revision to agentd
```

Harness runtime 是本轮执行资源，不是 Session 本身。当前 AgentGo Adapter 每次 `Run` 都从持久化
消息创建新的 `agentgo.Agent`，执行结束后释放；等待下一条用户输入不需要长期占用 AgentGo runtime。

## 状态与恢复

Harness State 由对应 Adapter 解释。agentd 和 Agentlet 只传递 `ResumeRef` 和 revision，不要求不同
Harness 转换成统一消息或 checkpoint schema。当前 AgentGo Adapter 使用版本化 opaque record 追加
已提交的原生 message，并以 revision 做乐观并发检查。

Adapter 恢复同一 input 时必须保证：

1. 未提交原生 user message 时才重新注入 Prompt；
2. 已提交 input 但尚未完成时从原生上下文继续；
3. 已存在完整 assistant message 时只幂等补齐 Managed Event；
4. 无法证明结果的 Tool Attempt 不自动重放，而是返回 `ErrUnsafeRecovery`，由 Agentlet 上报并交给
   agentd 终止和对账。

Checkpoint、Ledger 的使用顺序和失败窗口见 [agentlet.md](agentlet.md)。`Interrupt` 只停止活跃执行；
能否释放 runtime 取决于 Adapter 是否已到达可恢复的持久化边界。

## Ledger 与 Sandbox 集成

Harness Adapter 是 Agent Loop 与两个稳定能力边界之间的桥梁：

- 模型和工具 hook 通过对应 Ledger Adapter 翻译为跨 Harness 的规范化事实，并遵守
  write-before-execute；Ledger 不保存原生上下文。
- Harness 的 Tool/Workspace API 只通过 `sandbox.Engine` 使用隔离环境，不直接依赖具体 Engine 类型、
  HTTP 路由或本地路径；具体契约见 [sandbox-engine.md](sandbox-engine.md)。

这两类集成属于 Adapter，不应进入 agentd Control Plane。Agentlet 只消费 Harness 的运行结果、
持久化输出和明确的恢复错误。

## AgentGo Adapter

当前 `AgentGoRunner`：

- 根据 Session 中固定的 model、system prompt 和 toolset 创建 AgentGo runtime；
- 从 Harness State 恢复 AgentGo message，并在每次 message commit 后追加新 revision；
- 使用 Agent Ledger 的 AgentGo Adapter 记录 Run、Model Attempt 和 Tool Attempt；
- 通过 Sandbox Engine 组装 AgentGo tools；
- 仅将完整 assistant message 投影为 Managed Event，不持久化超时产生的部分输出；
- 维护活跃 Session 到 AgentGo runtime 的进程内映射，用于响应 `Interrupt`。

AgentGo 是当前实现，不是 agentd 的状态模型。其它 Harness 可以使用 snapshot、会话树或远端
session ID，只要能够满足同样的运行、状态、审计和隔离语义。

## 接入其它 Harness

新增 Adapter 至少需要：

1. 实现 Harness 身份、Session 准备、单轮运行和中断契约；
2. 定义原生状态格式、codec 版本、兼容性检查和 `ResumeRef` 语义；
3. 将模型与工具 hook 适配到 Agent Ledger，并保留稳定 input、run 和 attempt 身份；
4. 将文件和命令等工具能力适配到 Sandbox Engine；
5. 通过契约测试验证正常执行、进程替换、重复输入投影、模型中断和未决 Tool Attempt。

远端 Harness 也应通过同一语义接入；其 `ResumeRef` 可以指向远端原生会话，但超时、取消、版本固定、
状态耐久性和审计边界仍需由 Adapter 明确保证。

## 不变量

1. Session 的长期身份和执行时机属于 agentd，不能绑定某个 Agentlet 或 Harness runtime。
2. Harness 原生类型和状态格式不能泄漏到公开 API、Control Plane、Ledger 协议或 Sandbox Engine。
3. Session 固定的 Harness 名称与版本必须参与恢复兼容性判断。
4. Harness 输出先成为可幂等识别的 Managed Event，再由 Agentlet 上报、agentd 对外提供。
5. 状态可恢复不等于副作用可重放；结果不明确的 Tool Attempt 必须先对账。
