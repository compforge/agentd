# Managed Agent Server

## 理念与概念

agentd 解决的问题是：让一个 Agent Session 比承载它的 Web Server、Agent Loop 和沙箱
进程活得更久。运行时进程可以被替换，但已接收的输入、AgentGo 上下文和已发生的
外部调用不能随之丢失。

对外模型直接采用 Claude Managed Agents 的四个核心概念：

- **Agent** 是可复用、可版本化的模型、系统提示和工具配置。
- **Environment** 是沙箱配置，不是正在运行的沙箱。
- **Session** 是一个 Agent 在一个 Environment 中处理长期任务的稳定身份。
- **Event** 是应用与 Session 之间唯一的持久化通信方式。

`Run` 是 agentd 内部的一次语义执行，用于调度和 Ledger 因果关系，不增加为对外
API 资源。一个 Session 可以经历多次 Run。

## 边界

```text
Claude-compatible API
          │
          ▼
 Session Controller ──── Agent Ledger
          │              事实 + AgentGo native state
          ▼
    Harness Adapter
       AgentGo
          │ tool calls
          ▼
    Sandbox Engine
       Hostel adapter ──── local or remote Hostel
                              Bed / Executor / Execution
```

Session Controller 拥有期望状态和执行时机。Harness Adapter 负责模型循环和原生
会话语义，AgentGo 是首个实现；Sandbox Engine 负责隔离文件、命令和资源生命周期，Hostel
是首个实现。两类组件都可以替换，Harness 不运行在 Sandbox 内，因此恢复 Agent Loop
无需绑定某个沙箱进程。

Agent Ledger 的 normalized stream 记录 Run、Step、Model Attempt 和 Tool Attempt；
`framework/agentgo/<session_id>` stream 保存 AgentGo 的无损 Message 序列。Ledger 不判断
一个未完成 Tool Attempt 是否应重试，该决策由 Session Controller 依据工具副作用做出。

## 主流程

### 创建与执行

1. 应用创建 Agent 和 Environment，再创建引用两者的 Session。Session 锁定 Agent
   的具体版本。
2. `user.message` 先写入 Agent Ledger，再返回已接收。Controller 将 Session 置为
   `running`，同一 Session 的输入串行处理。
3. Controller 通过 Harness Adapter 创建短命 runtime，从 native stream 恢复会话，然后
   处理当前输入。
4. 首个沙箱工具调用通过 Sandbox Engine 确保 `sandbox_id = session_id`。Hostel adapter
   将该身份映射为 Bed，并把文件和命令操作转换为 Hostel API。
5. AgentGo 产生的完整消息投影为 `agent.message`；完成或等待新输入时，Session
   回到 `idle`。客户端可随时通过 SSE 消费持久化 Event。

### 恢复

agentd 启动时扫描非终态 Session，将中断的 `running` 标记为 `rescheduling`，再根据
Ledger 事实分类处理：

- 已持久化但尚未处理的用户 Event，从 AgentGo native stream 恢复后继续处理。
- 没有未决 Attempt 的中断 Run，从最后一条完整 Message 继续。
- 未决 Model Attempt 可以作为新 Attempt 重试，但保留原 Attempt 的中断事实。
- 未决且可能有副作用的 Tool Attempt 进入需要决策的 `idle`，不猜测也不自动重放。

Session 空闲或等待用户时不保留 Harness runtime。Sandbox 可以保留、休眠、驱逐或从快照
恢复，这不改变 Session 身份。

## Claude API 兼容边界

agentd 保持 Claude Managed Agents 的路径、核心 JSON 形状、错误 envelope、Session 状态和
`{domain}.{action}` Event 命名。兼容性由官方 Go SDK 对 agentd 的契约测试验证。

核心面包含 Agent、Environment、Session 的 create/get/list，以及 Session Event 的 send/list/SSE
stream。Agent 支持 model、system prompt 和 `agent_toolset_20260401`；Environment 的 `cloud`
配置由当前选定的 Sandbox Engine 实现。

MCP、Skills、Vault、Memory Store、Resource mount、Outcome、Multi-agent、Deployment 和 Webhook 不在
当前服务边界内。对这些字段或 Event，API 返回明确的 `unsupported_feature`，不接收后
静默忽略。

## 关键取舍

### 兼容核心子集，不复制全部产品面

直接复用资源和 Event 协议能让客户端、SDK 与工具链复用；对不具备语义保证的能力拒绝
接收，比只复制字段却运行出不同行为更可预测。

### Session 与计算资源分离

Session 的事实和上下文持久化在进程外；Harness runtime 和 Sandbox Executor 都可替换。这使
长时间等待用户不占用 Agent Loop goroutine，也避免沙箱崩溃时丢失会话历史。

### Ledger 保存事实，Controller 拥有可靠性策略

Agent Ledger 的 OCC、幂等追加和 write-before-execute 能证明崩溃位置，但不能让外部
副作用自动变成 exactly-once。Controller 显式区分可重试模型调用和需要协调的工具调用，
不将无法保证的行为包装成“自动恢复”。
