# agentd Kernel

## 定位

agentd 是 Managed Agent Server。借用电影生产的比喻，它不是演员，也不是导演，而是
**制片人**：不负责提供 Agent 智能，不决定具体任务如何规划，也不实现沙箱技术；它把
Agent Harness、Sandbox Engine、持久化和 API 组织成一个可以长期运行的服务。

agentd 的稳定职责只有三项：

1. **服务化**：通过 Claude Managed Agents 兼容 API 管理 Agent、Environment、Session
   和 Event。未来可由多个 agentd 实例共同承载 Session。
2. **冻结与恢复**：Agent 等待用户、外部事件或调度时，可以冻结耐久状态并尽可能释放
   Worker、Harness runtime 和沙箱资源；条件满足后，由任意可用实例恢复执行。
3. **可审计**：输入、执行、模型与工具调用、恢复决策和外部副作用都有持久化事实，能够
   回答 Agent 做了什么、为什么停下以及是否可以安全继续。

## 边界

```text
Claude-compatible API
          │
          ▼
       agentd
  lifecycle / ownership / recovery / audit
     │              │              │
     ▼              ▼              ▼
 Harness Adapter  Sandbox Engine  Agent Ledger
   AgentGo ...     Hostel ...     durable facts
```

- **Harness** 是演员：执行模型循环、工具调用和上下文处理。AgentGo 是首个实现，后续可接入
  其它 Harness；执行边界与适配契约见 `harness.md`。
- **具体业务工作流** 是导演：决定任务目标、步骤和验收方式，由 Agent、Skill 或更上层的
  业务编排提供，不进入 agentd 内核。
- **Sandbox Engine** 提供隔离的文件、进程和资源生命周期。Hostel 是首个实现，也可以接入
  其它本地或远端引擎；能力和资源边界见 `sandbox-engine.md`。
- **Agent Ledger** 记录不可变的执行事实和副作用边界。它帮助恢复判断、审计和追踪，但不保存
  Harness State，也不替 agentd 做调度决策。

agentd 只依赖这些组件的能力契约，不依赖其内部对象或进程模型。

## 核心对象

- **Agent**：可复用、可版本化的 Harness 配置。
- **Environment**：Sandbox 和运行环境需求，不等于一台正在运行的沙箱。
- **Session**：用户看到的长期 Agent 身份。Session 可以跨进程、跨 Worker 和跨多次执行存在。
- **Run**：Session 的一次实际执行。Run 可以中断、迁移或恢复，但不能与某个进程绑定。
- **Event**：用户和 Session 之间的持久化输入输出，也是唤醒 Session 的依据。

Session 是产品身份，Run 是执行身份，Harness runtime 和 Sandbox instance 都只是可释放的
计算资源。

## 服务主流程

1. 应用创建可版本化的 Agent 和 Environment，再创建锁定两者具体版本的 Session；
2. 用户 Event 持久化后才确认接收，Session Controller 决定是否唤醒对应 Session；
3. Controller 取得执行所有权，通过 Harness Adapter 创建或恢复短命 runtime；
4. Harness 执行模型循环，工具调用通过 Sandbox Engine 进入对应 Session 的隔离环境；
5. Harness 输出投影为持久化 Event，Session 完成当前输入后等待下一次唤醒；
6. 空闲或等待中的 Session 释放 Harness runtime，Sandbox 按引擎能力保留、休眠或回收。

Session Controller 拥有期望状态和执行时机，Harness Adapter 拥有模型循环和原生会话语义，
Sandbox Engine 拥有隔离资源生命周期。三类组件都可以替换，Session 身份不绑定任何一个运行中实例。

## Claude API 兼容边界

agentd 保持 Claude Managed Agents 的 Agent、Environment、Session 和 Event 核心资源，以及路径、
主要 JSON 形状、错误 envelope、Session 状态和 `{domain}.{action}` Event 命名。兼容性由官方 SDK
针对 agentd 的契约测试验证。

Agent 支持 model、system prompt 和 toolset；Environment 的 cloud 配置由当前 Sandbox Engine
实现。MCP、Skills、Vault、Memory Store、Resource mount、Outcome、Multi-agent、Deployment 和
Webhook 不属于当前服务边界。无法提供语义保证的能力返回明确的 `unsupported_feature`，不接收后
静默降级。

兼容核心资源和行为可以复用现有客户端与 SDK；agentd 不复制尚未具备运行语义的完整产品面。

## 持久化、恢复与审计

Control State、Harness State 和 Ledger 分别拥有调度状态、Harness 原生恢复材料和规范化执行事实。
它们的权威来源、冻结恢复顺序、失败窗口、审计和轨迹边界统一定义在 `state-ledger.md`，Kernel 不重复
展开。

## 扩展边界

agentd 通过稳定能力契约接入其它 Harness 和本地或远端 Sandbox Engine。多实例运行在同一模型上增加
durable wake、lease 与 fencing，不改变 Session、Run 和 Event 的产品语义，也不把具体业务工作流
引入内核。
