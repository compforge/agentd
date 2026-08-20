# agentd Kernel

## 定位

agentd 是 Managed Agent Server。借用电影生产的比喻，它不是演员，也不是导演，而是
**制片系统**：不负责提供 Agent 智能，不决定具体任务如何规划，也不实现沙箱技术；它通过
Control Plane 和 Agentlet，把 Agent Harness、Sandbox Engine、持久化和 API 组织成一个可以
长期运行的服务。

agentd 的稳定职责只有四项：

1. **资源控制**：agentd 作为 Control Plane，通过 Claude Managed Agents 兼容 API 管理 Agent、
   Environment、Session 和 Event，并通过 Worker 与 Assignment 选择执行位置。
2. **节点执行**：每个 Agentlet 接受有效 Assignment，实现执行相关的 Managed Agents API，并在
   容量边界内管理多个 Harness runtime。
3. **冻结与恢复**：Agent 等待用户、外部事件或调度时，可以冻结耐久状态并尽可能释放
   Worker、Harness runtime 和沙箱资源；条件满足后，由任意可用实例恢复执行。
4. **可审计**：输入、执行、模型与工具调用、恢复决策和外部副作用都有持久化事实，能够
   回答 Agent 做了什么、为什么停下以及是否可以安全继续。

## 边界

```text
Claude-compatible API / Baton / scheduled trigger
                         │
                         ▼
              agentd / Control Plane
       resource / scheduling / routing / reconcile
              │ Managed API + Assignment
                         ▼
                      Agentlet
          runtime / freeze / restore
           │              │              │
           ▼              ▼              ▼
     Harness Adapter  Sandbox Engine  Agent Ledger
       AgentGo ...    local/remote    durable facts
```

- **Harness** 是演员：执行模型循环、工具调用和上下文处理。AgentGo 是首个实现，后续可接入
  其它 Harness；执行边界与适配契约见 `harness.md`。
- **具体业务工作流** 是导演：决定任务目标、步骤和验收方式，由 Agent、Skill 或更上层的
  业务编排提供，不进入 agentd 内核。
- **agentd** 是 Control Plane 的代码与服务边界，做全局资源、placement 和请求路由；详细边界见
  `agentd.md`。
- **Agentlet** 只执行分配到本实例的 Session，拥有 Harness、Checkpoint、Ledger 与 Sandbox 的执行侧
  编排；详细边界见 `agentlet.md`。
- **Sandbox Engine** 提供隔离的文件、进程和资源生命周期，可以接入本地或远端引擎；能力和资源
  边界见 `sandbox-engine.md`。
- **Agent Ledger** 记录不可变的执行事实和副作用边界。它帮助恢复判断、审计和追踪，但不保存
  Harness State，也不替 agentd 做调度决策。

agentd 只依赖这些组件的能力契约，不依赖其内部对象或进程模型。

## 核心对象

- **Agent**：可复用、可版本化的 Harness 配置。
- **Environment**：Sandbox 和运行环境需求，不等于一台正在运行的沙箱。
- **Session**：用户看到的长期 Agent 身份。Session 可以跨进程、跨 Worker 和跨多次执行存在。
- **Run**：Session 的一次实际执行。Run 可以中断、迁移或恢复，但不能与某个进程绑定。
- **Event**：用户和 Session 之间的持久化输入输出，也是唤醒 Session 的依据。
- **Worker**：一个由 Lifecycler 管理、被 Observer 观测的 Agentlet 承载单元，也是 agentd 的调度和
  容量单位。
- **Assignment**：Session 当前所在 Worker 的临时执行归属，不是产品身份。

Session 是产品身份，Run 是执行身份，Harness runtime 和 Sandbox instance 都只是可释放的
计算资源，Assignment 只是它们在某一时刻的节点归属。

## 服务主流程

1. 应用通过 agentd 创建可版本化的 Agent 和 Environment，再创建锁定两者具体版本的
   Session；
2. 用户 Event 持久化后才确认接收，Controller 生成待执行意图；
3. Scheduler 从最新 observation 表明存在、Ready 且未达到并发上限的 Worker 中选择实例；若无容量，
   Lifecycler 根据持久化需求创建 Worker，Observer 确认 Ready 后再持久化 Assignment；
4. Connector 转发原始 Managed Agents API 请求和内部 Assignment 元数据；Agentlet 接受后通过
   Harness Adapter 创建或恢复短命 runtime；
5. Harness 执行模型循环，工具调用通过 Sandbox Engine 进入对应 Session 的隔离环境；
6. Harness 输出投影为持久化 Event，并提交最新恢复点；
7. 空闲或等待中的 Session 释放 Harness runtime，Sandbox 按引擎能力保留、休眠或回收，agentd
   释放或更新 Assignment；Lifecycler 最终回收超过 idle TTL 的空 Worker。

agentd 拥有期望状态、全局归属和执行时机，Agentlet 只拥有有效 Assignment 内可丢弃的本地执行状态，
Harness Adapter 拥有模型循环和原生会话语义，Sandbox Engine 拥有隔离资源生命周期。组件都可以
替换，Session 身份不绑定任何一个运行中实例。

## Claude API 兼容边界

agentd 保持 Claude Managed Agents 的 Agent、Environment、Session 和 Event 核心资源，以及
路径、主要 JSON 形状、错误 envelope、Session 状态和 `{domain}.{action}` Event 命名。兼容性由
官方 SDK 针对 agentd 的契约测试验证。Agentlet 内部执行端复用同一套 API 语义，agentd 在转发时
附加不进入公开 schema 的 Assignment 元数据；调用方只依赖 agentd API。

Agent 支持 model、system prompt 和 toolset；Environment 的 cloud 配置由当前 Sandbox Engine
实现。MCP、Skills、Vault、Memory Store、Resource mount、Outcome、Multi-agent、Deployment 和
Webhook 不属于当前服务边界。无法提供语义保证的能力返回明确的 `unsupported_feature`，不接收后
静默降级。

兼容核心资源和行为可以复用现有客户端与 SDK；agentd 不复制尚未具备运行语义的完整产品面。

## 持久化、恢复与审计

Control State 拥有 Worker、Assignment 和 Session 当前决策，其模型与调度规则定义在 `agentd.md`。
Agentlet 对 Harness Checkpoint 和 Agent Ledger 的使用顺序定义在 `agentlet.md`；Ledger 对象模型、
协议和存储规范由 Agent Ledger 项目拥有。三者的权威来源独立，Kernel 不重复展开。

## 扩展边界

agentd 通过稳定能力契约接入其它 Harness 和本地或远端 Sandbox Engine。Control Plane 可以增加调度
策略或水平扩展，Agentlet 可以增加 Harness / Sandbox 能力；Sandbox Engine Adapter 只位于
Agentlet。这些扩展不改变 Session、Run 和 Event 的产品语义，也不把具体业务工作流引入内核。
