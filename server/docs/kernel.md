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
  其它 Harness。
- **具体业务工作流** 是导演：决定任务目标、步骤和验收方式，由 Agent、Skill 或更上层的
  业务编排提供，不进入 agentd 内核。
- **Sandbox Engine** 提供隔离的文件、进程和资源生命周期。Hostel 是首个实现，也可以接入
  其它本地或远端引擎。
- **Agent Ledger** 记录不可变的执行事实和副作用边界。它帮助恢复、审计和追踪，但不替
  agentd 做调度决策。

agentd 只依赖这些组件的能力契约，不依赖其内部对象或进程模型。

## 核心对象

- **Agent**：可复用、可版本化的 Harness 配置。
- **Environment**：Sandbox 和运行环境需求，不等于一台正在运行的沙箱。
- **Session**：用户看到的长期 Agent 身份。Session 可以跨进程、跨 Worker 和跨多次执行存在。
- **Run**：Session 的一次实际执行。Run 可以中断、迁移或恢复，但不能与某个进程绑定。
- **Event**：用户和 Session 之间的持久化输入输出，也是唤醒 Session 的依据。

Session 是产品身份，Run 是执行身份，Harness runtime 和 Sandbox instance 都只是可释放的
计算资源。

## 冻结与恢复

```text
Event durable
    │
    ▼
  wake ──► acquire ownership ──► restore ──► run
                                            │
                 waiting / idle / interrupted
                                            │
                                            ▼
                    checkpoint ──► release resources
```

当 Agent 询问用户而用户没有及时回复时，Session 应进入等待态：

1. 先持久化已完成的 Agent 上下文、控制位置和副作用边界；
2. 停止当前 Harness runtime，释放执行所有权；
3. 让 Sandbox Engine 按能力保留、休眠、快照或释放沙箱；
4. 用户回复先作为 Event 持久化，再唤醒 Session；
5. 任意健康的 agentd 实例取得所有权，恢复 Harness 和 Sandbox 后继续执行。

空闲和等待中的 Session 不应长期占用 Agent goroutine 或 Worker。沙箱能释放到什么程度由
Sandbox Engine 的能力决定，但不能改变 Session 身份和恢复语义。

## 状态与账本原则

为避免把“能看到发生了什么”和“能从哪里安全继续”混为一谈，至少区分：

- **Session / Run 控制状态**：当前状态、固定版本、待处理 Event、恢复位置、资源绑定和
  执行所有权，供 agentd 调度与恢复。
- **Harness 状态**：消息、上下文和 Harness 原生恢复材料，供对应 Harness adapter 重建
  runtime。
- **Ledger 事实**：Run、Step、模型与工具 Attempt、恢复动作和外部副作用回执，供审计并
  支撑安全判断。

具体存储可以复用同一个 Event Store，但三类语义不能互相替代。Trace 和日志属于观测证据，
不能作为恢复的权威状态；外部副作用的最终真相仍来自外部系统的权威回执或对账结果。

## 不变量

1. 用户输入未持久化，不确认接收，也不开始执行。
2. Session 身份不依赖某个 agentd 进程、Harness runtime 或 Sandbox instance。
3. 恢复前必须固定 Agent、Harness 和执行定义的版本，不能用新代码静默解释旧状态。
4. 工具副作用先记录稳定身份再派发；结果不明确时先对账，不能自动重放。
5. 等待态必须先形成可恢复边界，再释放所有权和资源。
6. 多实例接管时，旧执行者不能继续提交状态或重复产生副作用。

## 演进方式

第一版先验证单进程链路：Claude 兼容 API、AgentGo、Hostel 和 Agent Ledger 能共同完成一次
可持久化的 Session。后续围绕三个边界逐步演进，而不扩大内核：

- Sandbox Engine 的能力、生命周期和远端协议；
- Session、Checkpoint、Harness 状态与 Ledger 的持久化分工；
- Harness adapter 契约以及 AgentGo 之外的实现；
- 多副本所需的 durable wake、lease 和 fencing。

