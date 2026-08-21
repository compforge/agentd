# agentd Kernel

## 定位

agentd 是 Managed Agent Server。借用电影生产的比喻，它不是演员，也不是导演，而是
**制片系统**：不负责提供 Agent 智能，不决定具体任务如何规划，也不实现沙箱技术；它通过
Control Plane 和 Agentlet，把 Agent Harness、Sandbox Engine、持久化和 API 组织成一个可以
长期运行的服务。

agentd 的稳定职责只有四项：

1. **资源控制**：agentd 作为 Control Plane，通过 Claude Managed Agents 兼容 API 管理 Agent、
   Environment、Session 和 Event，并通过 Worker 与 Assignment 选择执行位置。
2. **节点执行**：每个 Agentlet 通过内部执行 API 接受有效 Assignment，并在容量边界内管理多个
   Harness runtime；Claude 兼容协议止于 agentd。
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
              │ internal execution API + Assignment
              │                         ▼
              │                      Agentlet
              │             runtime / freeze / restore
              │                │              │
              │                ▼              ▼
              │          Harness Adapter  Sandbox Engine
              │            AgentGo ...    local/remote
              │
              └───────────────┬───────────────┘
                              ▼
                         Agent Ledger
                         durable facts
```

- **Harness** 是演员：执行模型循环、工具调用和上下文处理。AgentGo 是首个实现，后续可接入
  其它 Harness；执行边界与适配契约见 `harness.md`。
- **具体业务工作流** 是导演：决定任务目标、步骤和验收方式，由 Agent、Skill 或更上层的
  业务编排提供，不进入 agentd 内核。
- **agentd** 是 Control Plane 的代码与服务边界，做全局资源、placement 和请求路由；详细边界见
  `agentd.md`。
- **Agentlet** 只执行分配到本实例的 Session，拥有 Harness、Checkpoint 与 Sandbox 的执行侧
  编排，并写入它亲自产生的执行事实；详细边界见 `agentlet.md`。
- **Sandbox Engine** 提供隔离的文件、进程和资源生命周期，可以接入本地或远端引擎；能力和资源
  边界见 `sandbox-engine.md`。
- **Agent Ledger** 记录不可变的执行事实和副作用边界。它帮助恢复判断、审计和追踪，但不保存
  Harness State，也不替 agentd 做调度决策。

一个完整部署中的 agentd 与所有 Agentlet 必须连接同一个逻辑数据库，并通过同一个 Managed Event
Ledger Adapter 访问 Event 投影；共享存储不等于共享写入责任。边界遵循“谁产生，谁写入”：

- agentd 只写入它在公开 API 边界接收的 ingress 事实，如 `user.message` 和
  `user.interrupt`；
- Agentlet 只写入 Harness 执行、恢复和输出产生的 execution 事实，包括模型、
  工具、Checkpoint 提交与 Agent 输出；
- Managed Event Ledger Adapter 统一拥有 lane schema、幂等键、并发追加和对外投影语义，
  agentd 和 Agentlet 都不伪装成对方写事实。

因此 Agentlet 内部协议只传递 WorkSpec、wake、interrupt 和 observed state，不再代理公开
Event 读写。agentd 直接从共享 Ledger 提供 Event list/stream，并在持久化 ingress 后通知
Agentlet 执行。

### agentd 与 Agentlet 的职能边界

| 能力 | agentd | Agentlet |
|------|--------|----------|
| Managed Agents API | 拥有公开协议、View Model 和兼容性 | 不暴露公开 API |
| Event | 接收并持久化用户 ingress，直接提供 list/stream | 不接收、不代理、不查询公开 Event；只写自身产生的执行事实 |
| Session 与资源 | 拥有全局事实、期望状态和对外读模型 | 仅缓存当前 Assignment 所需的执行快照 |
| 调度 | 创建 Worker、选择 placement、维护 Assignment | 不选择 Worker，也不修改全局 Assignment |
| 执行 | 通过 `wake`、`interrupt` 发出带 Assignment 门禁的执行意图 | 校验 Assignment，驱动 Harness 并观测本地执行状态 |
| Harness 与 Sandbox | 只依赖能力契约 | 拥有 Adapter、短命 Harness runtime 和 Sandbox Engine 调用 |
| 恢复 | 决定何时、在哪里重新分配和唤醒 Work | 恢复 Harness，并调用 Sandbox Engine 提供的恢复能力 |

判断一项能力归属时，先看它是**全局控制事实**还是**单次分配内的执行实现**：前者属于
agentd，后者属于 Agentlet。共享数据库只是部署和一致性手段，不改变这个边界；尤其不能因为
Agentlet 能访问 Ledger，就让它承担公开 Event API。

Sandbox instance 的持久化、重建和迁移能力属于 Sandbox Engine。agentd 只决定 Session 的执行时机，
并把稳定的 Session 身份随 WorkSpec 传给 Agentlet；它不保存 `SandboxRef`，也不实现 sandbox 恢复。
`SandboxKey` 是 Engine 契约定义的参数，由直接 caller Agentlet 传入并解释，Engine 不解析其格式。
Hostel 与 Agentlet 同 Pod 是最小部署适配，接入 sandctl 等外部 Engine 时由其按 key 准备并恢复
sandbox。详见 `sandbox-engine.md`。

agentd 只依赖这些组件的能力契约，不依赖其内部对象或进程模型。

## 核心对象

- **Agent**：可复用、可版本化的 Harness 配置。
- **Environment**：Sandbox 和运行环境需求，不等于一台正在运行的沙箱。
- **Session**：用户看到的长期 Agent 身份。Session 可以跨进程、跨 Worker 和跨多次执行存在。
- **Work**：Session 的长期执行实体。Work 可以冻结、迁移或恢复，但不能与某个进程或 Worker 绑定。
- **Event**：用户和 Session 之间的持久化输入输出，也是唤醒 Session 的依据。
- **Worker**：一个由 Lifecycler 管理、被 Observer 观测的 Agentlet 承载单元，也是 agentd 的调度和
  容量单位。
- **Assignment**：Session 当前所在 Worker 的临时执行归属，不是产品身份。

Session 是产品身份，Work 是执行身份，Harness runtime 和 Sandbox instance 都只是可释放的
计算资源，Assignment 只是它们在某一时刻的节点归属。

## 服务主流程

1. 应用通过 agentd 创建可版本化的 Agent 和 Environment，再创建锁定两者具体版本的
   Session；
2. 用户 Event 持久化后才确认接收；Session Reconciler 以未处理 Event 为 durable demand，内存通知
   只用于降低唤醒延迟；
3. Scheduler 从最新 observation 表明存在、Ready 且未达到并发上限的 Worker 中选择实例；若无容量，
   Lifecycler 根据持久化需求创建 Worker，Observer 确认 Ready 后再持久化 Assignment；
4. agentd 直接从共享 Ledger 读取持久 Event；Connector 只把携带 Assignment 的 wake、
   interrupt 和状态请求转换为 Agentlet 内部调用；
5. Agentlet 接受执行请求后通过 Harness Adapter 创建或恢复短命 runtime；
6. Harness 执行模型循环，工具调用通过 Sandbox Engine 进入对应 Session 的隔离环境；
7. Harness 输出投影为持久化 Event，并提交最新恢复点；
8. 空闲或等待中的 Session 释放 Harness runtime，Sandbox 按引擎能力保留、休眠或回收，agentd
   释放或更新 Assignment；Lifecycler 最终回收超过 idle TTL 的空 Worker。

agentd 拥有期望状态、全局归属和执行时机，Agentlet 只拥有有效 Assignment 内可丢弃的本地执行状态，
Harness Adapter 拥有模型循环和原生会话语义，Sandbox Engine 拥有隔离资源生命周期。组件都可以
替换，Session 身份不绑定任何一个运行中实例。

## Claude API 兼容边界

agentd 保持 Claude Managed Agents 的 Agent、Environment、Session 和 Event 核心资源，以及
路径、主要 JSON 形状、错误 envelope、Session 状态和 `{domain}.{action}` Event 命名。兼容性由
官方 SDK 针对 agentd 的契约测试验证。Agentlet 的 `/internal/v1` 只服务 agentd，不属于公开兼容面；
调用方只依赖 agentd API。

Agent 支持 model、system prompt 和 toolset；Environment 的 cloud 配置由当前 Sandbox Engine
实现。MCP、Skills、Vault、Memory Store、Resource mount、Outcome、Multi-agent、Deployment 和
Webhook 不属于当前服务边界。无法提供语义保证的能力返回明确的 `unsupported_feature`，不接收后
静默降级。

兼容核心资源和行为可以复用现有客户端与 SDK；agentd 不复制尚未具备运行语义的完整产品面。

Claude 兼容面与 agentd 内部模型必须显式隔离：

- **API View Model** 只表达 Claude Managed Agents 的 request、response、分页和 error
  envelope，可随官方 API 演进；
- **Domain Model** 表达 agentd 自身的 Session、Worker、Assignment 和调度语义，不承诺
  与公开 JSON 同形；
- **DB Model** 是 Repository 内部的持久化行与索引形状，可随 agentd 业务和迁移需求
  调整，不直接暴露给 handler。

Handler 负责 API View 与 Domain command/result 的显式转换，Repository 负责 Domain 与 DB
row 的显式转换。官方 API 增加字段不意味着机械扩表，存储结构变化也不得泄漏
到兼容协议。

## 持久化、恢复与审计

Control State 拥有 Worker、Assignment 和 Session 当前决策，其模型与调度规则定义在 `agentd.md`。
Agentlet 对 Harness Checkpoint 和 Agent Ledger 的使用顺序定义在 `agentlet.md`；Ledger 对象模型、
协议和存储规范由 Agent Ledger 项目拥有。三者的权威来源独立，Kernel 不解释 Ledger Run，也不重复
展开其内部结构。

## 扩展边界

agentd 通过稳定能力契约接入其它 Harness 和本地或远端 Sandbox Engine。Control Plane 可以增加调度
策略或水平扩展，Agentlet 可以增加 Harness / Sandbox 能力；Sandbox Engine Adapter 只位于
Agentlet。这些扩展不改变 Session、Work 和 Event 的产品语义，也不把具体业务工作流引入内核。
