# Control Plane 与 Agentlet

## 定位

agentd 由控制面和节点执行面组成：

- **agentd** 是 Control Plane 的代码与服务边界，面向调用方提供 Managed Agents API，维护
  Agent、Environment、Session 等资源的期望状态，发现 Agentlet 实例，并为待执行的 Session
  选择有能力且有容量的实例；
- **Agentlet** 接受 Control Plane 的 Assignment，在一个进程内管理多个 Harness runtime，
  实现执行相关的 Managed Agents API 语义，负责本地执行、冻结、恢复、资源释放和状态上报。

这与 Kubernetes 的 control plane / kubelet 分工相近：Control Plane 做全局决策，Agentlet
保证分配到本实例的任务真正运行。类比只描述职责边界，不约束部署形态；agentd 预期采用
**一个 Pod 一个 Agentlet 实例**，而不是要求一台物理机只能运行一个 Agentlet。

agentd 将 API、Scheduler、Session Directory 和 Controller 收敛在同一个 Control Plane 服务中，
但这些职责保持独立模块边界。

## 名称与代码边界

目录边界为：

```text
├── cmd/
│   ├── agentd/     # Control Plane 二进制入口
│   └── agentlet/   # 节点执行二进制入口
├── agentd/         # Managed Agents API、资源状态、注册、调度、路由与 reconcile
└── agentlet/       # 执行 API、Harness runtime、Sandbox、冻结恢复与状态上报
```

Control Plane 是架构角色，`agentd` 是代码目录和部署角色。`scheduler` 和 `router` 只是 agentd
的内部职责，不能代表整个服务；`manager` 则容易与 Harness 内的 Manager 角色混淆。

Control Plane 二进制名为 `agentd`，节点执行进程名为 `agentlet`。`agentd/` 中只保留全局资源和
控制面职责；Harness、Sandbox Engine Adapter、Hostel 和本地 Run 执行属于 `agentlet/`。

## 同一套 Managed Agents API

agentd 和 Agentlet 复用 Claude Managed Agents API 的路径、请求、响应和 Event 语义：

```text
Client ── Managed Agents API ──▶ agentd
                                  │
                         schedule / lookup binding
                                  │
                  Managed Agents API + internal metadata
                                  ▼
                               Agentlet
```

agentd 对调用方是唯一公开入口。创建 Session 时，agentd 先选择 Agentlet、持久化 Assignment 和
`Session → Agentlet` Binding，再转发原始请求；已有 Session 的 Event、查询和控制请求根据 Binding
转发。Agentlet 实现这些请求的执行语义，但不成为调用方需要感知的另一个产品 API。

两端复用业务 API，不代表只存在这一套协议。Agentlet 注册、heartbeat、容量、Assignment 接受、
drain、observed state 和 fencing 属于内部控制协议；内部元数据不能加入 Claude 兼容的公开 schema。

Agent、Environment 和跨 Session 列表属于全局资源，由 agentd 保存或聚合。agentd 向 Agentlet
转发执行请求时携带固定版本的定义或可解析引用，Agentlet 不通过任意一个本地副本回答全局查询。

## Sandbox Engine 适配

Sandbox Engine Adapter 只位于 Agentlet。Environment 表达所需的隔离和运行能力，Agentlet 注册时
上报支持的 Engine、能力和可用容量；agentd 只使用这些描述做调度，不调用 Hostel 或其它具体引擎。

Agentlet 将 Managed Agents API 的 Environment / Session 语义映射到 Sandbox Engine 的创建、恢复、
执行、休眠和释放操作。Sandbox instance handle 对 agentd 不透明；Control State 只保存迁移和路由
所需的 Engine 类型、能力约束及 opaque `SandboxRef`。不同 Agentlet 可以使用不同 Sandbox Engine，
前提是满足固定 Environment 和 ResumePoint 的兼容性要求。

## 拓扑

```text
Claude-compatible API / Baton / scheduled trigger
                        │
                        ▼
              agentd / Agent Control Plane
        API / directory / registry / scheduler / router
                  │                 │
       API + Assignment A  API + Assignment B
                  │                 │
                  ▼                 ▼
            Agentlet Pod A     Agentlet Pod B
           ┌──────┴──────┐    ┌──────┴──────┐
           ▼             ▼    ▼             ▼
       Harness 1     Harness 2 ...       Harness N
           │             │                  │
           └──── Sandbox / Harness State / Ledger ────
```

一个 Agentlet 可以保有多个 Session Assignment，并在容量边界内并发运行多个 Harness
runtime。Session 是长期产品身份，Assignment 是一段执行的临时归属，Harness runtime 是可释放
的计算资源；三者不能合并为同一个对象。

空闲 Session 不等于已占用执行容量。Agentlet 可以释放其 runtime 和 Sandbox 后继续保留亲和性
信息，也可以完全释放 Assignment。Scheduler 判断实例是否已满，应基于已预留和正在使用的执行
资源，而不是简单统计该实例曾经承载过多少 Session。

## 职责边界

| 组件 | 权威职责 | 不负责 |
| --- | --- | --- |
| agentd | 公开 Managed Agents API、全局资源、Session Directory、Agentlet 注册与健康、容量视图、placement、Assignment generation、lease、fencing 与请求路由 | Harness Loop、原生会话状态、Sandbox Engine 调用 |
| Agentlet | 执行相关 Managed Agents API、Assignment 接收、本地 Run、Harness runtime、Adapter 与 Sandbox 调用、冻结恢复、容量和 observed state 上报 | 全局 placement、跨实例资源真相、全局列表聚合 |
| Harness Adapter | Harness 原生执行语义、状态 codec、安全冻结与恢复 | Session 全局归属、实例选择 |
| Ledger | 规范化执行事实、副作用边界与审计证据 | 当前调度状态、lease 和自动重放策略 |

agentd 是全局 Control State 的唯一权威来源。Agentlet 不直接推进 Control State，只上报
observed state、恢复点和容量变化；agentd 校验 Assignment generation 与 fencing token 后决定
是否提交。Agentlet 的本地内存不能覆盖全局决策。

## 控制对象

| 对象 | 含义 |
| --- | --- |
| AgentletInstance | 一个已注册的 Agentlet 进程及其版本、能力、heartbeat 和状态 |
| CapacitySnapshot | 某一 revision 下的总量、可分配量、已预留量和活跃使用量 |
| Assignment | 将一次 Session / Run 执行临时授予某个 Agentlet 的租约与 fencing 边界 |
| ResumePoint | `ResumeRef`、state revision、Harness / codec version 等组成的精确恢复位置 |

Assignment 面向执行而不是永久绑定 Session。ResumePoint 属于 Control State，但其中引用的 opaque
Harness State 仍由 Harness Adapter 拥有。

## Control State

Control State 回答“现在应该在哪里、由谁、从哪个恢复点执行”，分为两个协作视角。

### Fleet 与 Placement State

由 agentd 持有，至少包括：

- Agentlet 实例身份、版本、支持的 Harness / Sandbox 能力和健康状态；
- 容量维度及其 `capacity`、`allocatable`、`reserved`、`active`；
- Session / Run 的 placement 约束、亲和性和待调度原因；
- Assignment 的 generation、lease、fencing token 和接收状态；
- 无可用实例、排队、驱逐和重新调度等全局决策。

容量不是单个布尔值。首版可以只使用“最大并发 Harness runtime”作为容量维度，但协议应允许
后续加入 Sandbox slot、CPU、内存或特定 Harness 能力。agentd 在发出 Assignment 前先
持久化 reservation，Agentlet 接受后再转为 active，避免多个调度决策同时把同一实例放满。

### Session 与 Run State

由 agentd 持久化。Agentlet 在有效 Assignment 内上报执行观察，agentd 据此推进状态，至少包括：

- Session 对外的 `idle`、`running`、`terminated` 等状态，以及内部待调度、暂停和恢复原因；
- 固定的 Agent、Environment、Harness 和执行定义版本；
- 稳定的 Session ID、Run ID、input ID 与当前 Execution Attempt；
- 当前 Assignment、Agentlet、Sandbox 绑定及其 fencing token；
- 精确的 `ResumeRef`、state revision 和 codec version；
- 待处理输入、等待条件、暂停原因和已投影 cursor / watermark。

Control State 使用 revision 条件更新维护当前值。Agentlet 不需要加载完整 Control State，只接收
执行所需的 Assignment、固定版本和 ResumePoint。控制动作可以追加为 Ledger 事实，但 Ledger 中的
`session.suspended` 或 `ownership.acquired` 事件不能替代实时 Assignment、lease、fencing 和待唤醒
状态。

Harness State 和 Ledger 的独立所有权、恢复材料与执行事实边界见 `state-ledger.md`。

## 注册、调度与执行

1. Agentlet 启动后注册实例身份、能力和总容量，并持续上报 heartbeat、reserved、active 与
   observed state；
2. agentd 接收并持久化 Managed Agents API 请求或调度触发，生成待执行意图；
3. Scheduler 过滤不兼容或无可分配量的实例，再按负载、亲和性和故障域选择目标；
4. agentd 原子保存 reservation、Session Binding 和新一代 Assignment，再把原始 Managed Agents
   API 请求及内部 Assignment 元数据转发给 Agentlet；
5. Agentlet 校验 generation、lease 与 fencing token，接受后处理请求，并创建或从精确
   ResumePoint 恢复 Harness runtime；
6. Agentlet 执行并持续上报 observed state；完成、暂停或驱逐时持久化 Harness State，并报告
   ResumePoint 与本地资源释放结果；
7. agentd 校验 fencing 后提交 Control State、释放 active / reserved 容量，并根据最新 Binding
   路由后续请求或重新调度。

Scheduler 只做 placement 决策，Controller 负责持续把资源的期望状态收敛为 Assignment 和运行结果。
两者位于同一个 agentd 进程，接口边界仍应分开。

## 故障、迁移与 fencing

Agentlet heartbeat 超时不等于其中的外部副作用从未发生。agentd 重新调度前必须：

1. 使旧 Assignment 过期并推进 fencing token，阻止旧实例继续提交状态；
2. 确认目标 Agentlet 支持固定的 Harness、codec 和 Sandbox 能力；
3. 从 Control State 选择已经提交的精确 ResumePoint，而不是猜测“最新状态”；
4. 由 Harness Adapter 恢复原生上下文，并结合 Ledger 检查未决 Model / Tool Attempt；
5. 使用同一 Run ID 创建新的 Execution Attempt；无法证明副作用结果时进入 `terminated`，并记录
   `decision_required` 原因等待人工对账，不自动重放。

lease 用于判断 Assignment 是否仍然活跃，fencing token 用于拒绝迟到写入；两者不能互相替代。
进程存活、日志和 trace 也不能替代持久化 Control State。

## 单实例与多实例部署

单实例模式可以把 agentd 和 Agentlet 装配在同一进程：Scheduler 总是选择本地 Agentlet，
Assignment 仍通过同一语义边界创建和接受。这是部署上的折叠，不是概念上的合并。

多实例模式将两者拆成独立进程和 Pod。agentd 可以水平扩展，但共享同一个资源与 Assignment
事实来源；每个 Agentlet 独立上报容量和 observed state。调用方只连接 agentd，不直接依据
某个 Agentlet 的本地状态决定 Session 归属。

## 不变量

1. agentd 决定全局 placement；Agentlet 只执行有效 Assignment。
2. 一个 Agentlet 可以运行多个 Harness runtime，但每个 runtime 必须计入 reservation 或 active
   容量。
3. Assignment 是临时执行归属，不能成为 Session 的产品身份或 Harness State 的所有者。
4. Agentlet 的 observed state 报告必须携带当前 generation 与 fencing token；只有 agentd 可以将其
   提交为 Control State。
5. 等待态只有在 Harness State 已持久化且 Control State 已提交精确 ResumePoint 后才可释放资源。
6. Scheduler 的无容量判断来自可版本化容量快照和 reservation，不能只依赖 heartbeat 中的一个
   `full` 标志。
7. 多实例恢复不能只依赖进程存活、日志、trace 或 Ledger 推断当前 owner。
