# Sandbox Engine

## 定位

Sandbox Engine 是 Agentlet 与隔离执行环境之间的稳定边界。agentd 与 Agentlet 都不实现容器、
虚拟机、文件系统或远端执行服务；Agentlet 只通过 Engine 契约为 Harness 提供文件操作和命令执行
能力。

agentd 参考 [OpenSandbox Lifecycle API](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/sandbox-lifecycle.yml)
和 [Execd API](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/execd-api.yaml) 对 Sandbox
生命周期与执行面的划分，但不把 OpenSandbox 作为对外协议或默认实现。Go `Engine` 是 Agentlet
内部的能力契约，不复制 OpenAPI，也不把具体服务的 HTTP 路由暴露给 Control Plane 或 Harness。

这条边界把长期身份与短期计算资源分开：

- **Environment** 描述 Session 需要的运行环境，不是一台已经启动的沙箱；
- **Session** 是用户看到的长期身份，可以跨 agentd、Agentlet、Harness runtime 和 Sandbox instance
  存在；
- **Sandbox Engine** 负责按 caller 提供的 `SandboxKey` 解析或准备可用的隔离 Workspace；
- **Sandbox instance** 是 Engine 管理的可释放资源，可以是 Hostel Bed、容器、虚拟机或远端实例。

`SandboxKey` 是 Sandbox Engine 契约定义的调用参数；它的 value 由 Engine caller 传入，含义、组成和
生命周期也由 caller 解释和管理。Engine 必须把它当作 opaque key，不能假设其格式或把 provider
identity 泄漏给 caller。当前 Agentlet 是直接 caller，它把 agentd 在 `WorkSpec` 中传入的 Session ID
解释为 `SandboxKey`；其它 caller 可以按自身身份和隔离模型选择 conversation ID，或拼成
`tenant:session` 等更完整的 key。

Go 契约将 `SandboxKey` 定义为 model，而不是裸字符串；当前 model 只有 `Value string`。后续若 Engine
需要表达 group、scope 等通用寻址维度，可以向该 model 增加字段，而不修改每个 Workspace 和 Execute
方法的签名。字段仍由 caller 赋值和解释，不能演变成 agentd 持有的 provider resource model。

Engine 负责维护 `SandboxKey` 到 Bed、容器、虚拟机或远端实例的内部映射，并按 key 幂等地查找、
创建或恢复资源。agentd 不保存 Engine 返回的 `SandboxRef`，也不理解 provider resource ID；provider
identity 不能取代 Session ID 成为产品身份。

## 所有权边界

| 组件 | 拥有什么 | 不拥有什么 |
| --- | --- | --- |
| agentd Control Plane | Session 生命周期、Assignment 与执行时机；通过 WorkSpec 传递稳定的 Session 身份 | SandboxKey、SandboxRef、provider identity、Sandbox instance 的恢复与释放机制 |
| Agentlet | 作为 Engine caller 选择并解释 SandboxKey，调用 Engine 能力 | provider binding、全局 placement |
| Harness Adapter | 将 Harness 的 Tool/Workspace API 映射到 Engine | Sandbox instance 生命周期 |
| Sandbox Engine | 隔离环境、Workspace、命令执行和底层资源生命周期 | Agent Loop、Session 调度、重试决策 |
| Agent Ledger Adapter | Tool Attempt 和副作用边界的规范化事实 | 命令执行、Workspace 状态 |

Sandbox Engine 不直接写 Ledger，也不根据 Tool 结果决定是否重试。Harness Adapter 在调用 Engine 前后
记录 `tool.requested`、`tool.completed` 或失败事实；结果不明确时，由 Agentlet 上报证据，agentd
结合 Harness State 和 Ledger 决定对账、终止或恢复，不能让 Engine 静默重放命令。

Sandbox 的可恢复性属于 Sandbox Engine 的能力边界。agentd 只决定 Session 何时需要继续执行，并把
稳定的 Session 身份随 WorkSpec 传给 Agentlet；Agentlet 作为 caller 将其解释为 `SandboxKey`。
Workspace 如何持久化、Sandbox instance 如何重建或迁移、恢复后如何重新解析 endpoint，均由 Engine
按 key 保证。真实环境接入 sandctl 等具备控制面与持久化能力的 Engine 时，Agentlet 直接通过 Adapter
使用其能力，agentd 不实现一套 sandbox 恢复控制器。

Agent Control Plane 与 Sandbox Control Plane 独立调度。agentd 只为减少 Harness 恢复、模型调用重做
和工具副作用对账而尽量保留现有 Assignment，不读取 Sandbox locality，也不因 Bed、容器或虚拟机位置
选择 Worker。Sandbox Engine 必须向 Agentlet 屏蔽物理位置变化，并按 key 保证执行环境可用。

最小 Kubernetes 部署把 Hostel 与 Agentlet 放入同一个 Worker Pod，只是为了在没有外部 Sandbox
服务时提供开箱即用的 Engine。这个部署便利不改变所有权边界，也不能据此要求 agentd 在 Worker Pod
丢失后恢复 Hostel Bed；需要跨 Pod 保留和恢复 Workspace 时，应替换为具备对应语义的 Sandbox Engine。

## OpenSandbox 设计参考

OpenSandbox 将控制面和执行面分开，这个划分帮助 agentd 明确自己的组件边界：

| 协议面 | OpenSandbox 契约 | agentd 使用方式 |
| --- | --- | --- |
| Lifecycle | 创建、查询、暂停、恢复、续期和删除 Sandbox；从 image 或 snapshot 启动 | Sandbox Control Plane 决定资源策略，Agentlet 只按 key 调用 Engine |
| Runtime configuration | entrypoint、环境变量、CPU/内存/GPU、volume、network policy、TTL 和扩展字段 | Environment 提供不可变需求，Engine Adapter 转换为创建参数 |
| Endpoint | 解析 Sandbox 内端口的访问地址和访问凭据 | Adapter 解析 Execd 或其它服务 endpoint，不把地址持久化为 Session 身份 |
| Execd | 命令、文件、目录、执行上下文、SSE 输出和系统指标 | Harness 的 Tool/Workspace API 通过 Engine 调用 |

OpenSandbox 的 Sandbox 状态以 `Pending`、`Running`、`Pausing`、`Paused`、`Resuming`、`Stopping`、
`Terminated` 和 `Failed` 表达资源生命周期。它们不同于 agentd Session 的 `running`、`idle`、
`rescheduling` 和 `terminated`：前者描述计算资源，后者描述长期 Agent 是否应该执行。Sandbox 状态
不能直接投影成 Session 状态，也不能成为 agentd Worker 调度事实。

agentd 不把 Sandbox 实现作为自身差异化能力。当 OpenSandbox 形成稳定、广泛采用且能够表达 agentd
所需语义的行业规范时，应优先兼容规范并复用其 SDK，而不是继续扩展私有协议。内部
`sandbox.Engine` 仍作为 Agentlet/Harness 的稳定依赖边界，对外实现则可以直接接入兼容的本地或
远端 Sandbox 服务。这样既降低 Engine 接入成本，也让 OpenSandbox 生态中的部署、工具和供应商能够直接
成为 agentd 的可选运行环境。

agentd 的可用范围也取决于对 Sandbox Engine 的适配度。符合主流规范的 Engine 应尽量做到只需配置
endpoint 和凭据即可接入；未完全兼容规范的 Engine 只实现薄 Adapter，不把 provider 差异带入
Control Plane 与 Harness。所有实现使用同一组 Engine 契约测试验证 Workspace、命令执行、超时取消、
资源恢复和错误语义，适配度以语义完整性衡量，而不是以接入数量衡量。

是否采用规范由能力覆盖和兼容性决定，不以名字相似为依据。规范暂时无法表达的恢复、安全或资源语义，
继续留在 agentd 契约中明确建模，不能通过静默降级换取表面兼容。

## 能力契约

`agentlet/internal/sandbox/engine.Engine` 暴露 Agentlet 当前需要的最小能力。以下操作中的 sandbox
参数都是 Engine 契约定义、caller 传入和解释的 `SandboxKey`，不是 Engine 分配的 resource ID：

1. **可用性**：`Ensure` 幂等地创建、解析或重新连接 Sandbox；调用成功即表示当前 key 可用。
2. **Workspace**：提供 `Stat`、`ReadFile`、`ReadDir`、`WriteFile` 和 `MkdirAll`，供 Harness 的文件工具使用。
3. **执行**：`Execute` 在指定 Workspace 中运行带工作目录和超时的命令，并返回输出、退出码和终止原因。

所有操作都接收 `context.Context`，调用方取消、超时和 Engine 自身的资源上限必须能够终止正在等待的
请求。非零退出码属于命令结果；连接失败、协议损坏或无法确认执行结果才作为调用错误返回。Harness
只使用 `/workspace` 内的路径，不依赖 Agentlet 所在主机的本地路径。

OpenSandbox 对 pause/resume、delete、TTL renewal、snapshot/restore 和 endpoint resolution 的设计可供
Sandbox Engine 实现参考。Agentlet 当前只需要“按 key 随时可用”的语义，所以 Go 契约不暴露这些
物理资源动作；它们由 Sandbox Control Plane 收口，不能让 agentd 根据 Session 状态猜测底层行为。

## 主流程

```text
durable user Event
        │
        ▼
agentd assigns Session to Agentlet
        │
        ▼
Agentlet restores Harness State
        │
        ▼
Engine ensures or reconnects Sandbox binding
        │
        ▼
model emits Tool call ──► Ledger records requested Attempt
        │
        ▼
Sandbox Engine reads/writes Workspace or executes command
        │
        ▼
Ledger records result ──► Harness commits native state
        │
        ▼
Session becomes idle; runtime is released
```

Engine 可以在 `Ensure` 背后启动本地资源，也可以调用远端服务。Agentlet 只观察契约结果，不要求
Engine 与自己同进程、同主机或共享文件系统。

## 冻结与恢复

Harness State 和 Sandbox Workspace 是两类不同的恢复材料。Harness State 保存消息、上下文和 Tool
结果；Workspace 保存 Agent 在隔离环境中产生的文件。只恢复其中一类，不代表 Session 已完整恢复。

安全恢复遵循以下约束：

1. Agentlet 接受 Assignment 后把 WorkSpec 中的稳定 Session 身份解释为 `SandboxKey` 并调用
   `Ensure`，由 Engine 重新解析 endpoint；Agentlet 不缓存临时实例地址或 provider resource ID；
2. Engine 若保留 Workspace，必须让重新连接后的 Session 看到一致内容；
3. Engine 若会因 TTL 或策略回收 Workspace，必须先通过 snapshot、artifact 或 volume 提供耐久恢复路径；
4. 未决 Tool Attempt 是否可重放由 Ledger 和 agentd 判断，Workspace 中出现文件不能单独证明命令成功；
5. `Ensure` 只负责资源就绪，不授予 Session 执行所有权；执行归属仍由 Assignment 表达。

这里的“安全恢复”是 agentd/Agentlet 对 Engine 能力的使用协议，不表示 agentd 实现 Sandbox
恢复。若 Engine 不声明并提供所需的持久化或恢复语义，agentd 不能靠 Ledger、Assignment 或重复
`Ensure` 补出这些能力。

## 隔离与运行要求

Engine 实现至少应满足以下要求：

- **身份隔离**：不同 Sandbox ID 不能读取彼此的 Workspace、进程或凭据；
- **资源边界**：显式限制命令时长、并发、CPU、内存、磁盘和输出大小，服务端不能只依赖客户端超时；
- **网络边界**：通过 Environment 映射 OpenSandbox network policy，不继承 Agentlet 主机权限；
- **凭据边界**：Lifecycle API key、Execd access token 和工作负载凭据分别管理，不进入命令参数、日志、Ledger payload 或 Workspace 快照；
- **可观测性**：记录 Engine、Sandbox ID、Execution ID、耗时和终止原因，同时避免记录敏感输入输出；
- **远端安全**：远端 Engine 必须配置认证、加密传输、连接池、请求超时和容量上限。

这些要求由具体 Engine 和部署环境落实。Sandbox Control Plane 负责资源策略和调度，Agentlet 负责
配置、调用和验证能力；agentd 不感知这组物理事实。各组件都不能用一个宽泛的 Engine 接口掩盖实现
无法提供的隔离或持久性保证。

## Hostel Adapter

Hostel 的设计参考了 OpenSandbox，`agentlet/internal/sandbox/hostel` 是当前唯一的
`sandbox.Engine` 实现：

- 一个 Session ID 对应一个 Bed ID；
- `Ensure` 创建或重新连接 Bed，并等待其进入可用状态；
- 文件操作和命令流沿用 OpenSandbox Execd 语义，通过 Hostel HTTP API 完成；
- Quick Start 把 Hostel 作为 Worker Pod sidecar 运行，由 Kubernetes 管理进程生命周期和 readiness；
  Agentlet 只通过 endpoint 使用它，不负责启动或保活 Engine。正式部署由独立 Sandbox Control Plane
  提供相同接口。

Hostel 的 Bed 命名、endpoint 和协议细节只存在于 Adapter 内部。接入其它 Engine 时，可以继续参考
OpenSandbox 的 Sandbox、Execution 和 Workspace 设计，但只需实现 Agentlet 的 `sandbox.Engine` 契约并
复用 Sandbox/Harness 契约测试，不让 provider 专属类型和 HTTP header 进入 Control Plane、Agentlet
或 Harness。
