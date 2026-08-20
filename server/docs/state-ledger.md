# Control State、Harness State 与 Ledger

## 定位

agentd 要让一个 Session 跨越 Web Server、Harness runtime 和沙箱进程的生命周期，需要同时
保存三类语义不同的数据：

- **Control State** 决定 Session 当前是否应该执行、等待或恢复；
- **Harness State** 让具体 Harness 能无损重建上下文并继续运行；
- **Ledger** 记录执行期间已经发生的事实，支持安全判断、审计和轨迹提取。

三者可以复用数据库或对象存储等物理基础设施，但不能共用同一个语义模型或互相充当事实来源。

## 所有权与数据语义

| 数据面 | 回答的问题 | Owner | 主要语义 | 生命周期 |
| --- | --- | --- | --- | --- |
| Control State | 现在是否运行、由谁运行、从哪个恢复点运行 | agentd Session Controller | 可变状态、期望状态、所有权与恢复引用 | 跟随 Session，可 CAS 更新 |
| Harness State | 如何还原该 Harness 的原生上下文并继续 | Harness 与对应 Harness Adapter | Harness-specific、无损、版本敏感 | 可 snapshot、compact 和 GC |
| Ledger | 已经发生了什么，哪些外部动作处于什么边界 | Agent Ledger | 跨 Harness 的规范化、不可变执行事实 | 长期追加，供审计和投影 |

### Control State

agentd 拥有 Session 生命周期和恢复时机，因此 Control State 至少覆盖：

- Session 的运行、等待、恢复和终止状态；
- 固定的 Agent、Harness 与执行定义版本；
- 待处理输入、恢复引用和资源绑定；
- 多实例下的 owner、lease 与 fencing token；
- 重试、等待用户或需要人工决策等控制结果。

Control State 是 agentd 调度的权威状态。控制动作可以作为事实写入 Ledger，但 Ledger 中的
`session.suspended` 或 `ownership.acquired` 事件不能替代实时 lease、fencing 和待唤醒状态。

### Harness State

Harness State 由 Harness 解释，可能是消息序列、上下文压缩结果、会话树、checkpoint、远端
Harness session ID，或其它原生恢复材料。agentd 不定义统一 State schema，也不读取其内容。
Harness 的执行边界和 Adapter 契约见 [harness.md](harness.md)，本节只定义状态的数据所有权与
一致性要求。

agentd 只定义生命周期所需的能力契约：

```text
Freeze(session) -> ResumeRef
Restore(session, ResumeRef) -> Harness runtime
```

Harness Adapter 负责确定安全冻结边界、持久化原生状态、校验 Harness 与 codec 版本，并调用
Harness 自己的恢复或继续 API。`ResumeRef` 对 Session Controller 是不透明引用；其底层可以使用
agentd 提供的 blob/KV backend，也可以指向 Harness 自己的持久化服务。

Harness State 的物理记录即使暂时复用 Agent Ledger 的 EventStore，也仍属于 Harness Adapter，
不进入跨 Harness Ledger 协议，不能成为审计或轨迹投影必须理解的数据格式。

首个 AgentGo Adapter 将已提交的原生 message 序列保存为版本化 opaque record。其它 Harness
可以使用不同格式、snapshot 或远端 session ID，只要实现相同的准备、运行和恢复能力，不要求
转换成 AgentGo 的状态模型。

### Ledger

Agent Ledger 定义跨 Harness 的 Run、Step、Model Attempt、Tool Attempt、因果关系、幂等追加和
write-before-execute 契约。每个 Harness 的 recording adapter 把原生 hook 翻译为规范化事实，
但不借此拥有 Harness 的恢复状态。

Ledger 主要支持：

- 判断模型或工具 Attempt 是否完整、失败或结果不明确；
- 展示 Session 的全局 Timeline 和外部副作用边界；
- 关联模型、Prompt、Tool、Skill、Harness 和代码版本；
- 将事件流投影为供分析、评测和优化消费的 Trajectory。

Ledger 不决定是否恢复、重试或发布新的能力版本。它提供决策证据，策略仍由 agentd、Harness
Adapter 或下游 Eval/Optimizer 拥有。

## 冻结与恢复流程

### 冻结

```text
Harness reaches a safe boundary
          │
          ▼
Adapter persists Harness State
          │
          ▼
Adapter returns durable ResumeRef
          │
          ▼
agentd commits waiting state + ResumeRef
          │
          ▼
release ownership, runtime and optional sandbox resources
```

必须先得到可读取的 `ResumeRef`，再把 Control State 提交为可释放资源的等待态。若 State 已保存但
Control State 尚未更新就崩溃，新 State 只是未引用版本，可以回收；反向顺序会让 Session 指向尚未
完成的恢复材料。

### 恢复

1. 外部输入或定时条件先持久化，再触发 Session 唤醒；
2. agentd 获取执行所有权并读取固定的 Harness 版本与 `ResumeRef`；
3. Harness Adapter 校验兼容性并恢复原生上下文；
4. agentd 和 Adapter 检查 Ledger 中未决的 Model/Tool Attempt；
5. 可安全重试的动作生成新 Attempt，结果不明确的副作用进入对账或人工决策；
6. 恢复成功后才进入下一轮 Harness Loop。

Harness State 负责还原上下文，Ledger 负责证明崩溃边界；只有两者都满足条件，恢复才是安全的。

AgentGo Adapter 把来源 input ID 写入原生 user message。恢复同一 input 时：尚未提交 user message 才
执行 `Prompt`；已提交且停在 user/tool result 后则从现有上下文继续；已存在最终 assistant message
则只补齐幂等的 API Event 投影。若 Ledger 存在无终态 Tool Attempt，或状态停在无法证明结果的
tool-call 边界，Session 进入 `terminated`，等待人工对账，不自动重放工具。

`idle` 是当前 Claude API 下的冻结等待态：Harness State 和对应 revision 已持久化，AgentGo runtime
随本轮执行结束释放；新输入到达后才重建 runtime。Sandbox 是否保留、休眠或释放仍由 Sandbox Engine
能力决定。

## 存储与一致性

agentd 通过三个稳定接口使用持久化：Repository、Harness State Store 和 Agent Ledger
EventStore。`PersistenceProvider` 根据配置组装具体实现，Controller 与 Harness 不接触 GORM、Bolt
或连接信息。

当前提供 MySQL/GORM Provider：复用一个显式配置连接池与超时的 `*gorm.DB`，但三个数据面使用
独立表和存储接口。可替换性来自接口和依赖注入，agentd 不为此同时维护多套 backend。后续增加
其它数据库、对象存储或远端 Harness 自带存储时，不改变 Controller 与 Harness。

Agent Ledger 仓库可以继续提供 Bolt 等独立 EventStore 实现，但它们不是 agentd 的默认部署组成。

三个数据面不要求使用三套数据库，但同库部署也应保持独立 namespace、访问接口和保留策略：

- Control State 使用条件更新维护当前值和所有权；
- Harness State 由 Adapter 按原生语义保存版本、snapshot 或外部引用；
- Ledger 使用不可变追加、幂等键和乐观并发保存事实。

跨数据面的更新不应伪装成 exactly-once。Controller 和 Adapter 通过稳定 ID、`ResumeRef`、Ledger
event ID 与已覆盖 cursor 识别不完整窗口，并以恢复时对账收敛。Trace 和日志只用于诊断，不能充当
任何一个数据面的权威存储。

单实例 agentd 持续对账已持久化但尚未处理的用户输入。Worker 因短暂存储错误退出时，输入仍保留在
Ledger，Control State 进入 `rescheduling`；周期性 reconciler 在确认该 Session 没有活跃 Worker 后
重新拉起执行。控制事件和输出使用稳定 Event ID，因此重复对账不会产生重复投影。这个机制解决单进程
内部的遗漏唤醒和瞬时失败，不提供多实例执行所有权。

当前实现面向单 agentd 进程：Harness State 追加已使用 revision 做乐观并发检查，Control State
记录 revision，但多实例所需的数据库条件更新、lease 和 fencing 尚未实现。

## 轨迹与能力优化

Trajectory 只从规范化 Ledger 事实投影，不依赖可能被 compact 或 GC 的 Harness State。为支持归因，
Run 必须记录模型、Prompt、Toolset、Skill、Harness 和代码版本；模型输入输出等大对象通过
ArtifactRef 关联。

Eval、Optimizer 和能力发布系统是 Ledger 的下游消费者：它们选择轨迹、生成候选 Prompt/Skill/Tool
版本并验证效果。agentd 只在创建或恢复 Session 时固定已批准的能力版本，不根据历史轨迹自行修改
正在运行的 Agent。

## 不变量

1. Control State 是调度和所有权的权威来源，Ledger 事件只是其审计事实。
2. Harness State 的格式和恢复算法归对应 Harness Adapter，agentd 只持有不透明 `ResumeRef`。
3. Ledger 协议不依赖 Harness State schema，Trajectory 也不能要求解析 native state。
4. 等待态必须引用已持久化且版本兼容的 Harness State，之后才能释放执行资源。
5. 未决且可能产生副作用的 Tool Attempt 必须先对账，不能因 Harness State 已恢复就自动重放。
6. 同一用户 input 的 Harness message 和 API Event 使用稳定身份，恢复不得重复注入 Prompt 或重复投影。
