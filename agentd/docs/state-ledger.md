# Harness State 与 Ledger

## 定位

agentd 要让一个 Session 跨越 Agentlet、Harness runtime 和沙箱进程的生命周期，需要保存两类
语义不同的执行数据：

- **Harness State** 让具体 Harness 无损重建原生上下文并继续运行；
- **Ledger** 记录执行期间已经发生的事实，支持安全判断、审计和轨迹提取。

“现在应该在哪里、由谁、从哪个恢复点执行”属于 Control State，在 `control-plane.md` 中定义。
三者可以复用数据库或对象存储等物理基础设施，但不能共用同一个语义模型或互相充当事实来源。

## 所有权与数据语义

| 数据面 | 回答的问题 | Owner | 主要语义 | 生命周期 |
| --- | --- | --- | --- | --- |
| Harness State | 如何还原该 Harness 的原生上下文并继续 | Harness 与对应 Harness Adapter | Harness-specific、无损、版本敏感 | 可 snapshot、compact 和 GC |
| Ledger | 已经发生了什么，哪些外部动作处于什么边界 | Agent Ledger | 跨 Harness 的规范化、不可变执行事实 | 长期追加，供审计和投影 |

### Harness State

Harness State 由 Harness 解释，可能是消息序列、上下文压缩结果、会话树、checkpoint、远端
Harness session ID，或其它原生恢复材料。Control Plane 和 Agentlet 都不定义统一 State schema，
也不读取其内容。Harness 的执行边界和 Adapter 契约见 [harness.md](harness.md)，本节只定义状态的
数据所有权与一致性要求。

Agentlet 只通过 Harness Adapter 使用生命周期能力：

```text
Freeze(session) -> ResumeRef
Restore(session, ResumeRef) -> Harness runtime
```

Harness Adapter 负责确定安全冻结边界、持久化原生状态、校验 Harness 与 codec 版本，并调用
Harness 自己的恢复或继续 API。`ResumeRef` 对 Agentlet 和 Control Plane 是不透明引用；其底层
可以使用 agentd 提供的 blob / KV backend，也可以指向 Harness 自己的持久化服务。

Harness State 即使通过 Agent Ledger 的 CheckpointStore 保存，也仍属于 Harness Adapter；Store 只解释
Checkpoint envelope，不解释其中的原生状态，审计与轨迹投影也不能依赖其格式。

首个 AgentGo Adapter 将已提交的原生 message 序列保存为版本化 Checkpoint。CheckpointKey 组织同一
Session 的多个 revision，Control State 中的 `ResumeRef` 则引用某个精确 Checkpoint ID；尚未被
Control State 采纳的 revision 不能仅因它更新就自动成为恢复基线。其它 Harness 可以使用不同格式、
snapshot 或远端 session ID，只要实现相同的准备、运行和恢复能力，不要求转换成 AgentGo 的状态模型。
Checkpoint 可以锚定某条 Lane 上已经吸收的最后一个 Event；恢复器只处理 Anchor 之后的 Ledger 尾部。

### Ledger

Agent Ledger 的执行层级是 `Session → Run → Lane → Turn → Action → Attempt`。Action 表示
`model_call`、`tool_call`、`compact` 等逻辑动作，Attempt 表示一次物理尝试；重试沿用 Action 并递增
Attempt 序号。每个 Harness 的 recording adapter 把原生 hook 翻译为规范化事实，但不借此拥有
Harness 的恢复状态。

Ledger 主要支持：

- 判断模型或工具 Attempt 是否完整、失败或结果不明确；
- 展示 Session 的全局 Timeline 和外部副作用边界；
- 关联模型、Prompt、Tool、Skill、Harness 和代码版本；
- 将事件流投影为供分析、评测和优化消费的 Trajectory。

Ledger 不决定是否恢复、重试或发布新的能力版本。它提供决策证据，策略仍由 Control Plane、
Harness Adapter 或下游 Eval / Optimizer 拥有。

## 冻结与恢复

### 冻结

```text
Harness reaches a safe boundary
          │
          ▼
Adapter persists a Harness Checkpoint
          │
          ▼
Adapter returns the exact Checkpoint ID
          │
          ▼
Agentlet reports ResumePoint with Assignment token
          │
          ▼
agentd conditionally commits ResumePoint to Control State
          │
          ▼
release Assignment, runtime and optional sandbox resources
```

必须先得到可读取的 Checkpoint ID，Agentlet 再把它作为 `ResumeRef` 上报 ResumePoint，由 agentd 把
Control State 提交为可释放资源的等待态。若 Checkpoint 已保存但 Control State 尚未更新就崩溃，新
revision 只是未引用版本，可以对账后回收；反向顺序会让 Session 指向尚未完成的恢复材料。
ResumePoint、Assignment generation 和条件更新规则见 `control-plane.md`。

### 恢复

1. agentd 持久化外部输入或定时条件，并为 Session 创建有效 Assignment；
2. Agentlet 从 Assignment 与请求上下文获得固定的 Harness 版本和精确 `ResumeRef`，不读取完整
   Control State；
3. Harness Adapter 校验兼容性并恢复原生上下文；
4. Agentlet 和 Adapter 检查 Ledger 中未决的 Model / Tool Attempt；
5. 可安全重试的动作生成新 Attempt，结果不明确的副作用进入对账或人工决策；
6. 恢复成功后才进入下一轮 Harness Loop。

Harness State 负责还原上下文，Ledger 负责证明崩溃边界；只有两者都满足条件，恢复才是安全的。

AgentGo Adapter 把来源 input ID 写入原生 user message。恢复同一 input 时：尚未提交 user message 才
执行 `Prompt`；已提交且停在 user / tool result 后则从现有上下文继续；已存在最终 assistant message
则只补齐幂等的 API Event 投影。若 Ledger 存在无终态 Tool Attempt，或状态停在无法证明结果的
tool-call 边界，Session 进入 `terminated`，等待人工对账，不自动重放工具。

`idle` 是当前 Claude API 下的冻结等待态：Harness State 和对应 revision 已持久化，AgentGo runtime
随本轮执行结束释放；新输入到达后才重建 runtime。Sandbox 是否保留、休眠或释放仍由 Sandbox Engine
能力决定。

## 存储与一致性

agentd 通过 Agent Ledger CheckpointStore 和 EventStore 两个稳定接口使用这两类数据。
`PersistenceProvider` 根据配置组装具体实现，Control Plane、Agentlet 与 Harness 不接触 GORM、Bolt
或连接信息。Control State Repository 是独立接口，定义在 `control-plane.md`。

当前提供 MySQL / GORM Provider：复用一个显式配置连接池与超时的 `*gorm.DB`，Checkpoint 与 Ledger
直接使用 Agent Ledger 提供的 GORM Store，但仍使用独立表和存储接口。可替换性来自接口和依赖注入，
Agentlet 不重复实现 Agent Ledger backend；增加其它数据库、对象存储或远端 Harness 自带存储时，
不改变 Control Plane、Agentlet 与 Harness。

Agent Ledger 仓库可以继续提供 Bolt 等独立 EventStore 实现，但它们不是 agentd 的默认部署组成。

同库部署也应保持独立 namespace、访问接口和保留策略：

- Harness State 由 Adapter 按原生语义保存版本、snapshot 或外部引用；
- Ledger 使用不可变追加、幂等键和乐观并发保存事实。

跨数据面的更新不应伪装成 exactly-once。Agentlet 和 Adapter 通过稳定 ID、精确 `ResumeRef`、Ledger
event ID 与已覆盖 cursor 识别不完整窗口，并在恢复时对账收敛。Trace 和日志只用于诊断，不能充当
任何一个数据面的权威存储。

当前 AgentGo CheckpointKey 下的 revision 已使用乐观并发检查。只有当对应精确 Checkpoint ID 被带
Assignment generation 与 fencing token 的 Control State 更新接受后，该 revision 才能成为当前
恢复点。

## 轨迹与能力优化

Trajectory 只从规范化 Ledger 事实投影，不依赖可能被 compact 或 GC 的 Harness State。为支持归因，
Run 必须记录模型、Prompt、Toolset、Skill、Harness 和代码版本；模型输入输出等大对象通过
ArtifactRef 关联。

Eval、Optimizer 和能力发布系统是 Ledger 的下游消费者：它们选择轨迹、生成候选 Prompt / Skill /
Tool 版本并验证效果。agentd 只在创建或恢复 Session 时固定已批准的能力版本，不根据历史轨迹自行
修改正在运行的 Agent。

## 不变量

1. Harness State 的格式和恢复算法归对应 Harness Adapter，agentd 只持有不透明 `ResumeRef`。
2. Ledger 协议不依赖 Harness State schema，Trajectory 也不能要求解析 native state。
3. 等待态必须引用已持久化且版本兼容的 Harness State，之后才能释放执行资源。
4. 未决且可能产生副作用的 Tool Attempt 必须先对账，不能因 Harness State 已恢复就自动重放。
5. 同一用户 input 的 Harness message 和 API Event 使用稳定身份，恢复不得重复注入 Prompt 或重复投影。
6. Harness State 和 Ledger 可以共用物理存储，但不能互相替代所有权或恢复语义。
