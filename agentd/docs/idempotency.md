# 请求幂等

## 理念与边界

幂等属于资源处理流程：调用方用稳定的 `Idempotency-Key` 标识一次提交，重试不能重复创建资源
或重复接受输入。它保证 API 提交的去重，不保证随后模型调用或工具副作用 exactly-once。

资源记录足以重建首次响应时，应复用原始记录；否则使用 `idempotency_keys` 保存响应快照。
该表只是幂等机制的数据支持，不是新的业务资源，也不提供公开 CRUD API。
API 拥有请求与响应编码，service 协调执行或重放，repo 负责记录与事务。

## Session 创建

`POST /v1/sessions` 支持可选的 `Idempotency-Key`。同一个键在 `session` 资源类型下唯一，
不同键代表不同创建意图；未携带键时，每次请求正常创建独立 Session。

1. 校验请求格式，计算请求摘要；不以当前 Agent 版本或 Session 状态计算摘要。
2. 已有记录且摘要一致时，重放首次成功响应；摘要不一致返回 `409 conflict_error`。
3. 首次请求在一个数据库事务内插入幂等记录、创建 Session、追加 initial events、保存响应。
4. 提交后才通知执行并返回响应。并发重复请求由数据库唯一约束裁决，不能仅依赖先查后写。

Session 响应包含可变的状态、标题、metadata、usage 和时长，查询当前 Session 无法重建首次响应。
因此保存当时的 HTTP 状态码与 JSON 响应字节；资源后续修改或归档不改变重放结果。
每次 HTTP 请求仍产生新的 `request-id`，不重放旧的诊断标识或连接相关响应头。

失败且事务回滚时，不保留成功凭证或部分资源，调用方可以使用原键重试。
提交结果因连接中断而不明确时，也应以同一个键重试：已提交则重放，未提交则重新执行。
重复请求可以再次唤醒协调器，但不会重新追加 initial events；周期扫描仍是遗漏通知的恢复机制。

## 存储约束

- `(resource_type, idempotency_key)` 唯一，不另设 scope、资源 ID 或操作列。
- 键按大小写敏感的原始值比较；请求摘要保留 JSON 数组顺序，但忽略对象字段顺序和空白。
- 响应快照按原始字节保存，避免数据库 JSON 类型归一化影响重放。
- 幂等记录、控制状态与 Ledger 使用同一数据库事务；事务内不得调用 Agentlet 或模型服务。
- 记录不自动过期。删除记录意味着放弃该键的历史去重保证，因此不能把它当普通可淘汰缓存。
- 不记录原始幂等键、完整请求或响应正文到运行日志。

本能力仅用于 Session 创建。Event 提交的原始事实与追加凭证仍由 Ledger 拥有；本表不自动赋予
其它资源接口 `Idempotency-Key` 语义，也不替代 Event 追加去重。

## 参考

- [Rocket Rides Atomic schema](https://github.com/brandur/rocket-rides-atomic/blob/master/schema.sql)：
  `idempotency_keys` 将请求身份与首次响应持久化。
- [Fiber idempotency middleware](https://github.com/gofiber/fiber/blob/main/middleware/idempotency/idempotency.go)：
  响应重放的编码与存储分工；agentd 额外要求资源写入与幂等记录原子提交。
