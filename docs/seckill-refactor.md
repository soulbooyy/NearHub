# 秒杀成功标记精简重构记录

本文描述 2026-10-31 的当前实现，替代原四状态与自动缺单恢复方案。旧阶段实现和测试记录可在 Git 历史中查阅；旧版 107 项测试结果不代表当前版本的验证结果。

## 当前链路与技术取舍

登录鉴权 → ZSET + Lua 用户限流 → 生成订单 ID → RocketMQ 半消息 → 本地事务回调执行 Lua（库存校验、一人一单、预扣、Set/Hash 归属、成功标记 TTL）→ COMMIT → Consumer → 独立 MySQL 事务条件扣库存与订单插入。

- 成功标记为 `seckill:txn:{orderId} = 1`，默认 86400 秒。失败请求不创建事务记录，读取与同订单回调不续期。
- 回查读标记：存在 COMMIT；Redis 查询异常 UNKNOWN；缺失时消息年龄不足 60000 ms 暂时 UNKNOWN，之后 ROLLBACK。使用原始 MQ `bornTimestamp`，不登记准备状态。
- 同订单归属存在但标记缺失时，本地回调返回 UNKNOWN，不再次扣库存或补建成功标记。短期标记到期不删除购买资格。
- Consumer 先查 MySQL 幂等；首次消费核验 Set/Hash 原订单归属，不要求成功标记仍在。事务回滚后重新查库分类、数据库主键/联合唯一索引、失败重试均保留。
- 按订单 ID 查询未落库记录统一返回 UNKNOWN，不用没有身份字段的标记泄露他人预占状态。按当前用户与券查询可借助归属 Hash 找回原订单，返回 PROCESSING/UNKNOWN；落库后两条接口均检查身份并返回 SUCCESS 和订单状态。
- 缺单改为活动结束超过 2 分钟后的只读对账与日志，保留对账锁；不自动重投或按差额改库存。订单关单与 Redis 库存回补仍有持久化任务，不能与删除的缺单恢复混为一谈。

正常提交少了一次 PREPARE Lua 调用；回查改为 GET 标记，不再先查 MySQL。首次消费复用现有归属查询（HGET、SISMEMBER 和两个 TTL 检查），不再执行原 CHECK Lua，Redis 往返成本因此也有变化。没有测量整体耗时或吞吐收益，不能据代码量推断性能提高。

## 修改文件

Java 路径以下均相对于 `src/main/java/com/hmdp/`：

| 文件 | 调整 |
| --- | --- |
| `service/SeckillReservationService.java` | 删除 prepare/check/read/identity/SCAN 状态协议；保留 reserve、归属查询、身份校验，新增成功标记读取和 TTL 配置校验 |
| `mq/SeckillTransactionListener.java` | Lua 成功/业务拒绝/异常映射 COMMIT/ROLLBACK/UNKNOWN；按标记和消息年龄回查，移除 MySQL 回查依赖 |
| `service/impl/VoucherOrderServiceImpl.java` | 提交前不再 PREPARE；消费使用长期归属；删除恢复任务和事务 Hash 查询依赖 |
| `task/SeckillReconcileTask.java` | 删除恢复任务发现；活动结束后等待 2 分钟做缺单告警，保持只读库存核对与关单兜底 |
| `mq/SeckillTxContext.java`、`utils/RedisConstants.java` | 清理废弃返回码与注释 |
| `src/main/resources/seckill.lua`（仓库相对路径） | 删除三动作、四状态和诊断字段；预校验后写成功标记与 TTL |
| `src/main/resources/application.yaml`、`.env.example`（仓库相对路径） | 删除 recovery 配置，新增标记 TTL 和回查宽限；保留 compensation 配置 |
| `scripts/dev.sh`、`README.md`、本文（仓库相对路径） | 测试入口去除缺单恢复依赖；更新架构、故障和升级说明 |

删除 `task/SeckillRecoveryWorker.java`、`entity/SeckillRecoveryTask.java`、`mapper/SeckillRecoveryTaskMapper.java` 及其专属 `src/test/java/com/hmdp/order/SeckillRecoveryIntegrationTest.java`。不再维护缺单恢复租约、Token、退避、重投预算或事务 Key 全量 SCAN。

保留 `V003__seckill_recovery_task.sql` 历史迁移及正常初始化迁移顺序；没有执行 DROP、TRUNCATE、旧任务清理或 Redis 历史证据清理。隔离订单测试库不再需要该表。

未改动独立持久化事务 Bean、数据库索引、支付/关单 CAS、延迟消息实现、持久化库存回补 Worker/Lua、商铺缓存或限流实现。保留此前“原订单在两次查询之间提交”的幂等分类修复。

## 测试调整与执行记录

修改 `OrderIntegrationSupport`、`OrderSubmissionIntegrationTest`、`OrderConsumptionIntegrationTest`、`SeckillTransactionIntegrationTest`（位于 `src/test/java/com/hmdp/order/`）。

- 删除的 14 个恢复任务用例对应已删除的发现、任务预算、租约与重投职责；消费侧并发重放、提交后重放、事务失败后原订单消费等业务正确性测试保留。
- 原 16 个事务协议用例改为 18 个标记协议用例：覆盖成功/不足/重复、TTL 与不续期、回查宽限、时间戳异常、响应丢失、Redis 不可用、旧 Hash 拒绝覆盖、非法库存与类型、并发回调、24 用户争抢 10 库存，以及标记过期后消费。
- 明确保留两个“暴露限制”的用例：Lua 运行错误不回滚已扣库存；回查 ROLLBACK 后迟到回调仍可能执行。这些用例验证故障边界，不代表消除了风险。
- 新增“标记缺失仍可首次消费，已落库重投无需 Redis”回归。提交测试验证 MQ 回调前没有预扣或事务登记、发送失败返回 202、按券找回原订单和查询身份边界。
- 原 MySQL 原子性、唯一键竞争、子进程事务中断、关单/支付竞争、回补失败/响应丢失、租约接管及永久幂等标记测试保留。

首轮 96 项订单测试中 1 项异常类型断言失败：旧归属冲突测试期望 IllegalStateException，新路径抛专用 SeckillUnavailableException；未产生订单或库存修改。更新这一精确异常契约后，在 clean 构建清除旧类文件的基础上重跑，96 项全部通过。没有通过删除该用例规避失败。

| 验证 | 当前已执行结果 | 本地日志（不提交 Git） |
| --- | --- | --- |
| 干净构建 | BUILD SUCCESS，打包阶段跳过测试 | `.local/seckill-simplified-build.log` |
| 订单测试 | 96 通过，0 失败/错误/跳过 | `.local/seckill-simplified-orders.log` |
| 用户限流与 HTTP 回归 | 16 通过，0 失败/错误/跳过 | `.local/seckill-simplified-rate.log` |
| 商铺缓存回归 | 24 通过，0 失败/错误/跳过 | `.local/seckill-simplified-cache.log` |
| 脚本语法与 diff 空白检查 | `bash -n scripts/dev.sh`、`git diff --check` 通过 | — |

构建产物检查确认不含 SeckillRecoveryWorker、SeckillRecoveryTask 或 SeckillMode 类，保留 V003 历史迁移，打包 Lua 与源文件一致。

订单测试使用真实 MySQL `hmdp_order_test`、真实 Redis DB 15 与隔离数据；没有测试外层事务掩盖提交边界。MQ 传输为 mock，直接调用真实事务监听器和消费业务方法。Redis 响应丢失通过执行成功后抛异常模拟，部分写入使用真实 Redis 注入 Lua 错误。新监听器对象不等于真实生产者进程重启。

未执行真实 Broker 故障注入、Broker 半消息落盘/回查上限/DLQ 转移验证、完整网络端到端演练或性能压测，没有 QPS、P95/P99 数据。

## 风险与部署边界

1. 60 秒宽限不是原子隔离；回调长时间暂停后恢复，仍可能在半消息已被回滚后预扣。标记过期或丢失也可能让已预扣事务被回滚。
2. Lua 只保证正常执行的原子可见性，不提供数据库式回滚。只扣库存而未写归属的部分失败甚至不会出现在 Set 缺单差集中，需库存差异和原消息/日志人工核查；不能盲目重执行预扣。
3. MQ 回查和消费重试都有边界。缺单自动恢复已移除，DLQ、缺单与库存差异告警必须有人处理；活动未结束时不承诺实时发现缺单。
4. 成功标记 TTL 控制的是事务记录增长。购买 Set、归属 Hash、回补标记与持久化回补任务仍需长期保留和后续归档策略；已结束券对账仍有历史数据规模成本。
5. Hash → String 不兼容混跑。切换前停止入口，在旧版本排空/核查旧事务消息和恢复任务；完成核账后停旧 Worker/消费者并统一切换。新代码不会覆盖旧 Hash，也不会自动处理旧恢复任务。已结束旧 Hash 不会自动获得 TTL，须在消息与订单核验后另行归档清理。
6. Redis 数据丢失、备份回滚、所有权缺失或永久回补标记丢失均不保证自动恢复。没有全局强一致性或系统总 QPS 保护承诺。

这次精简把主链路收敛为半消息、一次预扣脚本、短期成功证据与 MySQL 事务，保留正常并发与可重试故障所需的正确性机制；少见的跨系统证据丢失交给人工核查，更符合当前项目的实现和运维规模。
