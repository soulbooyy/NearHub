# 秒杀归属 Hash 成功凭据重构记录

当前版本（2026-11-04）删除独立事务成功标记，以永久归属 Hash 作为预扣成功证据。2026-10-31 和 2026-11-03 的实现与测试结果保留为历史记录，不代表本轮验证。

## 当前链路与技术取舍

登录鉴权 → ZSET + Lua 用户限流 → 生成订单 ID → RocketMQ 半消息 → 本地回调 Lua（全部预校验 → DECR 库存 → 最后 HSET 归属）→ COMMIT → Consumer → 独立 MySQL 事务条件扣库存与订单插入。

- `seckill:claim-owner:{voucherId}[userId] = originalOrderId` 不设 TTL，同时用于一人一单、回调幂等、Broker 回查、消费授权、原订单找回和回补归属校验。
- 回查精确匹配券/用户/原订单 ID 才 COMMIT；归属另一合法订单则 ROLLBACK；归属缺失时沿用 60 秒宽限，之后 ROLLBACK；Redis 故障及非法证据 UNKNOWN。
- 删除 `seckill:txn:*`、`hasSuccessMarker()`、标记 TTL 配置。预扣少一次 SET EX 和一个事务 Key；回查从 GET 改成 HGET + Hash TTL 查询，不据此宣称总体性能提升。
- 同订单重复回调返回成功且不再次扣库存；同用户不同订单仍被拒绝。取消后保留 Hash，旧消息依靠 MySQL 状态和唯一键幂等。
- 按券查到合法归属、未落库时返回 PROCESSING，不再受事务标记到期影响；仅订单 ID 的未落库查询仍 UNKNOWN，保持身份边界。PROCESSING 不保证最终自动建单。
- 保留独立 MySQL 事务、消费重试、支付/关单 CAS、延迟消息、持久化回补任务与永久回补幂等标记；对账仍只读并记录人工核查日志。

## 2026-11-04 修改及测试

修改预扣 Lua、SeckillReservationService、SeckillTransactionListener、VoucherOrderServiceImpl、RedisConstants、application.yaml、.env.example、五个订单测试文件与本文/README；不新增数据库表、状态机或调度框架。

旧协议测试按新契约调整：标记到期不再导致回查 ROLLBACK；只有合法归属也足以幂等成功；旧事务 Hash 不再影响预扣或单独授权提交；按券查询不再因为标记缺失返回 UNKNOWN。保留并发不超卖、消费重试、MySQL 回滚、权限和回补测试。

新增身份不匹配、其他订单归属、库存耗尽后的回调、错误类型/非法归属、回查宽限配置和取消后回调/消费重放用例。原“标记能发现归属丢失”测试替换成明确暴露新边界的测试：唯一归属丢失后，回调可能重复扣库存。该用例不代表修复了数据丢失。

| 验证 | 2026-11-04 结果 | 本地日志（不提交 Git） |
| --- | --- | --- |
| 订单链路 | 106 通过，0 失败/错误/跳过 | `.local/seckill-owner-evidence-orders.log` |
| 限流与 HTTP 匹配 | 16 通过，0 失败/错误/跳过 | `.local/seckill-owner-evidence-rate.log` |
| 干净打包 | BUILD SUCCESS，打包阶段跳过测试 | `.local/seckill-owner-evidence-build.log` |
| 静态及产物检查 | diff 空白检查通过；打包 Lua 与源文件一致，旧常量/标记读取方法不在产物中 | — |

测试使用真实本地 MySQL 隔离库和 Redis DB 15（订单）/14（限流）。MQ 传输仍为 mock，直接调用真实监听器；不等同于真实 Broker 故障注入。未开展性能压测。测试只清理自身数据，没有清理存量 Redis 业务数据。

## 2026-10-31 成功标记重构文件

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

2026-10-31 重构未改动独立持久化事务 Bean、数据库索引、支付/关单 CAS、延迟消息实现、持久化库存回补 Worker/Lua、商铺缓存或限流实现。保留此前“原订单在两次查询之间提交”的幂等分类修复。

## 2026-10-31 测试调整与执行记录

修改 `OrderIntegrationSupport`、`OrderSubmissionIntegrationTest`、`OrderConsumptionIntegrationTest`、`SeckillTransactionIntegrationTest`（位于 `src/test/java/com/hmdp/order/`）。

- 删除的 14 个恢复任务用例对应已删除的发现、任务预算、租约与重投职责；消费侧并发重放、提交后重放、事务失败后原订单消费等业务正确性测试保留。
- 原 16 个事务协议用例改为 18 个标记协议用例：覆盖成功/不足/重复、TTL 与不续期、回查宽限、时间戳异常、响应丢失、Redis 不可用、旧 Hash 拒绝覆盖、非法库存与类型、并发回调、24 用户争抢 10 库存，以及标记过期后消费。
- 明确保留两个“暴露限制”的用例：Lua 运行错误不回滚已扣库存；回查 ROLLBACK 后迟到回调仍可能执行。这些用例验证故障边界，不代表消除了风险。
- 新增“标记缺失仍可首次消费，已落库重投无需 Redis”回归。提交测试验证 MQ 回调前没有预扣或事务登记、发送失败返回 202、按券找回原订单和查询身份边界。
- 原 MySQL 原子性、唯一键竞争、子进程事务中断、关单/支付竞争、回补失败/响应丢失、租约接管及永久幂等标记测试保留。

首轮 96 项订单测试中 1 项异常类型断言失败：旧归属冲突测试期望 IllegalStateException，新路径抛专用 SeckillUnavailableException；未产生订单或库存修改。更新这一精确异常契约后，在 clean 构建清除旧类文件的基础上重跑，96 项全部通过。没有通过删除该用例规避失败。

| 验证 | 2026-10-31 结果 | 本地日志（不提交 Git） |
| --- | --- | --- |
| 干净构建 | BUILD SUCCESS，打包阶段跳过测试 | `.local/seckill-simplified-build.log` |
| 订单测试 | 96 通过，0 失败/错误/跳过 | `.local/seckill-simplified-orders.log` |
| 用户限流与 HTTP 回归 | 16 通过，0 失败/错误/跳过 | `.local/seckill-simplified-rate.log` |
| 商铺缓存回归 | 24 通过，0 失败/错误/跳过 | `.local/seckill-simplified-cache.log` |
| 脚本语法与 diff 空白检查 | `bash -n scripts/dev.sh`、`git diff --check` 通过 | — |

构建产物检查确认不含 SeckillRecoveryWorker、SeckillRecoveryTask 或 SeckillMode 类，保留 V003 历史迁移，打包 Lua 与源文件一致。

订单测试使用真实 MySQL `hmdp_order_test`、真实 Redis DB 15 与隔离数据；没有测试外层事务掩盖提交边界。MQ 传输为 mock，直接调用真实事务监听器和消费业务方法。Redis 响应丢失通过执行成功后抛异常模拟，部分写入使用真实 Redis 注入 Lua 错误。新监听器对象不等于真实生产者进程重启。

未执行真实 Broker 故障注入、Broker 半消息落盘/回查上限/DLQ 转移验证、完整网络端到端演练或性能压测，没有 QPS、P95/P99 数据。

## 2026-11-03 删除冗余购买 Set

- `seckill.lua` 删除 Set 的类型/TTL 校验、SISMEMBER、SADD 和双结构一致性判断；Hash 同时承担一人一单与原订单映射。原返回码和标记最后写入的顺序不变。
- `SeckillReservationService` 缩减预扣脚本参数；`findOriginalOrderId()` 只读取并校验 Hash，调用方契约不变。
- `return-order-stock.lua`、`RedisStockReturnService` 删除 Set 参数和校验，仍严格匹配原订单 ID、保留 Hash 并按永久回补标记幂等。
- `SeckillReconcileTask` 用 Hash 条目与数据库用户集合比对，缺单日志仍含券、用户、原订单 ID；无自动库存修复。
- 删除 `RedisConstants.SECKILL_ORDER_KEY`，更新四个订单测试文件及 README。订单服务、Controller、MQ 监听器、独立 MySQL 事务、补偿 Worker、表结构、限流及缓存代码未修改。
- 测试基类在正常预扣后断言未创建旧 Set。新增四项用例：遗留 Key 即使类型错误也不影响新链路；成功标记不能绕过缺失/冲突 Hash；过期 Hash 不能授权预扣或查询；同用户 12 个不同订单并发仅一笔预扣成功。
- 原双结构冲突用例改为非法 Hash 订单 ID 校验；旧 Set-only 消费用例保留，断言其不能代替 Hash 授权建单。原按券找回、标记到期后消费、归属冲突、支付/关单和重复回补回归保留；缺单测试新增日志断言，确认没有 Set 时仍输出原订单 ID。

| 验证 | 本轮结果 | 本地日志（不提交 Git） |
| --- | --- | --- |
| 订单链路 | 100 通过，0 失败/错误/跳过 | `.local/seckill-hash-only-orders.log` |
| 限流回归 | 16 通过，0 失败/错误/跳过 | `.local/seckill-hash-only-rate.log` |
| 打包 | `./scripts/dev.sh build` 成功（跳过测试，测试单独运行） | `.local/seckill-hash-only-build.log` |

本地存量清理检查扫描了 Compose 项目 Redis 的全部 16 个逻辑库，未发现 `seckill:order:*`，实际删除 0 个 Key，检查前后均为 3 个 Key。未清空数据库，也未修改库存、归属 Hash、成功标记或回补标记。检查报告保存在 `.local/seckill-hash-only-cleanup.json`，不提交 Git。

MQ 传输仍为 mock，测试使用真实本地 MySQL/Redis；未做真实 Broker 故障注入或性能压测。移除双结构一致性检测是去除重复存储的直接结果，原订单归属、消息身份与回补幂等校验仍保留。Lua 运行错误不自动回滚的边界不变。

## 当前风险与部署边界

1. 60 秒宽限不能隔离迟到回调；半消息已回滚后仍可能预扣。没有全局强一致性保证。
2. Lua 不提供数据库式回滚。DECR 后 HSET 失败可能只有库存变动；Hash 差集无法发现这笔订单，需库存、日志与 MQ 证据核查。
3. Hash 是唯一预扣证据。丢失、误删、覆盖或备份回退后，回调可能再次扣库存；外部不能伪造归属来代替预扣。取消后不能删除 Hash 字段或给它设置 TTL。
4. MQ 有限重试、DLQ 和缺单需要人工处理；没有恢复自动重投。归属长期存在不保证消息一定送达。
5. 归属 Hash、永久回补标记和补偿任务仍需容量监控与未来归档策略；不可随意过期或清理。
6. 不支持新旧版本混跑：旧版按成功标记回查，新请求不再创建该标记。停止入口、处理在途消息和回调并核账后统一切换。旧事务 String 可自然到期，旧四状态 Hash/恢复任务需另行核验；本轮不删除历史业务数据或历史迁移。
7. 归属存在但 MySQL 尚无订单时查询 PROCESSING，只表示预扣证据存在；故障下可能长期停留，需要对账及人工核查。订单 ID 单独查询仍无足够信息确认未落库归属。
