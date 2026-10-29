# 秒杀链路分阶段重构记录

## 阶段一：主链路收敛

- 修改订单服务、MQ 事务监听器/生产者、订单实体、接口、Redis 常量与配置；新增 `SeckillReservationService`，重写 `seckill.lua` 的 PREPARE/RESERVE/CHECK 协议。
- 删除 `SeckillMode`、B 模式普通 CREATE 发送和消费者预占、订单创建 Redisson 锁、演示开关及旧注释代码；删除 `seckill-claim.lua`、`seckill-stock-only.lua`。保留 Set/Hash 归属结构及缓存/对账锁。
- 复用独立 MySQL 事务、库存条件扣减、主键与联合唯一索引。竞争异常离开事务代理回滚后重新查询；不以手动加库存抵消失败事务。
- `./scripts/dev.sh test-orders`：70 项通过，0 失败/错误/跳过。A/B 双参数提交用例改为单链路，因此比原测试集少 2 个参数实例；原有数据库回滚与回补测试保留。新增消费入口无订单锁并发重投断言。
- 测试使用真实 MySQL/Redis，MQ 发送/消费传输为 mock；未验证真实 Broker 投递、回查或死信行为。此阶段尚未完成统一查询、未知结果 HTTP 契约和独立缺单恢复。

## 阶段二：可靠性完善

- 修改事务监听器、预占服务/Lua、订单服务/控制器/接口与异常处理器；新增 `SeckillPendingException`、`SeckillUnavailableException` 和事务集成测试。
- 业务状态只保留 PREPARED / RESERVED / REJECTED / ABORTED；writePending、review 是独立诊断保护。Lua 错误不回滚已写入内容，未决记录禁止再次扣减或自动终止。
- PREPARED 超时与迟到回调由同一 Lua 原子竞争；事务/归属不设自动 TTL，避免回查和恢复证据自然消失。回查优先确认已落库的同身份订单，证据缺失或 Redis 异常返回 UNKNOWN。
- 增加 HTTP 202 不确定结果及原订单 ID，订单 ID / 当前用户+券两种查询；查询 Redis 不可用返回 503，不伪装成不存在。支付补充当前用户校验；保留原支付/关单 CAS 和持久化回补。
- `test-orders`：90 项通过；`test-rate-limit`：16 项通过；均为 0 失败/错误/跳过。覆盖真实 Redis Lua 部分写入注入、预占响应丢失、新监听器回查、超时竞争、24 个用户争抢 10 库存、查询身份与 HTTP 契约。
- 回查测试调用监听器，未使用真实 Broker；新监听器实例不等同于实际生产者进程重启。尚未验证 Broker 恢复、真实网络端到端、性能或 Redis 数据丢失后的自动修复。

## 阶段三：最小化缺单恢复

- 新增 `entity/SeckillRecoveryTask.java`、`mapper/SeckillRecoveryTaskMapper.java`、`task/SeckillRecoveryWorker.java`、`db/migrations/V003__seckill_recovery_task.sql` 和 `order/SeckillRecoveryIntegrationTest.java`。Java 路径相对于 `src/main/java/com/hmdp`；测试相对于 `src/test/java/com/hmdp`。
- 修改 `VoucherOrderPersistenceService`，将两次查库之间刚提交的同 ID 订单正确分类为幂等成功，避免误入人工核查。
- 修改 `SeckillReconcileTask` 接入发现逻辑；订单查询纳入恢复任务人工核查状态。更新 `application.yaml`、`scripts/dev.sh` 与 `compose.yaml` 的任务配置/增量建表；清理已无生产调用的旧订单服务预占转发方法及普通生产者组常量。
- 复用回补任务的专用表、CAS 租约、令牌隔离、持久化次数与指数退避模式，没有抽象通用调度框架。发现只登记经过等待窗口、身份与归属核验的 RESERVED 缺单，重复发现不重置预算。
- 恢复重投使用原订单 ID 和事务消息，回调只复用原 RESERVED 决策；MQ 发送成功不标记 DONE，必须确认 MySQL 订单。最终发送后仍有落库确认机会，超限缺单进入 MANUAL_REVIEW。保留现有 Set/Hash、对账锁、缓存重建锁和幂等回补。
- 最终 `test-orders`：107 项通过，0 失败/错误/跳过。其中恢复集成测试 14 项，新增 Lua 数据校验用例 2 项，新增“两次查库之间原订单提交”的幂等分类回归 1 项。覆盖原消息/恢复消息并发、提交响应丢失、最后一次重投落库确认、有限预算、退避、租约抢占、旧令牌隔离、Redis 不可用、证据缺失和 MySQL 库存异常。
- `test-cache`：24 项通过，0 失败/错误/跳过。限流回归在阶段二执行：16 项通过，覆盖新增按券查询不触发限流。各阶段订单测试数为累积测试集，不应相加当作独立用例总数。

## 验证记录与边界

| 验证 | 结果 | 本地日志（未提交 Git） |
| --- | --- | --- |
| 阶段一订单回归 | 70 通过 | `.local/seckill-stage1-tests.log` |
| 阶段二订单回归 | 90 通过 | `.local/seckill-stage2-tests.log` |
| 用户滑动窗口与 HTTP 回归 | 16 通过 | `.local/seckill-stage2-rate-tests.log` |
| 阶段三最终订单回归 | 107 通过 | `.local/seckill-stage3-tests.log` |
| 商铺缓存回归 | 24 通过 | `.local/seckill-cache-regression.log` |
| 干净打包 | BUILD SUCCESS（打包阶段跳过重复测试） | `.local/seckill-build.log` |

测试使用真实本地 Redis 与 MySQL；MQ 传输用 mock，直接调用真实事务监听器/业务消费方法。Redis 故障包含注入基础设施异常、真实 Lua 执行部分写入后错误以及执行成功后模拟响应丢失；没有停止真实 Broker 或做实际网络分区。MySQL 子进程退出用例验证数据库事务边界，不代表完整生产者重启恢复。

未验证真实 Broker 半消息持久化/回查上限/死信转移、跨进程端到端灾难恢复、Redis 全量丢失或备份回滚后的自动恢复，也未执行任何性能压测，没有 QPS/P95/P99 数据。已测用例的库存和订单断言不等于所有故障下的无损承诺。

永久证据、任务和幂等标记需要容量监控，未来清理须配套禁止旧消息重放的归档协议。每轮 SCAN 仍遍历全部事务记录；其历史数据规模下的性能未验证。对账遇到缺单、待回补或库存差异只依据业务证据处理，不按差额覆盖库存。不提供全局入口流量保护。

本轮迁移在独立测试库验证。现有环境部署前执行 `./scripts/dev.sh migrate`，仅增加两张任务表；不要重导含删除/重建表操作的课程 SQL。旧普通消息、旧事务 String 标记和新 Hash 协议不能混跑，升级前须排空/核查旧消息并统一切换。具体链路、查询契约、前后架构图见 [README](../README.md)。

最终 JAR 已检查：包含新预占脚本、恢复 Worker 和 V003 迁移，不含旧模式类、消费者预占脚本、OpenResty 或令牌桶资源。`bash -n scripts/dev.sh` 与 `git diff --check` 通过。
