# 78 个 HTTP 202：生产端拒绝专项分析

追溯日期：2026-11-07。本报告仅涉及 `formal-create-only-100-20261104-2155` 的 78 个 pending 请求。

**结论：78 次请求均到达 Broker 的内存发送处理队列，随后因排队超时在消息写入前被拒绝，没有成功写入事务半消息或业务消息，也没有执行 Redis 本地预扣。原“未决”来自应用和自动对账的保守分类，并不表示消息仍在等待消费。**

## 精确关联与存储证据

| 证据 | 结果 |
|---|---:|
| JTL 202、未决清单和发送异常的原始订单 ID 集合 | 完全相同的 78 个 |
| 根异常为 MQBrokerException CODE 2 / TIMEOUT_CLEAN_QUEUE | 78 / 78 |
| 对应 MySQL 订单 / Redis 归属 | 0 / 0 |
| 封存 CommitLog 中匹配的消息 | 0 |
| 所有已落库正式订单在半消息/业务消息中的匹配 | 各 147231 / 147231 |

异常时间为 21:55:19.164–21:55:49.298，记录的排队时间为 200–393 ms。示例异常已去掉订单/用户标识：

```text
MQBrokerException: CODE: 2
[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while
period in queue: 211ms, size of queue: 63
```

[pending-78.json](pending-78.json) 按 `sample_index` 关联公开的全部请求样本，包含每笔 HTTP 耗时、异常时间、Broker 排队时间与原始根异常文本，不公开原始身份。

对已停止的旧 Broker 只读复制存储，未启动旧应用、中间件或消费流程。使用精确依赖版本 RocketMQ 4.9.7 MessageDecoder 和 UtilAll.crc32，完整扫描 `[0,126364614)`，与当时 Broker 最大位点一致。441708 条有效记录全部通过消息体 CRC 校验，未使用的预分配尾部全零：

* 事务半消息：147236。
* 业务 Topic 消息：147236。
* 事务操作记录：147236。

每组的 147236 = 本轮正式落库 147231 + 冒烟 5。全部正式落库订单在半消息和业务消息中均可找到，而 78 个 pending 均未出现；这是对扫描器识别能力的正向交叉验证。[CommitLog 审计摘要](commitlog-audit.json)

原始 CommitLog、用户/订单身份及完整日志保留本地；本目录公开关联后的脱敏证据和审计摘要，不宣称能仅凭这些摘要独立重做原始二进制审计。

## 具体失败路径

1. BrokerFastFailure 将超时且尚未执行的请求从发送线程池队列移除、禁止执行，回复 SYSTEM_BUSY（CODE 2），此时尚未进行消息存储。[BrokerFastFailure 4.9.7](https://github.com/apache/rocketmq/blob/rocketmq-all-4.9.7/broker/src/main/java/org/apache/rocketmq/broker/latency/BrokerFastFailure.java#L86)
2. 客户端虽然配置 `retryTimesWhenSendFailed=2`，但默认可重试错误码集合不包含 SYSTEM_BUSY。该配置不是所有错误一律重试两次。SDK 的 `resend at once` 日志发生在响应码判断之前，不能据此认定实际重试。[默认错误码集合](https://github.com/apache/rocketmq/blob/rocketmq-all-4.9.7/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java#L68)；[响应码判断](https://github.com/apache/rocketmq/blob/rocketmq-all-4.9.7/client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java#L609)
3. sendMessageInTransaction 先发送半消息，抛错就退出；只有 SEND_OK 才调用本地事务。项目 Lua 预扣在本地事务回调中，因此这批被拒绝请求没有扣库存或后续 COMMIT。[事务发送实现](https://github.com/apache/rocketmq/blob/rocketmq-all-4.9.7/client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java#L1237)
4. 应用将发送 Exception 统一转为 SeckillPendingException，WebExceptionAdvice 统一返回 202 + UNKNOWN；没有存储明确拒绝的查询终态。
5. benchmark 的对账只依据 MySQL/归属匹配来确认不确定请求，不分析日志根因；没有记录就保留 UNRESOLVED。这一保守行为避免把真正可能已接收的网络超时误判为失败，但也不能区分此次明确拒绝。

没有成功存储的半消息便没有回查对象，也没有 CREATE 可消费；本轮不对这些身份重提请求，没有持久化发送失败重试任务，所以继续等待不会自动生成订单。尚未进入消费阶段，也就不会因为消费失败进入 retry/DLQ。

## 风险与结果口径

* 未发现已接受/已预扣订单丢失：147131 个 accepted 与 100 个读取超时后查回的订单全部落库，库存一致。
* 这 78 次请求没有生成订单。用户不重试且服务没有可靠重提机制时，不会自动恢复；统一返回 202 会带来长期 UNKNOWN 或用户误以为仍在处理的风险。
* 不能据此保证故障场景绝不丢消息。本轮没有验证 Broker 崩溃、磁盘故障或 Redis 证据丢失的恢复，Broker 为 ASYNC_MASTER + ASYNC_FLUSH。
* 后续诊断分类为 `BROKER_REJECTED_BEFORE_PERSISTENCE`。原 HTTP 202、历史 UNRESOLVED/passed=false 和性能数值保留，不回填为成功，不把持续积压改写成稳定容量。

可优先区分“可证明写入前拒绝”与“真实发送不确定”，为前者提供明确可重试状态，为后者保留 202；若重试，应有界退避、保持业务身份并核验幂等。不能将所有 MQ 异常一律映射为明确失败，也不应只提高即时重试次数来掩盖 Broker 排队。本次仅诊断和发布结果，未修改业务逻辑。
