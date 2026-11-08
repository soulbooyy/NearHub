# NearHub：4 vCPU / 8 GiB，100 线程 CREATE-only 压测

本目录只发布 `formal-create-only-100-20261104-2155` 这一轮的方案、结果与后续追溯。

**稳定窗口有效预扣 QPS 1687.10，HTTP P95/P99 为 93/145 ms。147131 个明确成功响应全部落库，100 个读取超时也找回订单；78 个 HTTP 202 在原自动对账中保持未决，后续日志及封存 CommitLog 追溯确认它们均被 Broker 在消息写入前拒绝。发压期间积压持续增长，本轮不能作为最大稳定容量或完整业务链路成绩。**

## 环境与测量边界

| 项目 | 实际配置 |
|---|---|
| 宿主机 | Apple M5、10 核、32 GiB |
| Docker VM | 4 vCPU、配置 8 GiB；Engine 可用 8322555904 bytes，约 7.75 GiB |
| 部署 | MySQL/Redis/RocketMQ 在 Docker；Spring Boot 和 JMeter 在宿主机 |
| 应用 | 单实例 Spring Boot 2.3.12，Zulu aarch64 JDK 8u504 |
| 应用堆 | 未显式调参；InitialHeapSize=512 MiB、MaxHeapSize=8 GiB、Parallel GC |
| JMeter | 5.6.3，Java 17.0.20.1，非 GUI，堆 256 MiB–1 GiB |
| MySQL / Redis | 8.0.45 / 7.2.16 |
| RocketMQ | 4.9.7，单 Broker，amd64 仿真；Broker 堆 256 MiB，NameServer 堆 128 MiB |
| MQ 持久化配置 | ASYNC_MASTER + ASYNC_FLUSH，4 个业务队列 |
| 业务参数 | 不修改连接池、线程池、索引、限流、一人一单或消息一致性协议 |

4 vCPU / 8 GiB 是中间件 VM 配额，不是整台施压机和应用的总资源。宿主同时运行其他开发容器；存在同机竞争、仿真和监测开销，不能直接外推至独立原生 Linux 服务器。环境及镜像指纹见 [environment.json](environment.json)。

压测时 Git HEAD 为 `90c833307378840d8e03d6f62bac292d5def295c`，实际包含当时工作区的业务改动。发布前已确认提交 `81f8075297b5ade41a42b862d8fb2d0af753d6c8` 的 `pom.xml + src/main` 内容与实际运行源码指纹一致：`3db38c67767e1dfbd628ad18f2bda7076314457d9d434ed448b989f5762527e3`。不能仅 checkout 当时旧 HEAD 便声称复现同一业务代码。

## 完整压测方案

接口：`POST /voucher-order/seckill/{voucherId}`。

```text
真实 Token 鉴权 / 用户限流
→ RocketMQ 事务半消息
→ 本地事务 Redis Lua：校验库存及一人一单、预扣、写入归属
→ COMMIT
→ Consumer 核验 Redis 归属
→ MySQL 事务扣库存并创建订单
```

* 使用全新专用 MySQL、Redis、NameServer、Broker 和环境 UUID；旧环境停止并封存，保留历史消息和数据。
* 通过独立外部启动 JAR 拦截 `sendOrderTimeout` 和 `cancelTimeoutOrder`，正常业务源码和普通启动入口不变。真实 CREATE 链路保留。本轮是 **CREATE-only**，不等待 15 分钟关单，不将此数字当作含关单成本的完整链路成绩。
* 先以 5 个真实新用户完成冒烟，全部成功落库，延迟队列为空。
* 正式轮次使用新券 **14**、**500000 新用户**和 **500000 库存**。MySQL 用户真实存在，Redis 登录 Hash 遵循实际字段和 TTL，没有跳过鉴权。
* CSV 为所有线程共享游标，逐请求更换用户，`recycle=false`、`stopThread=true`；未耗尽身份和库存。修正版 JMX 的 `same_user_on_next_iteration=true` 保留 TCP 连接，不影响 CSV 换身份。
* **100 线程，30 秒 Ramp-up，60 秒稳定运行，共 90 秒。**不设目标 RPS，是闭环并发测试；100 线程不代表固定 QPS，慢响应会降低到达率。
* 每 5 秒采样 MySQL 已提交订单计数、Redis 库存/归属和资源；MQ/JVM/Docker 较重采集间隔参数为 30 秒，实际间距还包含命令执行耗时。
* 停止发压后默认观察 180 秒，逐笔核对订单、用户、券、归属、库存及不确定 HTTP 结果。该窗口不足时保留原结果，再延长 CREATE 观测，连续 3 次确认所有已知预扣落库。

[复现步骤](REPRODUCE.md) · [完整工具操作手册](../../README.md) · [JMX](../../jmeter/nearhub-seckill.jmx)

## HTTP 与 MySQL 性能结果

2026-11-04（Asia/Hong_Kong）21:54:41–21:56:11 发压。稳定窗口为 **21:55:11.872–21:56:11.872**，完整覆盖 60 秒。

| 稳定窗口指标 | 实测值 |
|---|---:|
| 尝试数（含传输失败） | 101404 |
| HTTP 响应数 | 101304 |
| 明确预扣成功数 | 101226 |
| HTTP 响应 QPS | **1688.40** |
| 明确预扣成功 QPS | **1687.10** |
| 尝试 QPS | 1690.07 |
| HTTP 平均响应时间 | **49.14 ms** |
| HTTP P95 / P99 | **93 / 145 ms** |
| MySQL 观测落库 TPS | **136.70**（有效采样区间 51.61 秒） |
| 传输等异常率 | **0.0986%** |
| 非 accepted 比例（含 202） | **0.1755%** |

稳定窗口另有 78 个 HTTP 202 和 100 个读取超时。全程 147309 次尝试：147131 accepted、78 pending、100 transport_error；无库存不足、重复购买、429、5xx 或连接建立前失败。

HTTP QPS 只计算收到 HTTP 响应的请求；有效预扣 QPS 只计算 200 + success=true + 正确订单 ID，不用事后落库结果回填。P95/P99 为 nearest-rank，基于 HTTP 响应样本；100 个约 10 秒的读取超时单列，不能因 P99=145 ms 忽略。包含传输失败的全部稳定样本 P95/P99 为 93/150 ms。全程和稳定窗口指标分开保存在 [summary.json](summary.json)。

MySQL TPS 来自实际采样间已提交订单数量的差分；`create_time` 秒桶是辅助审计，不能作为精确提交 TPS。

## 最终落库与积压

| 项目 | 最终结果 |
|---|---:|
| accepted 精确对应落库 | **147131 / 147131** |
| 读取超时事后找到对应订单 | **100 / 100** |
| MySQL 订单 / Redis 归属 | **147231 / 147231** |
| MySQL / Redis 剩余库存 | **352769 / 352769** |
| 重复 / 超卖 / 归属不匹配 / 已取消 | **均为 0** |
| 原自动对账未决的 HTTP 202 | **78** |
| 最终已投递 MQ 积压 / retry / DLQ | **0 / 0 / 0** |

库存满足 `500000 - 147231 = 352769`。原定 180 秒窗口末次业务快照仅落库 **73437** 笔；未落库归属采样峰值 **132606**。扩展观测后，距最后 HTTP 完成 **417.8 秒**首次逐笔确认已知预扣全部落库，连续 3 次已知 CREATE 一致，最终快照在 **453.9 秒**。这是采样发现完成的上界，不是精确提交时刻。

历史自动结果保留 `KNOWN_CREATES_PERSISTED_WITH_UNRESOLVED_HTTP`、`passed=false`。后续追溯没有把 202 回填为成功、没有改变 QPS，也没有将负载阶段的持续积压解释为稳定容量。详见 [verification.json](verification.json)、[落库采样](metrics.jsonl) 和 [扩展观测](catchup-metrics.jsonl)。

关单发送拦截计数为 147236（正式 147231 + 冒烟 5），取消拦截调用为 0；延迟 Topic 18 个队列 min/max 位点均为 0。只等待 CREATE 落库。

## 78 个 202 的后续追溯

2026-11-07 逐笔匹配后确认：78 条发送异常均为 `MQBrokerException CODE 2 / TIMEOUT_CLEAN_QUEUE`，Broker 排队 200–393 ms 后在写入前拒绝。对封存 Broker 进行完整离线解码和 CRC 校验，441708 条记录、物理区间 `[0,126364614)` 与结束位点一致；78 个订单均无半消息或业务消息。全部 147231 个落库订单则在两类消息中均有记录。

业务统一将发送异常映射为 202/UNKNOWN，对账仅凭订单/归属确认，故历史结果保持未决。4.9.7 默认重试响应码不包含 SYSTEM_BUSY，该错误不受“重试 2 次”的配置兜底。这 78 笔未进入预扣，未发现“已预扣后丢单”，但用户请求确实没有生成订单，继续等待不会自动成功。[专项分析与风险边界](forensics.md)

## 诊断结论与限制

* HTTP 接收远快于 MySQL 落库，积压持续上升；本轮验证的是短时接收吞吐和事后落库情况，没有证明无持续积压的最大吞吐。
* 稳定采样区间 MySQL 行锁等待增加 7056 次、累计等待增加 387524 ms。单券库存热点和事务提交是排查方向；未采 Hikari pending 或独立 IO 实验，不能直接归因某个池大小。
* 100 个读取超时集中始于 21:55:37，同期监测出现一次 socket 超时，Broker 出现连接移除/重注册。未证明这次链路抖动的具体根因。
* JMeter 采样 CPU 峰值 95.1%（单核单位）、RSS 347.86 MiB；Broker/MySQL/Redis 采样 CPU 峰值分别为 113.34%/40.43%/22.86%。低频采样不等于瞬时峰值；修正版未出现 BindException 或连接建立超时。
* 只有一个并发档位的一次正式测量，关闭了延迟关单；没有故障注入或生产容灾验证。Broker 为 ASYNC_MASTER + ASYNC_FLUSH，不能声称任何故障下都不丢消息。

## 公开证据与离线复核

```bash
python3 benchmark/reports/4c8g-20261104/verify.py
```

该命令不访问网络、数据库或 MQ；从全部 **147309 行脱敏请求样本**重算全程/稳定窗口 QPS、平均/P95/P99、分类、异常率及采样 MySQL TPS，并检查公开摘要的一致性及文件 SHA256。

| 文件 | 内容 |
|---|---|
| `samples.csv.gz` | 原 JTL 全部请求，保持顺序与原时间/耗时/分类；移除 Token、用户、订单和 URL 字段，新增 sample_index 作为匿名关联序号 |
| `summary.json`、`summary.csv`、`parameters.json` | 原实测统计、配置及历史判定 |
| `scenario.json`、`environment.json` | 数据量、观测范围、环境和代码/镜像指纹，移除个人路径、进程 ID 和其他项目名称 |
| `metrics.jsonl`、`catchup-metrics.jsonl`、`mysql-observed-tps.json`、`mysql-create-time.csv` | 本轮落库/积压/库存和资源采样 |
| `verification.json`、`mq-series.json` | 原核验的聚合结果及 MQ 采样，不含用户/订单清单 |
| `pending-78.json`、`commitlog-audit.json` | 按 sample_index 关联的 78 条脱敏异常证据、离线 CommitLog 审计摘要 |
| `create-only-final.json`、`scheduled-queues.json` | 关单拦截计数与空延迟队列 |
| `provenance.json`、`SHA256SUMS` | 原始输入校验值、公开转换方式、公开文件完整性校验 |

原始 Token、口令、JTL/HTML、完整身份对账 CSV、运行目录和 CommitLog 保留本地。公开样本足以重算性能数值，但不包含重新执行逐身份对账或完整 CommitLog 扫描所需的私有原始数据；此部分发布的是已完成审计的摘要，不能伪称仅靠本目录能独立重做全部业务取证。
