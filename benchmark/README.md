# NearHub JMeter 压测准备与执行

本目录独立于业务实现。默认使用真实 MySQL、Redis、RocketMQ 和宿主机 Spring Boot，**不会绕过鉴权、限流或一人一单，不修改任何业务池大小、数据库索引、消息协议或订单状态机**。首次使用先完成小规模冒烟；100～1000 线程是并发档位，不是 QPS。本次公开的 [4 vCPU / 8 GiB 完整方案与结果](reports/4c8g-20261104/README.md) 包含脱敏样本和离线复核命令；验收记录见 [VALIDATION.md](VALIDATION.md)。

## 1. 检查到的实际实现

检查基线：远端 `soulbooyy/NearHub` HEAD 与本地 HEAD 均为 `90c833307378840d8e03d6f62bac292d5def295c`。工作区另有未提交的归属协议改动；工具按**工作区实际构建代码**运行。每轮保存 SHA、dirty 状态、源码 SHA256、实际 JAR SHA256，不能仅凭 Git SHA 复现实验。本次发布已核实业务提交 `81f8075297b5ade41a42b862d8fb2d0af753d6c8` 的源码指纹与该轮一致，详见公开报告。

| 项目 | 当前实现与压测含义 |
|---|---|
| Spring Boot / Java | Boot 2.3.12.RELEASE，pom Java 8；旧 Lombok/项目脚本要求 JDK 8 构建运行。宿主默认 Java 17 不等于应用 Java。工具分别记录。 |
| Tomcat | 未显式配置；本地 Boot metadata 默认 max threads 200、min spare 10、max connections 8192、accept count 100；外部配置可能覆盖，不当成动态利用率。 |
| HikariCP | 未显式配置，依赖默认最大池 10；工具不提高池容量。采集 MySQL 全局连接数无法直接代表 Hikari 等待线程。 |
| Redis | Lettuce max-active=10、max-idle=10、min-idle=1；Redisson 独立客户端、独立默认池。 |
| 原 Compose | MySQL 8.0.45、Redis 7.2、RocketMQ 4.9.7；无容器 CPU/内存硬限额，受 Docker VM 总额度约束。 |
| RocketMQ | 单 Broker，ASYNC_MASTER + ASYNC_FLUSH，4 个业务队列；NameServer 128 MiB heap、Broker 256 MiB heap；未修改可靠性模式。 |
| Producer | send timeout 3000 ms、失败重试配置 2；事务回查线程 2～5、队列 200。 |
| Consumer | 单应用中订单组 `order-consumer-group`，CREATE/TIMEOUT 同 Topic，batch=1、maxReconsumeTimes=3；客户端消费线程未显式配置。另有 Canal 消费者。 |
| 鉴权 | `authorization` 直接携带 Token，**没有 Bearer 前缀**；Redis `login:token:{token}` 为 Hash，字段 `id,nickName,icon`。当前 TTL=36000 **分钟**，请求续期。准备工具从 Java 常量提取 TTL。 |
| 一人一单 | `seckill:claim-owner:{voucherId}`：`userId -> originalOrderId`，永久 Hash，取消后也不删除。MySQL `(user_id,voucher_id)` 唯一索引。 |
| 库存 | `seckill:stock:{voucherId}` 永久 String；MySQL `tb_seckill_voucher.stock / initial_stock` 与 Redis 一同初始化。 |
| 当前预扣证据 | 工作区**没有独立事务成功标记**；Lua `DECR -> HSET -> return 0`，HSET 是最后一次写入。禁止准备工具人工创建归属。 |
| 回补证据 | `seckill:stock-returned:{orderId}` 永久 String，内容 `voucherId:userId:orderId`；`tb_order_stock_compensation` 持久化任务。 |
| 异步查询 | 按订单 ID 查询无记录为 UNKNOWN；按券查询有归属为 PROCESSING，无归属为 NOT_FOUND。缺失证据不能证明未发生预扣。 |
| 15 分钟关单 | DB create_time + 15 min；延迟 TIMEOUT 可能提前送达后重调度，分钟级任务兜底。每 5 秒扫描最多 100 条回补任务，有限退避/重试，可能 MANUAL_REVIEW。 |
| 其他定时任务 | 活动结束 2 分钟后读 Hash/DB 告警，不自动恢复缺单；V003 历史任务表保留但新代码不执行其任务。 |
| 已有测试 | 有真实 DB/Redis 集成测试，但订单测试的 MQ 为 mock；原仓库没有 JMeter/完整监控栈。 |

影响真实性的现状（本工具不擅自修业务）：

* 提交路径没有校验券的起止时间/上下架状态。准备正确时间范围，preflight 校验；不能把该接口性能表述成已测完整活动校验。最小后续方案应单独评估提交前校验，另做基线。
* HTTP 200 可返回 `success:false`，通用 RuntimeException 也可能返回 200 的“服务器异常”；不按状态码计算有效 QPS。
* Redis Lua 部分写入不自动回滚，Hash 缺失不能证明库存未扣；同时核对库存公式，缺单/202/丢响应单独列出。
* MQ 有限重试、永久归属和补偿证据有运维保留要求；没有全局强一致和无限重放承诺。
* 原 `RedissonConfig` 没有 Redis database 配置，因此仅用逻辑 DB 隔离不充分；本方案使用独立 Redis **实例**，DB 0。
* Topic/group 为业务常量，因此隔离整个 Broker/NameServer，避免测试消费者与开发消费者互相分流。

## 2. 初次安装与隔离启动

在仓库根目录执行，需要 Docker Desktop、Python 3.9+、Maven、curl、tar、JDK 8（应用）及 Java 8+（JMeter）。

```bash
./benchmark/scripts/bootstrap.sh
# 必要时编辑 benchmark/.env，尤其 APP_JAVA_HOME
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
./benchmark/scripts/env.sh check
```

`bootstrap.sh` 仅安装到 `benchmark/.runtime/`：PyMySQL 1.1.1、redis-py 5.2.1（批量写入/真实对账所需）及 Apache JMeter 5.6.3（标准组件，无插件）。从 Apache 下载并核对 SHA512。不修改系统 Python/Java，不安装 Prometheus/Grafana。首次生成随机数据库口令；`.env` 采用简单 `KEY=value`，不是 Shell 文件，不使用引号/变量展开。

`env.sh start` 重新构建现有应用，保存 JAR 副本后后台启动，日志在 `.runtime/build.log`、`.runtime/app.log`。它不执行已有集成测试，也不修改业务参数；测试命令见第 9 节。`APP_JAVA_OPTS` 留空会采用 JDK 默认堆策略，实际参数由 `jcmd VM.flags` 记录。构建代码变化后先 `env.sh stop` 再 `env.sh start`。

| 配置 | 默认 |
|---|---|
| Compose 项目 | 默认 `nearhub-benchmark`；`fresh` 创建带时间戳的独立项目，不复用开发项目 |
| MySQL DB / user / host port | `nearhub_benchmark` / `benchmark` / 33306 |
| Redis host port | 36379 |
| NameServer host port | 39876 |
| Broker advertised/listen port | **30911**；配套 30909、30912，必须与宿主映射相同 |
| Spring Boot | 18081，单实例；中间件只发布到 loopback |
| APP_JAVA_HOME | 优先配置，否则发现仓库 `.local/jdk8/Contents/Home` 或 `.local/jdk8` |
| JMETER_JAVA_HOME | 留空使用当前环境 Java；可配置 Java 17 |
| JMETER_HEAP | `-Xms256m -Xmx1g`，只影响压测客户端 |
| SAMPLE_SECONDS / MQ_SAMPLE_SECONDS | 5 / 30；诊断采集本身也占资源 |

原 SQL 仅由**新建专用 MySQL 卷**首次导入，不会对开发库重新导入 DROP/CREATE。项目名称、容器 label、端口绑定、MySQL/Redis 环境 UUID 都要匹配才允许准备/运行/清理。新环境 UUID 不匹配已有卷时直接拒绝，不能用删除标记绕过。

## 3. 小规模真实冒烟

```bash
./benchmark/scripts/prepare.sh --run-id smoke-01 --users 12 --stock 12
./benchmark/scripts/run.sh --run-id smoke-01 --threads 2 --rampup 1 --duration 10 --target-rps 2 --smoke
./benchmark/scripts/verify.sh --run-id smoke-01 --wait 180
```

`--smoke` 限制最多 5 线程、100 身份、30 秒；全部身份用尽可以提前退出，但会标记无完整稳定窗口，**不能当成容量数据**。run 自动执行 preflight、非 GUI JMeter、采集、等待 CREATE 收敛、生成 HTML、JSON/CSV/Markdown。

准备流程：为每轮事务创建真实券及秒杀表记录；生成不同 MySQL 用户和 UUID Token；按真实登录 Hash 格式批量写入 Redis；永久 SET NX 初始化库存；落盘 manifest/CSV。不发送短信，不 Mock，不跳过 `RefreshTokenInterceptor`。CSV 格式为 `token,userId`，后者用于丢响应时按身份对账。JTL 不记录 Token。

同一 run-id 在未使用阶段重复 prepare 会复用已有身份与券，检查参数、库存和无订单/无归属后续做；不覆盖已消耗库存。开始 run 即锁定该轮，失败也不能复用身份，需新 run-id。100000 身份只是示例：不足以覆盖窗口会自动标记样本无效。

## 4. 正式单轮、多档与目标 RPS

本任务按最新约定只观测到 MySQL 成功创建订单。单轮在 CREATE 校验通过后结束，多档立即使用新券、新身份继续，不等待 15 分钟关单。默认模式保留后台业务任务；如需排除延迟关单，显式使用下面的 CREATE-only 模式。

### 可选：独立 CREATE-only 模式

```bash
./benchmark/scripts/cleanup.sh
./benchmark/scripts/env.sh fresh --create-only
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
./benchmark/scripts/env.sh check
```

`fresh` 要求当前压测应用和容器均已停止。它归档旧环境标识和配置，保留旧数据卷，切换到全新 MySQL、Redis 和 RocketMQ 存储；因此本轮不会消费历史延迟消息。它不是删除旧 Broker 消息，也不代表旧消息已消费。新的用户序号和券 ID 从本地历史 manifest 之后分配，轮次名称也必须更新。

`overlay/nearhub/benchmark/CreateOnlyBenchmark.java` 是独立启动入口，编译为外部 JAR，仅在 `BENCH_CREATE_ONLY=true` 时加载。它拦截 `sendOrderTimeout` 和 `cancelTimeoutOrder`，分别阻止新关单消息发送和 MQ/定时任务触发的超时取消；CREATE 事务消息、鉴权、限流、Redis 预扣、MySQL 事务均执行真实代码。没有修改 `src/main`、`pom.xml` 或正常应用 JAR。边界测试验证其他调用继续委托原实现，运行时 JMX 保存拦截计数。

此模式的报告明确标记 `create_only=true`，只衡量关闭关单后的 CREATE 链路，不能直接作为完整业务链路性能或据此计算优化提升。MQ 自身的消费重试延迟仍然保留。专用拦截需要本地压测库/NameServer 校验通过，默认正常入口不加载它。

恢复完整链路时先 `cleanup.sh`，再执行不带 `--create-only` 的 `env.sh fresh`，然后 `up`、`start`。这样也避免重新开启关单时处理大量历史压测订单。正常开发启动命令始终保持原行为。

```bash
./benchmark/scripts/prepare.sh --run-id baseline-100 --users 100000 --stock 100000
./benchmark/scripts/run.sh --run-id baseline-100 --threads 100 --rampup 30 --duration 60
./benchmark/scripts/verify.sh --run-id baseline-100 --wait 180

# 每档新身份/新券；CREATE 落库后进入下一档，不等待关单
./benchmark/scripts/suite.sh --prefix baseline-a --levels 100,200,500,800,1000 \
  --users 100000 --stock 100000 --rampup 30 --duration 60
```

`duration` 指稳定运行的计划时长，JMeter 总计划时长为 `rampup + duration`。完成采样按 Thread scheduler 的开始 epoch 划分窗口，稳定统计排除 ramp-up；EOF、启动慢或客户端故障可能使实际覆盖不足。

线程数表示并发闭环用户，**不表示 QPS**。无 pacing 时，响应慢会降低发送速率，有 coordinated omission 风险；本方案不宣称开环到达率。另提供标准 Constant Throughput Timer（所有活跃线程共享）的目标 RPS 模式：

```bash
./benchmark/scripts/prepare.sh --run-id paced-300 --users 40000 --stock 40000
./benchmark/scripts/run.sh --run-id paced-300 --threads 100 --rampup 30 --duration 60 --target-rps 300
```

300 是计划发送速率，不是实测值。Timer 不能在工作线程耗尽时维持开环流量；检查实际 HTTP QPS、JMeter CPU/GC 和达到率。身份数至少覆盖预期 `(rampup + duration) * RPS` 并留余量；无目标速率时根据小规模校准准备更大池。库存至少等于用户数。每轮默认发送窗口仅 90 秒。较长实验或多轮累计超过 15 分钟时，历史订单的后台关单可能占用资源；保留原始监测说明这一背景，不等待其完成。

也可直接加载 `.jmx`（自己负责隔离、安全检查和单次身份使用）：

```bash
benchmark/.runtime/apache-jmeter-5.6.3/bin/jmeter -n \
  -t benchmark/jmeter/nearhub-seckill.jmx -q benchmark/jmeter/results.properties \
  -Jthreads=100 -Jrampup=30 -Jduration=60 -Jhost=127.0.0.1 -Jport=18081 \
  -JvoucherId=实际券ID -Jtokens="$PWD/benchmark/data/实际轮次/tokens.csv" \
  -Jclassifier="$PWD/benchmark/jmeter/classify.groovy" \
  -l /tmp/nearhub-new-run.jtl -j /tmp/nearhub-new-run.log \
  -e -o /tmp/nearhub-new-html
```

JMX 自带默认 host=127.0.0.1、port=8081、threads=100、rampup=30、duration=60，脚本覆盖为隔离端口。CSV `shareMode.all`、`recycle=false`、`stopThread=true`；Thread Group 的 `same_user_on_next_iteration=true` 用于保留 TCP 连接，不控制 CSV 身份，每个请求仍使用新的 authorization Token；每次循环仅一个 HTTP 请求，没有登录/结果轮询混进 QPS。

## 5. 指标、业务结果与报告

每轮 `benchmark/results/{run-id}/`：

| 文件 | 内容 |
|---|---|
| environment.json、parameters.json、manifest.json | 硬件、Docker 限制、JDK/堆、实例数、源码/JAR 指纹、券/库存、参数 |
| requests.jtl、html/index.html | 每个请求的耗时、状态及分类；JMeter 原生 HTML |
| summary.json / summary.csv / report.md | 全请求与稳定窗口的 HTTP QPS、有效预扣 QPS、平均/P95/P99、分类、异常率 |
| mysql-observed-tps.json | 监测端读到已提交订单数量的差分 / 实际采样时间；区间平均 MySQL 落库 TPS |
| mysql-create-time.csv | 数据库 create_time 秒桶的新增订单数，**不是提交时刻 TPS**；用于辅助审计 |
| metrics.jsonl | 订单数、Hash 数、尚未落库归属数量估计、库存、Redis 操作/内存、MySQL 连接/查询/锁等待等 |
| mq/*.json、mq-baseline.json | 组积压、消费 TPS、retry/DLQ 历史、Broker 内部队列、调度等原始输出 |
| docker-*.jsonl、jvm-platform-*.json、heap-*.txt、process-*.txt、jmeter-process.jsonl、jmeter-jvm-*.json | 容器 CPU/内存、JVM heap/GC 次数与累计耗时、应用线程数、宿主应用/JMeter 资源 |
| verification-create.json | 精确订单身份与库存、MQ 诊断、未决列表及 CREATE 是否落库 |
| orders/claims-*.csv、unresolved-*.csv | 可独立核对的业务证据；无 Token |
| app.log、jmeter*.log | 本轮诊断；失败仍保留，不打印逐用户响应 |

各轮汇总：`benchmark/results/summary.csv`。`./benchmark/scripts/report.sh --run-id 轮次` 可用保存的证据重新生成汇总，不发送 HTTP 请求；若最初 MySQL 秒桶尚未落盘，需原隔离数据库在线才能补采。JTL/CSV/HTML 全部忽略提交，且权限默认 0600；目录默认 0700。日志可能包含订单身份，分享前脱敏，禁止上传 Token、密码。

| 分类 | 判断 | 有效下单 QPS |
|---|---|---|
| accepted | HTTP 200、success=true、正数订单 ID | 计入 |
| sold_out | HTTP 200、success=false、“库存不足” | 不计 |
| duplicate | HTTP 200、success=false、“不能重复下单” | 不计 |
| rate_limited | 429 | 不计 |
| pending | 202，单独保留候选 orderId/身份 | 不计；事后确认另列 |
| server_error / transport_error / unexpected | 5xx、非 HTTP 错误码、未知格式/业务错误（含 200 服务器异常） | 不计，属于异常 |

HTTP QPS = 窗口内**收到 HTTP 响应**的请求数 / 窗口秒数；另列 attempt_qps，包含连接建立前失败的全部 JMeter 尝试，不能视作服务端吞吐；有效下单 QPS = 窗口内 accepted 数 / 同一窗口秒数；不以 MySQL 计数回填 accepted QPS。P95/P99 用收到 HTTP 响应的请求耗时 nearest-rank；全部采样（包括连接失败）的百分位另存 attempt_p95_ms / attempt_p99_ms，以免零毫秒连接失败掩盖尾延迟。异常率只含异常分类；另列非 accepted 比例。JMeter HTML Error% 因断言规则表示**非 accepted**，不能直接用作异常率。

零发送字节的 BindException / ConnectTimeoutException 单独记为 confirmed_not_sent，仍属于压测异常，不算有效订单。读超时等仍可能已发送。202/丢响应按精确订单或唯一 user/voucher 查 MySQL，再核对归属；有明确订单才计 `resolved_uncertain`，否则 `UNRESOLVED`，不能因 NOT_FOUND 或等待超时宣布失败。后续核实不会重写历史 HTTP 分类。

## 6. 一致性观测与安全清理

CREATE 窗口默认停止发请求后观察最多 180 秒，每轮须连续 3 次：已接受订单全部落库；精确用户/订单归属一致；无重复、超卖；无未知结果。未发生取消时同时核对库存。MQ 组位点、retry/DLQ 保留为诊断数据，因为 CREATE/TIMEOUT 共用组，组积压不再作为完成条件。多次读不构成全局原子快照，收敛后连续采样降低中间态误判，异常保留证据。

库存计算：

* MySQL 应为 `初始库存 - 非取消订单数`。
* Redis 应为 `初始库存 - 归属条数 + 已验证永久返还标记的取消订单数`。
* 取消后归属仍保留；不能拿 `初始库存 - Hash 数` 对取消后的库存做错误判断。
* 默认 CREATE 校验不检查关单或补偿。即使后来已取消，数据库中的订单记录仍证明已成功创建；此时库存还原属于范围外，不阻塞本轮。缺单或无证据 202 仍使校验不通过。

`consumerProgress` 的组位点包含已投递 CREATE/TIMEOUT，不含尚未投递的延迟消息和事务半消息，因此仅作为诊断。默认只核验 CREATE，并返回 `CREATE_PERSISTED`，不等待关单、不检查延迟队列排空，不宣称整个消息生命周期完成。归属/库存审计的通用计算函数保留取消场景测试，但压测流程不采集或等待回补任务。`unpersisted_claims_estimate` 为本轮 Hash 数减已落库订单数的非负估计，辅助观察 CREATE 落库滞后，不等同于 Broker 消息数量。

```bash
# 默认仅停应用/隔离容器，所有卷、任务、消息和对账证据保留，可 env.sh up / start 恢复
./benchmark/scripts/cleanup.sh
```

如需彻底重建独立环境，明确退役整个命名空间；先停止应用、再停止所有隔离中间件，核验全部容器停止，再一起销毁仅该项目的三个卷（包括 Broker Store）。这会**放弃尚未完成的测试消息**，不是宣布它们被消费完。工具先保存 retirement.json。不会对运行中的数据库做逐行删除，不提供 FLUSHALL、DELETE 全库或清空开发环境操作。

```bash
# 从 benchmark/.runtime/environment.json 读取本次 UUID，显式确认；此命令不可恢复。
./benchmark/scripts/cleanup.sh --destroy-isolated --confirm 本次环境UUID
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
# 旧本地 CSV/报告仍保留；新环境不可复用旧 run-id 的文件，改用新 run-id。
```

整个运行目录和 Token 由操作者按保留政策离线归档/删除。工具不在 CREATE/延迟关单/补偿仍活动时单删购买归属或订单，因此不会留下旧消息访问部分删除数据的窗口。

## 7. 环境记录与瓶颈定位

首次检查（2026-11-04）：Apple M5 / 10 核 / 32 GiB；Docker 4 vCPU、8,322,555,904 bytes（约 7.75 GiB），默认宿主 Java 17.0.20.1。Docker 上另有多个开发容器；本次不擅自停它们。正式测试前由操作者选择空闲机器/隔离后台负载，每轮以 `environment.json` 实测为准。

**RocketMQ linux/amd64 在 Apple Silicon 上仿真**，Broker/NameServer 和诊断 mqadmin 都受影响；宿主同时跑应用和 JMeter，Docker VM 又共享 CPU，结果不能直接外推到原生 Linux 服务器。镜像 tag 可变，报告保存 image ID；跨机器复现实验应固定验证过的 digest。

工具复用 docker stats、ps、JDK jcmd/jstat、Redis INFO、MySQL STATUS、mqadmin。另有无需第三方依赖的 `tools/JvmSnapshot.java`，通过 JDK 本地 Attach 启动本地管理连接器读取平台 MBean 的 Heap、GC 次数/累计毫秒和线程数，不开放公开 JMX 端口、不改变业务池/堆参数。实测此 macOS/JDK 的 jstat 无法发现 PID，本地 JMX 与 jcmd 可用；采集失败会保留 error，不伪装为零。MySQL 查询计数与 Redis ops 包含监测流量。默认每 30 秒串行启动若干 mqadmin 会产生明显开销，适合粗粒度定位；不能保证捕捉亚秒峰值，正式报告注明采样频率。

| 现象 | 进一步证据/可能瓶颈 |
|---|---|
| HTTP 延迟升高、CPU 高或线程阻塞 | `jcmd PID Thread.print`，查 Tomcat 工作线程、锁/Socket；对比 GC 和发送等待 |
| Redis Lua/连接池候选瓶颈 | INFO CPU/ops、blocked clients、内存/evictions；按需 Redis SLOWLOG；应用线程栈查 Lettuce pool 等待 |
| MQ 发送慢/202 增多 | app.log 的 SECKILL_SEND_UNCERTAIN、Broker send/EndTransaction queue、disk/put latency；仿真开销 |
| HTTP accepted 正常、落库 TPS 低、lag 上升 | Consumer 线程栈、retry/DLQ、MySQL lock waits / Threads_running、连接池等待 |
| DB CPU/IO 高 | docker stats、InnoDB buffer reads/lock time、必要时只读 `SHOW ENGINE INNODB STATUS` |
| 客户端 CPU/GC 高、目标 RPS 未达到 | JMeter 进程 CPU/RSS、jmeter.log、平台 GC/Heap 采样（或可用时的 `jstat -gcutil JMeterPID`）；更换独立施压机后复测 |

没有新增 Actuator/JMX 暴露端点，所以不自动获得精确 Tomcat busy threads、Hikari active/pending、Lettuce 等待分布；不能仅凭全局连接数声称某连接池就是瓶颈。必要时对受控 PID 附加只读 JDK 工具，避免默认高频线程 dump 干扰负载。

最大稳定有效 QPS 的认定要在多次完整稳定窗口下：无持续上涨积压、窗口后可收敛、无异常/未决、无库存/Token 提前耗尽、客户端未饱和、CREATE 对账通过；再选择最高重复可达档位。当前脚本不自动从一次短跑宣称最大容量或性能提升百分比。

## 8. 故障与恢复

* preflight 失败：未发送请求，保留日志；检查端口、UUID、Token TTL、券/库存、消费者在线状态。
* run 已开始失败：该轮永不重新用 Token；修复原因后新建轮次。报告在原目录保留，执行 verify 继续追踪原订单。
* JMeter ERROR / 无样本 / 无 HTML / CREATE 未落库：非零退出；suite 不执行后续档位。
* `verification-*` 未通过：查看未决列表、业务库存与 MQ/DLQ。不能重置库存、删除 Hash 或生成新订单 ID“修复”结果。
* 消费组未在线或 CLI 输出变化：MQ 指标为 unknown，不能当作 0；保守停止运行。
* 未验证 Broker 崩溃、网络分区、内存压力或故障恢复协议；本次公开结果包含真实 100 线程测量，不能外推故障恢复保证。

## 9. 验证命令与参考

```bash
PYTHONPYCACHEPREFIX=benchmark/.runtime/pycache python3 -m unittest discover -s benchmark/tests -v
./benchmark/scripts/validate.sh
# 原有项目测试，在原有开发隔离测试库/Redis DB 上运行（不指向压测实例）
./scripts/dev.sh test-orders
./scripts/dev.sh test-rate-limit
```

原生组件与 CLI 依据 Apache 文档：[非 GUI 执行](https://jmeter.apache.org/usermanual/get-started.html)、[CSV / Thread Group / Constant Throughput Timer / JSR223](https://jmeter.apache.org/usermanual/component_reference.html)、[结果保存](https://jmeter.apache.org/usermanual/listeners.html)、[官方 5.6.3 发布包](https://jmeter.apache.org/download_jmeter.cgi)。实际执行的验收结果见 [VALIDATION.md](VALIDATION.md)。
