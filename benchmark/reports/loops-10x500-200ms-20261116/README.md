# NearHub 秒杀 Burst · 10 线程 × 500 次循环，Broker 200 ms

2026-11-16，10 个闭环线程各完成 500 次真实下单。5000 个独立用户全部明确预扣成功并最终落库。与同为 200 ms、发压期开启 mqadmin 的 50 × 100 单轮相比，本轮少拒绝 36 笔，接口 P99 缩短 215 ms，有效预扣 QPS 降低 13.0%。

| 指标 | 50 × 100，券 29 | 10 × 500，券 33 |
|---|---:|---:|
| 发起 / 完成 | 5000 / 5000 | 5000 / 5000 |
| 首笔开始至末笔完成 | 7.411 s | 8.582 s |
| HTTP QPS | 674.67 | 582.61 |
| 明确预扣成功 / 有效预扣 QPS | 4964 / 669.82 | 5000 / 582.61 |
| HTTP 202 / 其中 Broker 写入前拒绝 | 36 / 36 | 0 / 0 |
| TIMEOUT_CLEAN_QUEUE 拒绝率 | 0.72% | 0.00% |
| 全样本接口平均 / P95 / P99 | 72.27 / 232 / 317 ms | 16.46 / 53 / 102 ms |
| 仅成功预扣样本的接口 P99 | 309 ms | 102 ms |
| 读取超时 / 其他异常 | 0 / 0 | 0 / 0 |
| MySQL 订单 / Redis 归属 | 4964 / 4964 | 5000 / 5000 |
| MySQL / Redis 最终库存 | 36 / 36 | 0 / 0 |
| 停压后首次采到全部成功订单 | +11.00 s | +9.84 s |
| 最终已投递积压 / Retry / DLQ | 0 / 0 / 0 | 0 / 0 / 0 |

### 条件与口径

Docker Desktop 配额 4 vCPU / 8192 MiB，引擎实际可见内存 8,322,555,904 字节。Spring Boot 和 JMeter 在宿主机运行。已核对本轮与对照轮的应用源码摘要、容器镜像 ID 一致。专用 Broker 启动日志确认 `waitTimeMillsInSendQueue=200`。零 ramp-up、无 RPS 限制，60 秒发压安全上限，120 秒 CREATE 观察上限。

先在券 32 上使用 6 个独立身份完成 2×3 真实冒烟，6 单全部落库。正式券 33 和 5000 个身份均为新建，本轮 Token 和用户 ID 与冒烟及历史两轮 50×100 均无交集。共享 CSV 每次循环分配一个新身份，JTL 显示 10 个线程各完成 500 次，所有请求均在截止时间内结束。两次原生 mqadmin 会话在正式发压期启动，消费进度和主题查询分别与窗口重叠 5.03 秒与 3.56 秒。JMeter 在发压期有 29 个进程快照，CPU 峰值 215.3%，RSS 峰值 341920 KiB。

有效预扣 QPS 使用明确预扣成功数除以实际发压秒数。接口 P99 取全部 JMeter 请求耗时的 nearest-rank。停压后异步订单继续落库，逐单落库 P99 未测量。两种线程设置各只有一次观测，本轮的零拒绝仅适用于这次 8.582 秒突发。

### 一致性与证据

逐笔核对用户、券、接口返回订单 ID、MySQL 订单和 Redis 归属，全部 5000 笔一致。无重复订单、一人多单、超卖或未确认结果。离线核验 Broker 的有效 CommitLog，15018 条记录均通过 CRC 校验，正式订单 ID 在事务半消息和业务主题中均全部命中。持续采样期间 Redis 归属最多领先已提交订单 3094 笔，停压后约 9.84 秒首次观测到全部订单。CREATE 核验通过，原始运行退出码为 0。[逐笔核验明细](request-audit.csv.gz) · [审计数据](audit.json)

### 本轮使用的复现命令

以下命令依赖运行时工作区中的 Burst 固定循环功能和逐笔核验脚本。本次提交仅发布实测结果。

在仓库根目录使用已停机的隔离环境和上述配额。使用默认 benchmark Broker 配置，并在发压前从专用 Broker 启动日志确认 200 ms。

```bash
run="loops-10x500-200ms-$(date +%Y%m%d-%H%M%S)"
./benchmark/scripts/env.sh fresh --create-only
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
./benchmark/scripts/prepare.sh --run-id "$run-smoke" --users 6 --stock 6
./benchmark/scripts/run.sh --run-id "$run-smoke" --smoke --mode burst --threads 2 --loops 3 --rampup 0 --duration 20 --load-mqadmin on
./benchmark/scripts/prepare.sh --run-id "$run" --users 5000 --stock 5000
rc=0
./benchmark/scripts/run.sh --run-id "$run" --mode burst --threads 10 --loops 500 --rampup 0 --duration 60 --create-wait 120 --load-mqadmin on || rc=$?
printf '%s\n' "$rc" > "benchmark/results/$run/run-exit.txt"
python3 benchmark/reports/loops-50x100-200ms-20261116/verify.py "$run" --threads 10 --loops 500 --wait-ms 200 --collect --output "benchmark/results/$run/audit"
./benchmark/scripts/cleanup.sh
```

本轮原始 JTL、资源快照、应用及 Broker 日志、落库进度和有效 CommitLog 在 `benchmark/results/loops-10x500-200ms-20261116/`。离线复核时指定该运行 ID 并省略 `--collect`。
