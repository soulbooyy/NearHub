# 复现 4 vCPU / 8 GiB CREATE-only 方案

这些命令会创建隔离测试数据并发送真实负载。已发布结果可直接用 `python3 benchmark/reports/4c8g-20261104/verify.py` 离线复核，无需重新压测。

## 1. 资源与依赖

* Docker Desktop **4 vCPU、8 GiB**，通过 `docker info --format '{{.NCPU}} {{.MemTotal}}'` 核验实际资源。本方案没有自动改全局 Docker 配置；调整资源可能重启现有容器。
* JDK 8 用于应用；Java 17 用于 JMeter；Python 3.9+、Maven、Docker Compose、curl、tar。
* 业务代码基线为 `81f8075297b5ade41a42b862d8fb2d0af753d6c8`，源码 SHA256 见 environment.json。应用和 JMeter 在宿主机运行。
* 不调线程池、连接池、应用 JVM 堆或索引。源码变更、资源或后台负载变化都需要作为新实验记录。

## 2. 初始化全新的本地环境

在仓库根目录执行：

```bash
./benchmark/scripts/bootstrap.sh
```

编辑自动生成且被 Git 忽略的 `benchmark/.env`：将 `BENCH_CREATE_ONLY=false` 改为 `BENCH_CREATE_ONLY=true`，必要时填写 `APP_JAVA_HOME`（JDK 8）和 `JMETER_JAVA_HOME`（Java 17）。保留自动生成的随机数据库口令及默认隔离端口，不把此文件提交。

全新安装随后执行：

```bash
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
./benchmark/scripts/env.sh check
```

如果本地已有压测环境，不直接复用旧数据，而是在旧环境可访问时先停止并封存，再切换新命名空间：

```bash
./benchmark/scripts/cleanup.sh
./benchmark/scripts/env.sh fresh --create-only
./benchmark/scripts/env.sh up
./benchmark/scripts/env.sh start
./benchmark/scripts/env.sh check
```

`fresh` 用于已有容器的环境切换；不要在从未创建容器的全新安装上把它作为第一条命令。其他开发实例、旧数据卷和历史消息均保留；不执行 FLUSHALL 或逐行清库。

CREATE-only 外部入口启动时自动执行边界测试；正常应用启动入口不会加载此入口。

## 3. 相同的小规模冒烟

以下 run-id 是新实验示例，每次实际执行都需换成未使用的名称：

```bash
./benchmark/scripts/prepare.sh --run-id reproduce-4c8g-smoke-new --users 5 --stock 5
./benchmark/scripts/run.sh --run-id reproduce-4c8g-smoke-new \
  --threads 1 --rampup 0 --duration 5 --target-rps 1 --smoke
```

确认 5 个 accepted 全部落库、库存/归属一致。检查环境中的 `create_only=true`，关单发送计数对应 5 笔；生产者/消费者及数据库都是实际实例。

## 4. 正式单轮

```bash
./benchmark/scripts/prepare.sh --run-id reproduce-4c8g-100-new --users 500000 --stock 500000
./benchmark/scripts/run.sh --run-id reproduce-4c8g-100-new \
  --threads 100 --rampup 30 --duration 60
```

脚本自动检查新券/空订单及归属、库存、所有 Token 存在和 TTL，抽查真实鉴权、消费组实例数和源码指纹，再发压。总发压计划 90 秒，不设置目标 RPS，身份不复用。默认停止发压后观察 CREATE 最多 180 秒，生成 JTL、HTML、指标、对账和日志；出现未决或异常会非零退出并保留结果，不自动加跑。

本次历史轮次的实际编号为 `formal-create-only-100-20261104-2155`、券 14；复现时由工具生成新的券、用户和订单 ID，不强制复用历史编号。

## 5. CREATE 观测与收尾

如已接受订单仍在落库，保留第一次核验文件后延长观测，不重新运行 JMeter、不重置库存：

```bash
# 根据结果判断是否需要延长；未决请求不会因多等一会儿就被当作成功
./benchmark/scripts/verify.sh --run-id reproduce-4c8g-100-new --wait 900
./benchmark/scripts/report.sh --run-id reproduce-4c8g-100-new
```

本次历史执行在 180 秒窗口不足后，用额外采样观察所有已知预扣，约 417.8 秒首次确认全部落库、连续 3 次一致；但 78 个 202 仍未决，因此没有将整体判定改为通过。通用 verify 要求所有不确定请求都得到确认，若剩余只有未决 202，也可能运行到指定等待上限。不能将按既有证据无法确认的请求自动当成失败或成功。

默认停止并保留数据：

```bash
./benchmark/scripts/cleanup.sh
```

不等待超时关单，不销毁卷。普通业务运行方式始终不变；需要恢复完整压测链路时，在停止旧环境后用不带 `--create-only` 的 `fresh` 创建新的环境，避免重新开启关单时处理大量旧测试订单。

## 6. 不发压的验证

```bash
PYTHONPYCACHEPREFIX=benchmark/.runtime/pycache python3 -m unittest discover -s benchmark/tests -v
./benchmark/scripts/validate.sh
python3 benchmark/reports/4c8g-20261104/verify.py
```

validate.sh 使用空 CSV，只验证 JMX 加载和响应分类，不发送 HTTP 请求。原订单/限流测试属于其他验证范围，本次发布检查不启动它们依赖的开发中间件。
