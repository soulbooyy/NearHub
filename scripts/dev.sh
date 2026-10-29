#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo 'Created .env from .env.example (local development credentials).'
fi
set -a
# .env is a trusted local shell file; quote values containing shell characters.
source .env
set +a

use_java8() {
  if [[ -x .local/jdk8/Contents/Home/bin/java ]]; then
    export JAVA_HOME="$PWD/.local/jdk8/Contents/Home"
  elif [[ -x .local/jdk8/bin/java ]]; then
    export JAVA_HOME="$PWD/.local/jdk8"
  fi
  if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  fi
  if ! java -version 2>&1 | head -1 | grep -q '"1.8.'; then
    echo 'JDK 8 is required. Set JAVA_HOME in .env or install it under .local/jdk8.' >&2
    exit 1
  fi
}

migrate() {
  docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' \
    < src/main/resources/db/migrations/V002__order_stock_compensation.sql
  docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' \
    < src/main/resources/db/migrations/V003__seckill_recovery_task.sql
}

case "${1:-help}" in
  up)
    docker compose up -d --wait --wait-timeout 240
    migrate
    for topic in order-seckill-topic canal-binlog-topic; do
      docker compose exec -T broker sh mqadmin updateTopic \
        -n namesrv:9876 -b 127.0.0.1:10911 -t "$topic" -r 4 -w 4
    done
    ;;
  build)
    use_java8
    mvn -B -DskipTests clean package
    ;;
  run)
    use_java8
    exec mvn spring-boot:run
    ;;
  test)
    use_java8
    # Existing targeted tests validate context startup, DB/Redis access and locks.
    exec mvn -B '-Dtest=NormalTest,RedissonTest,HmDianPingApplicationTests#loadShopData' test
    ;;
  test-cache)
    use_java8
    if [[ ! "$MYSQL_DATABASE" =~ ^[a-zA-Z0-9_]+$ || ! "$MYSQL_USER" =~ ^[a-zA-Z0-9_]+$ ]]; then
      echo 'test-cache requires simple MySQL database/user identifiers.' >&2
      exit 1
    fi
    docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -u root' <<SQL
CREATE DATABASE IF NOT EXISTS hmdp_cache_test CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS hmdp_cache_test.tb_shop LIKE $MYSQL_DATABASE.tb_shop;
GRANT ALL ON hmdp_cache_test.* TO '$MYSQL_USER'@'%';
SQL
    exec mvn -B '-Dtest=MultiLevelCacheIntegrationTest,CacheRebuildWebTest,ShopCacheInvalidationIntegrationTest' test
    ;;
  test-rate-limit)
    use_java8
    exec mvn -B '-Dtest=SlidingWindowWebTest,SlidingWindowIntegrationTest' test
    ;;
  test-orders)
    use_java8
    # Only provision an isolated test schema; never import/drop production tables.
    if [[ ! "$MYSQL_DATABASE" =~ ^[a-zA-Z0-9_]+$ || ! "$MYSQL_USER" =~ ^[a-zA-Z0-9_]+$ ]]; then
      echo 'test-orders requires simple MySQL database/user identifiers.' >&2
      exit 1
    fi
    docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -u root' <<SQL
CREATE DATABASE IF NOT EXISTS hmdp_order_test CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS hmdp_order_test.tb_seckill_voucher LIKE $MYSQL_DATABASE.tb_seckill_voucher;
CREATE TABLE IF NOT EXISTS hmdp_order_test.tb_voucher_order LIKE $MYSQL_DATABASE.tb_voucher_order;
GRANT ALL ON hmdp_order_test.* TO '$MYSQL_USER'@'%';
SQL
    docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" hmdp_order_test' \
      < src/main/resources/db/migrations/V002__order_stock_compensation.sql
    docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" hmdp_order_test' \
      < src/main/resources/db/migrations/V003__seckill_recovery_task.sql
    exec mvn -B '-Dtest=SeckillRecoveryIntegrationTest,SeckillTransactionIntegrationTest,OrderSubmissionIntegrationTest,OrderConsumptionIntegrationTest,OrderCancellationIntegrationTest,OrderTimeoutPolicyTest,OrderMQConsumerTest,RocketMQTimeoutProducerTest' test
    ;;
  migrate) migrate ;;
  status) docker compose ps ;;
  down) docker compose down ;;
  *) echo 'Usage: ./scripts/dev.sh {up|migrate|build|run|test|test-cache|test-rate-limit|test-orders|status|down}' ;;
esac
