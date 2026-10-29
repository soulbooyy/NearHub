package com.hmdp.mq;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.utils.MultiLevelCacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * Canal binlog 缓存驱逐：MySQL 变更 → RocketMQ → 清除 Caffeine + Redis
 * 监听 tb_shop，驱逐 Redis 与当前 JVM 的 Caffeine；多实例本地缓存失效仍需完善。
 */
@Slf4j
@Component
public class CanalMQConsumer {

    public static final String CANAL_TOPIC = "canal-binlog-topic";

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Resource
    private MultiLevelCacheService multiLevelCacheService;

    private DefaultMQPushConsumer consumer;

    private final Map<String, Consumer<JSONObject>> tableHandlers = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {
        tableHandlers.put("tb_shop", this::evictShop);

        consumer = new DefaultMQPushConsumer("canal-cache-consumer-group");
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(CANAL_TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    handleBinlogEvent(new String(msg.getBody(), StandardCharsets.UTF_8));
                } catch (Exception e) {
                    log.error("binlog 事件处理失败, msgId={}", msg.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        log.info("Canal 缓存驱逐消费者启动成功，监听 topic={}", CANAL_TOPIC);
    }

    private void handleBinlogEvent(String body) {
        JSONObject flat = JSONUtil.parseObj(body);
        String table = flat.getStr("table");
        Consumer<JSONObject> handler = tableHandlers.get(table);
        if (handler == null) {
            return;
        }
        JSONArray data = flat.getJSONArray("data");
        if (data == null || data.isEmpty()) {
            return;
        }
        for (Object row : data) {
            handler.accept((JSONObject) row);
        }
    }

    private void evictShop(JSONObject row) {
        String id = row.getStr("id");
        if (id == null) {
            return;
        }
        multiLevelCacheService.evict(CACHE_SHOP_KEY, id);
        log.info("[Canal] tb_shop 变更，已驱逐缓存 cache:shop:{}", id);
    }

    @PreDestroy
    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
