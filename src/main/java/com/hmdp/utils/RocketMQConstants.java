package com.hmdp.utils;

/**
 * RocketMQ 消息队列常量
 */
public class RocketMQConstants {

    /** 秒杀订单 Topic */
    public static final String ORDER_TOPIC = "order-seckill-topic";
    /** 创建订单消息 Tag */
    public static final String ORDER_TAG_CREATE = "CREATE";
    /** 超时关单消息 Tag（延迟消息，到点触发） */
    public static final String ORDER_TAG_TIMEOUT = "TIMEOUT";

    /** 事务消息生产者组（Broker 按组回查） */
    public static final String ORDER_TX_PRODUCER_GROUP = "seckill-tx-producer-group";
    public static final String ORDER_CONSUMER_GROUP = "order-consumer-group";

}
