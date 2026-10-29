package com.hmdp.order;

import com.hmdp.config.RedissonConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.OrderStockCompensationMapper;
import com.hmdp.task.OrderStockCompensationWorker;
import com.hmdp.task.SeckillRecoveryWorker;
import com.hmdp.mapper.SeckillRecoveryTaskMapper;
import com.hmdp.mq.RocketMQProducer;
import com.hmdp.service.*;
import com.hmdp.service.impl.SeckillVoucherServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.task.SeckillReconcileTask;
import com.hmdp.utils.*;
import org.junit.jupiter.api.*;
import org.mybatis.spring.annotation.MapperScan;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.delegatesTo;

/** Real MySQL + Redis, no outer test transaction, scheduler or live MQ listeners. */
@ActiveProfiles("order-integration-test")
@SpringBootTest(classes = OrderIntegrationSupport.TestApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://${MYSQL_HOST:127.0.0.1}:${MYSQL_PORT:13306}/hmdp_order_test?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8",
        "spring.redis.database=15"
})
abstract class OrderIntegrationSupport {
    @Configuration
    @Profile("order-integration-test")
    @EnableAutoConfiguration
    @MapperScan("com.hmdp.mapper")
    @Import({VoucherOrderPersistenceService.class, VoucherOrderServiceImpl.class, SeckillReservationService.class,
            SeckillVoucherServiceImpl.class, RedissonConfig.class, RedisIdWorker.class, SeckillReconcileTask.class,
            VoucherOrderCancellationService.class, RedisStockReturnService.class, OrderStockCompensationWorker.class, SeckillRecoveryWorker.class})
    static class TestApplication {
        @Bean
        static BeanPostProcessor mapperFaultInjection() {
            // MyBatis creates final JDK proxies; use interface mocks delegating to real mappers.
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    if (bean instanceof VoucherOrderMapper) {
                        return mock(VoucherOrderMapper.class, delegatesTo(bean));
                    }
                    if (bean instanceof SeckillVoucherMapper) {
                        return mock(SeckillVoucherMapper.class, delegatesTo(bean));
                    }
                    if (bean instanceof OrderStockCompensationMapper) {
                        return mock(OrderStockCompensationMapper.class, delegatesTo(bean));
                    }
                    if (bean instanceof RedisStockReturnService) {
                        return spy((RedisStockReturnService) bean);
                    }
                    return bean;
                }
            };
        }
    }

    @Autowired SeckillRecoveryTaskMapper recoveryTasks;
    @Autowired SeckillRecoveryWorker recovery;
    @Autowired SeckillReservationService reservations;
    @Autowired VoucherOrderPersistenceService persistence;
    @Autowired VoucherOrderServiceImpl service;
    @Autowired SeckillReconcileTask reconcile;
    @Autowired JdbcTemplate jdbc;
    @Autowired StringRedisTemplate redis;
    @Autowired RedissonClient redisson;
    @Autowired VoucherOrderMapper orders;
    @Autowired SeckillVoucherMapper vouchers;
    @Autowired OrderStockCompensationMapper tasks;
    @Autowired RedisStockReturnService redisReturns;
    @Autowired OrderStockCompensationWorker worker;
    @Autowired VoucherOrderCancellationService cancellation;
    @MockBean RocketMQProducer producer;

    static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis() * 1000);
    long voucherId, userId, orderId;

    @BeforeEach
    void setup() {
        assertEquals("hmdp_order_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
        voucherId = IDS.incrementAndGet();
        userId = IDS.incrementAndGet();
        orderId = IDS.incrementAndGet();
        jdbc.update("INSERT INTO tb_seckill_voucher(voucher_id,stock,initial_stock,begin_time,end_time) VALUES (?,10,10,?,?)",
                voucherId, LocalDateTime.now().minusHours(1), LocalDateTime.now().plusDays(1));
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "10");
        reservations.prepare(message());
        login(userId);
    }

    @AfterEach
    void cleanup() {
        UserHolder.removeUser();
        reset(orders, vouchers, tasks, redisReturns);
        org.springframework.test.util.ReflectionTestUtils.setField(worker, "maxAttempts", 10);
        jdbc.update("DELETE FROM tb_seckill_recovery_task WHERE voucher_id=?", voucherId);
        jdbc.update("DELETE FROM tb_order_stock_compensation WHERE voucher_id=?", voucherId);
        jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", voucherId);
        jdbc.update("DELETE FROM tb_seckill_voucher WHERE voucher_id=?", voucherId);
        redis.delete(Arrays.asList(SECKILL_STOCK_KEY + voucherId, SECKILL_ORDER_KEY + voucherId,
                SECKILL_CLAIM_OWNER_KEY + voucherId, SECKILL_TXN_KEY + (orderId + 1), SECKILL_TXN_KEY + orderId,
                SECKILL_STOCK_RETURNED_KEY + orderId, SECKILL_STOCK_RETURNED_KEY + (orderId + 1)));
    }

    VoucherOrder message() {
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        return order;
    }

    int stock() { return jdbc.queryForObject("SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", Integer.class, voucherId); }
    int count() { return jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=?", Integer.class, voucherId); }
    void login(long id) {
        com.hmdp.dto.UserDTO user = new com.hmdp.dto.UserDTO(); user.setId(id); UserHolder.saveUser(user);
    }
    VoucherOrder reservedMessage() { VoucherOrder order = message(); reserve(order); return order; }
    void reserve(VoucherOrder order) {
        if (reservations.read(order.getId()).isEmpty()) reservations.prepare(order);
        assertEquals(0, reservations.reserve(order));
    }

}
