package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RocketMQTimeoutProducerTest {
    @Test void nonOkSendStatusIsFailureForConsumerRetry() throws Exception {
        TransactionMQProducer client = mock(TransactionMQProducer.class);
        RocketMQProducer producer = new RocketMQProducer();
        ReflectionTestUtils.setField(producer, "producer", client);
        VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
        when(orders.databaseNow()).thenReturn(LocalDateTime.now());
        ReflectionTestUtils.setField(producer, "orders", orders);
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.FLUSH_DISK_TIMEOUT);
        when(client.send(any(Message.class))).thenReturn(result);
        VoucherOrder order = new VoucherOrder();
        order.setId(1L); order.setCreateTime(LocalDateTime.now());
        assertThrows(MQClientException.class, () -> producer.sendOrderTimeout(order));
    }

    @Test void overdueOrderIsSentWithoutAnotherDelay() throws Exception {
        TransactionMQProducer client = mock(TransactionMQProducer.class);
        RocketMQProducer producer = new RocketMQProducer();
        ReflectionTestUtils.setField(producer, "producer", client);
        VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
        when(orders.databaseNow()).thenReturn(LocalDateTime.now());
        ReflectionTestUtils.setField(producer, "orders", orders);
        SendResult result = new SendResult(); result.setSendStatus(SendStatus.SEND_OK);
        when(client.send(any(Message.class))).thenReturn(result);
        VoucherOrder order = new VoucherOrder();
        order.setId(1L); order.setCreateTime(LocalDateTime.now().minusMinutes(16));
        producer.sendOrderTimeout(order);
        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(client).send(sent.capture());
        assertEquals(0, sent.getValue().getDelayTimeLevel());
    }
    @Test void delayUsesDatabaseClockEvenWhenApplicationClockDiffers() throws Exception {
        TransactionMQProducer client = mock(TransactionMQProducer.class);
        RocketMQProducer producer = new RocketMQProducer();
        ReflectionTestUtils.setField(producer, "producer", client);
        VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
        LocalDateTime created = LocalDateTime.of(2000, 1, 1, 0, 0);
        when(orders.databaseNow()).thenReturn(created.plusMinutes(5));
        ReflectionTestUtils.setField(producer, "orders", orders);
        SendResult result = new SendResult(); result.setSendStatus(SendStatus.SEND_OK);
        when(client.send(any(Message.class))).thenReturn(result);
        VoucherOrder order = new VoucherOrder(); order.setId(1L); order.setCreateTime(created);
        producer.sendOrderTimeout(order);
        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(client).send(sent.capture());
        assertEquals(14, sent.getValue().getDelayTimeLevel()); // Ten minutes left on DB clock.
    }
}
