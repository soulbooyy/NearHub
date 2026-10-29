package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.StockRejectedException;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderMQConsumerTest {
    private final IVoucherOrderService service = mock(IVoucherOrderService.class);
    private final OrderMQConsumer consumer = new OrderMQConsumer(service);

    MessageExt message(String body, int attempt) {
        ReflectionTestUtils.setField(consumer, "maxReconsumeTimes", 3);
        MessageExt msg = new MessageExt();
        msg.setTags("CREATE");
        msg.setBody(body.getBytes(StandardCharsets.UTF_8));
        msg.setReconsumeTimes(attempt);
        return msg;
    }

    @Test void databaseStockMismatchIsNotAcknowledgedEvenAtRetryLimit() {
        VoucherOrder order = new VoucherOrder();
        order.setId(1L); order.setUserId(2L); order.setVoucherId(3L);
        doThrow(new StockRejectedException(order)).when(service).createOrderFromMQ(any());
        for (int attempt : new int[]{0, 3}) {
            assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                    consumer.consumeMessages(Collections.singletonList(message("{\"id\":1,\"userId\":2,\"voucherId\":3}", attempt))));
        }
    }

    @Test void postCommitFailureRemainsRetryable() {
        doThrow(new IllegalStateException("timeout send failed")).when(service).createOrderFromMQ(any());
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                consumer.consumeMessages(Collections.singletonList(message("{\"id\":1}", 0))));
    }

    @Test void malformedPayloadIsRetriedInsteadOfDiscarded() {
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                consumer.consumeMessages(Collections.singletonList(message("not-json", 0))));
        verifyNoInteractions(service);
    }

    @Test void completedHandlingAcknowledgesMessage() {
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                consumer.consumeMessages(Collections.singletonList(message("{\"id\":1}", 0))));
        verify(service).createOrderFromMQ(any());
    }
}
