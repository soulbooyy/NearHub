package nearhub.benchmark;

import com.hmdp.mq.RocketMQProducer;
import com.hmdp.service.impl.VoucherOrderServiceImpl;

/** Tests interception boundaries without starting or substituting a middleware client. */
public class CreateOnlyBenchmarkTest {
    public static void main(String[] args) throws Exception {
        RocketMQProducer producer = (RocketMQProducer) CreateOnlyBenchmark.wrap(new RocketMQProducer());
        producer.sendOrderTimeout(null); // Would otherwise dereference the order/mapper.
        boolean createDelegated = false;
        try { producer.sendOrderCreateInTransaction(null, null); }
        catch (NullPointerException expected) { createDelegated = true; } // Uninitialized real producer, not a no-op.
        if (!createDelegated) throw new AssertionError("CREATE was intercepted");
        VoucherOrderServiceImpl service = (VoucherOrderServiceImpl) CreateOnlyBenchmark.wrap(new VoucherOrderServiceImpl());
        service.cancelTimeoutOrder(1L);
        boolean persistenceDelegated = false;
        try { service.createOrderFromMQ(null); }
        catch (RuntimeException expected) { persistenceDelegated = true; }
        if (!persistenceDelegated) throw new AssertionError("Persistence was intercepted");
        CreateOnlyBenchmark.CreateOnlyMode mode = new CreateOnlyBenchmark.CreateOnlyMode();
        if (mode.getSkippedTimeoutSends() != 1 || mode.getSkippedCancellations() != 1) {
            throw new AssertionError("Unexpected interception counts");
        }
        Object unrelated = new Object();
        if (CreateOnlyBenchmark.wrap(unrelated) != unrelated) throw new AssertionError("Unrelated bean altered");
        System.out.println("CREATE-only boundaries passed: only timeout send/cancel suppressed; CREATE/persistence delegated");
    }
}
