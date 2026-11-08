package nearhub.benchmark;

import com.hmdp.HmDianPingApplication;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/** Optional external benchmark entry point. Never packaged into the normal application. */
@Configuration
public class CreateOnlyBenchmark {
    private static final AtomicLong TIMEOUT_SENDS = new AtomicLong();
    private static final AtomicLong CANCELLATIONS = new AtomicLong();

    public static void main(String[] args) {
        if (!Boolean.getBoolean("nearhub.benchmark.createOnly")) {
            throw new IllegalStateException("Explicit isolated benchmark opt-in required");
        }
        SpringApplication.run(new Class<?>[]{HmDianPingApplication.class, CreateOnlyBenchmark.class}, args);
    }

    public interface CreateOnlyModeMBean {
        boolean isEnabled();
        long getSkippedTimeoutSends();
        long getSkippedCancellations();
    }
    public static class CreateOnlyMode implements CreateOnlyModeMBean {
        public boolean isEnabled() { return true; }
        public long getSkippedTimeoutSends() { return TIMEOUT_SENDS.get(); }
        public long getSkippedCancellations() { return CANCELLATIONS.get(); }
    }

    @Bean
    public static BeanPostProcessor createOnlyInterceptor(Environment env) throws Exception {
        String url = env.getRequiredProperty("spring.datasource.url");
        String nameserver = env.getRequiredProperty("rocketmq.name-server");
        if (!Boolean.getBoolean("nearhub.benchmark.createOnly")
                || !url.contains("/nearhub_benchmark?")
                || !url.startsWith("jdbc:mysql://127.0.0.1:")
                || !nameserver.equals("127.0.0.1:" + System.getenv("NAMESRV_PORT_FOR_BENCHMARK"))) {
            throw new IllegalStateException("CREATE-only mode is restricted to the isolated benchmark environment");
        }
        ManagementFactory.getPlatformMBeanServer().registerMBean(
                new CreateOnlyMode(), new ObjectName("nearhub.benchmark:type=CreateOnlyMode"));
        System.out.println("BENCHMARK_CREATE_ONLY_ENABLED: TIMEOUT sends and timeout cancellations are disabled; real CREATE pipeline remains enabled");
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                return wrap(bean);
            }
        };
    }

    static Object wrap(Object bean) {
        String target = AopUtils.getTargetClass(bean).getName();
        final String method;
        final AtomicLong count;
        if (target.equals("com.hmdp.mq.RocketMQProducer")) {
            method = "sendOrderTimeout";
            count = TIMEOUT_SENDS;
        } else if (target.equals("com.hmdp.service.impl.VoucherOrderServiceImpl")) {
            // Both the MQ listener and the periodic fallback call this public service boundary.
            method = "cancelTimeoutOrder";
            count = CANCELLATIONS;
        } else {
            return bean;
        }
        MethodInterceptor advice = invocation -> {
            if (invocation.getMethod().getName().equals(method)) {
                count.incrementAndGet();
                return null;
            }
            return invocation.proceed();
        };
        if (bean instanceof Advised) {
            ((Advised) bean).addAdvice(0, advice);
            return bean;
        }
        ProxyFactory factory = new ProxyFactory(bean);
        factory.setProxyTargetClass(true);
        factory.addAdvice(advice);
        return factory.getProxy();
    }
}
