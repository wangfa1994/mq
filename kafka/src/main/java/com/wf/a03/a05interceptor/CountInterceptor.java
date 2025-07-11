package com.wf.a03.a05interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/* 统计在特定时间窗口内发送和接收的消息数量 */
public class CountInterceptor implements ProducerInterceptor {

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);



    @Override
    public ProducerRecord onSend(ProducerRecord record) {

        numSent.incrementAndGet();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {
        executorService.shutdown(); // 生产者被关闭时调用，我们可以借助这个机会清理拦截器的状态,和生产者绑定的相关资源，可以在这里进行释放
    }

    @Override
    public void configure(Map<String, ?> configs) {
        /*这个方法来自于 Configurable 接口，可以覆盖实现configure方法，系统会进行回调，然后通过方法参数configs 可以拿到生产者所有的配置属性，然后就可以进行访问他们了 */
        Long windowSize = Long.valueOf(
                (String) configs.get("counting.interceptor.window.size.ms")); // 得到我们自定义的配置属性

        executorService.scheduleAtFixedRate(CountInterceptor::run,
                windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    public static void run(){
        System.out.println(numSent.getAndSet(0));
        System.out.println(numAcked.getAndSet(0));
    }



}
