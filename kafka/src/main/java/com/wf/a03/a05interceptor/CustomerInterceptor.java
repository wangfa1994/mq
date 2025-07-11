package com.wf.a03.a05interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

import java.util.Map;

/**
 * 应用场景 捕获监控和跟踪信息、为消息添加标头，以及敏感信息脱敏
 *
 *
 * */
public class CustomerInterceptor implements ProducerInterceptor {


    /** 在记录被发送给 Kafka 之前，甚至是在记录被序列化之前调用。
     * 如果覆盖了这个方法，那么你就可以捕获到有关记录的信息，甚至可以修改它。
     * 只需确保这个方法返回一个有效的ProducerRecord 对象。
     * 这个方法返回的记录将被序列化并发送给 Kafka
     */
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String topic = record.topic();
        Headers headers = record.headers();

        return null;
    }

    /**
     * 在收到 Kafka 的确认响应时调用。
     * 如果覆盖了这个方法，则不可以修改 Kafka 返回的响应，但可以捕获到有关响应的信息
     * */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
