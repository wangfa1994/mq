package com.wf.a04;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        props.put(ConsumerConfig.GROUP_ID_CONFIG,"CountryCounter"); //  指定了消费者所属的群组的名字

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer01(consumer);

    }

    private static void consumer01(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList("customerCountries")); // 订阅主题 ，可以传递一个正则表达式
        // consumer.subscribe(Pattern.compile("test.*"));


        // 消费者API最核心的东西就是通过一个简单的轮训向服务器请求数据。

        Map<String,Integer> custCountryMap = new HashMap<>();
        Duration timeout = Duration.ofMillis(100);
        // 消费者实际上是一个长时间运行的应用程序，他通过持续轮询来向Kafka请求数据
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(timeout); //消费者持续对Kafka进行轮询,否则就会被认为是死亡，他所消费的分区就会被移交给群组里其他的消费者
            // timeout表示超时时间间隔，用于控制poll的阻塞时间，当消费者缓冲区中没有消息的时候会发生阻塞，如果这个参数没有被设定为可用的数据或者为0,那么poll就会立即返回，否则他会等待指定的毫秒数
            // 返回一个记录列表，列表中的每一条记录都包含了主题,分区,记录在分区里的偏移量,记录的键值对信息。我们遍历列表，逐条处理记录

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                int updatedCount = 1;
                if (custCountryMap.containsKey(record.value())) {
                    updatedCount = custCountryMap.get(record.value()) + 1;
                }
                custCountryMap.put(record.value(), updatedCount);
                System.out.println(JSON.toJSONString(custCountryMap));
            }


        }

    }
}
