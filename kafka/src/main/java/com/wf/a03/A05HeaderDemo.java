package com.wf.a03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class A05HeaderDemo {

    // 标头
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","127.0.0.1:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String,String>(kafkaProps); /*必须需要的三个属性*/

        // 主题名，发送的消息的键和值，这里的键和值要和设置的序列化器相对应
        ProducerRecord<String,String> record = new ProducerRecord<>("customerCountry","precision Products","France");

        // 标头添加一些有关记录的元数据。标头指明了记录数据的来源 ，标头由一系列有序的键–值对组成。键是字符串，值可以是任意被序列化的对象，就像消息里的值一样
        record.headers().add("privacy-level","YOLO".getBytes(StandardCharsets.UTF_8));

        try {
            producer.send(record); // 生产者接收ProducerRecord对象，进行发送消息，消息会被先放入到缓存区，然后通过单独的线程再发送给服务器端。
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
