package com.wf.a03.a03Avro;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class AvroSerializerDemo {

    public static void main(String[] args) {

        sendMsessage();
    }

    /* 如何把生成的 Avro 对象发送给 Kafka */
    private static void sendMsessage() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"); // KafkaAvroSerializer
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"); // 使用 KafkaAvroSerializer 来序列化对象 ,注意 KafkaAvroSerializer 也可以处理原始类型 包括String
        props.put("schema.registry.url", "schemaUrl"); //  生产者要传给序列化器的参数，其指向模式的存储位置

        String topic = "customerContacts";
        Producer<String, Customer> producer = new KafkaProducer<>(props);
        // 不断生成新事件，直到有人按下Ctrl-C组合键
        while (true) {
            Customer customer = null; // CustomerGenerator.getNext();  //Customer 是生成的对象，也就是记录的值  从 Avro 模式生成对象请参考 Avro 文档
            System.out.println("Generated customer " + customer); //
            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer.getName(), customer); // 实例化一个 ProducerRecord 对象，指定值的类型为 Customer，并传给它一个 Customer 对象
            producer.send(record); // 把 Customer 对象作为记录发送出去，KafkaAvroSerializer 会处理剩下的事情
        }
    }

    /*
    Customer 类不是一个普通的 Java 类（POJO），而是基于模式生成的 Avro 对象。Avro 序列化器只能序列化 Avro 对象，不能序列化 POJO。
    可以用 avro-tools.jar 或 Avro 的 Maven 插件来生成 Avro 类，它们都是 Apache Avro 项目的一部分。关于如何生成 Avro 类，请参考 Apache Avro 入门指南
    * */
}
