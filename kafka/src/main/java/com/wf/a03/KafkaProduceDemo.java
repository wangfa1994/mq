package com.wf.a03;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.InterruptException;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProduceDemo {


    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","127.0.0.1:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String,String>(kafkaProps); /*必须需要的三个属性*/

        sendMessage01(producer);



    }

    /*最简单的消息发送方式 发送并忘记*/
    private static void sendMessage01(KafkaProducer producer) {
        // 主题名，发送的消息的键和值，这里的键和值要和设置的序列化器相对应
        ProducerRecord<String,String> record = new ProducerRecord<>("customerCountry","precision Products","France");
        try {
            producer.send(record); // 生产者接收ProducerRecord对象，进行发送消息，消息会被先放入到缓存区，然后通过单独的线程再发送给服务器端。
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /*同步发送消息*/
    private static void sendMessage02(KafkaProducer producer) {
        ProducerRecord<String,String> record = new ProducerRecord<>("customerCountry","precision Products","France");
        try {
            Future<RecordMetadata> result = producer.send(record); // 一般我们需要接收到send返回的包含的RecordMetadata的ReFuture对象，
            RecordMetadata recordMetadata = result.get();
        }catch (Exception e){
            e.printStackTrace(); // 可以忽略在发送消息时发生的错误或服务器端返回的错误，但在发送消息之前，生产者仍有可能抛出其他的异常。这些异常可能是 SerializationException（序列化消息失败）、BufferExhaustedException 或 TimeoutException（缓冲区已满），或者InterruptException（发送线程被中断）
        }
    }

    /*发送者KafkaProducer一般会出现两种错误
    一种是可重试错误，这种错误可以通过重发消息来解决，例如:链接错误，非分区首领错误(not leader for partition)，如果多次重试仍然无法解决，那么就会收到重试异常
    一种是无法通过重试解决的错误，比如“Message size too large”（消息太大）。对于这种错误，不进行重试，直接抛出异常
    * */

    /*异步发送消息*/
    private static void sendMessage03(KafkaProducer producer) {
        ProducerRecord<String,String> record = new ProducerRecord<>("customerCountry","precision Products","France");
        try {
            producer.send(record,new DemoProducerCallBack()); //设置回调函数，这样的话，我们就不需要进行get方法等待了
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /* 异步发送消息的回调*/
    private static class DemoProducerCallBack implements Callback{
        /* 回调的执行将在生产者主线程中进行，如果有两条消息被发送给同一个分区，则这可以保证它们的回调是按照发送的顺序执行的。这就要求回调的执行要快，避免生产者出现延迟或影响其他消息的发送。不建议在回调中执行阻塞操作，阻塞操作应该被放在其他线程中执行 */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception != null){
                exception.printStackTrace();
            }
        }
    }
}
