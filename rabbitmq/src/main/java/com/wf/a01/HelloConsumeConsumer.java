package com.wf.a01;

import com.rabbitmq.client.*;

import java.io.IOException;

public class HelloConsumeConsumer {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://guest:guest@127.0.0.1:5672/%2fwangfa");

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        // 确保MQ中有该队列，如果没有则创建
        channel.queueDeclare("queue.biz", false, false, true, null);


        int i = 0;
        DeliverCallback callback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                System.out.println(consumerTag+"=="+new String(message.getBody()));
            }

        };

        // 监听消息，一旦有消息推送过来，就调用第一个lambda表达式
        channel.basicConsume("queue.biz", callback, (consumerTag) -> {});


        // 创建消费者
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received message: " + message);
            }
        };
        // 启动消费者并指定消费者标签
        channel.basicConsume("queue.biz", true, "sss", consumer);
    }
}
