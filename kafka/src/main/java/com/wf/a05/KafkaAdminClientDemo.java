package com.wf.a05;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientDemo {

    public static void main(String[] args) throws Exception {

        /*创建一个kafkaAdminClient*/
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // 唯一必须提供的配置参数是集群的 URI
        AdminClient admin = AdminClient.create(props); // create静态方法会接受一个包含配置参数的Properties对象作为参数。唯一必须提供的配置参数是集群的 URI：用逗号分隔的要连接的 broker 地址清单。
        // TODO: 用AdminClient做一些有用的事情
        admin.close(Duration.ofSeconds(30));//启动了一个 AdminClient，到最后总是要关闭它。
        //需要注意的是，在调用close方法时，可能还会有一些正在执行中的 AdminClient 操作。为此，close方法提供了一个超时参数。一旦调用了close方法，就不能再调用其他方法或发送其他请求，客户端会一直等待响应，直到超时。超时之后，客户端会异常中止所有正在执行中的操作并释放资源。如果没有指定超时时间，则客户端会一直等待所有正在执行中的操作完成

        // 主题管理
        topManager(admin);

        // 配置管理
        configManager(admin);

        // 消费者群组管理
        consumerGroupManager(admin);


    }

    private static void consumerGroupManager(AdminClient admin) throws ExecutionException, InterruptedException {
        // 查看消费者组 如果想查看和修改消费群组，那么第一步是将它们列出来
        admin.listConsumerGroups().valid().get().forEach(System.out::println); //调用valid()方法，可以让get()返回的消费者群组只包含由集群正常返回的消费者群组。错误都将被忽略，不作为异常抛出
        admin.listConsumerGroups().errors().get().forEach(System.out::println);// errors()方法获取所有的异常
        admin.listConsumerGroups().all().get().forEach(System.out::println); // all()方法，则只有集群返回的第一个错误会作为异常抛出。这类错误有可能是因没有查看群组的权限或某些群组协调器不可用导致的

        //获得对应的群组明细
        List<String> CONSUMER_GRP_LIST = new ArrayList<>();
        String CONSUMER_GROUP = "CONSUMER_GROUP";
        // 描述信息中包含了大量有关群组的信息，包括群组成员、它们的标识符和主机地址、分配给它们的分区、分配分区的算法以及群组协调器的主机地址。在对消费者群组进行故障诊断时，这些描述信息非常有用。
        ConsumerGroupDescription groupDescription = admin.describeConsumerGroups(CONSUMER_GRP_LIST).describedGroups().get(CONSUMER_GROUP).get();
        System.out.println("Description of group " + CONSUMER_GROUP+ ":" + groupDescription);
        // 但这里缺了一个比较重要的信息——我们想要知道消费者群组最后消费的每个分区的偏移量以及相比最新消息滞后了多少

        // 通过client进行获得

        Map<TopicPartition, OffsetAndMetadata> offsets =        admin.listConsumerGroupOffsets(CONSUMER_GROUP)                .partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for(TopicPartition tp: offsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =        admin.listOffsets(requestLatestOffsets).all().get();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e: offsets.entrySet()) {
            String topic = e.getKey().topic();
            int partition =  e.getKey().partition();
            long committedOffset = e.getValue().offset();
            long latestOffset = latestOffsets.get(e.getKey()).offset();




        }

    private static void configManager(AdminClient admin) throws ExecutionException, InterruptedException {
        // 很多应用程序使用了压实的主题，它们会定期（为安全起见，要比默认的保留期限更加频繁一些）检查主题是否被压实，如果没有，就采取相应的行动来纠正主题配置
        String TOPIC_NAME = "TOPIC_NAME";
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME); //ConfigResource有几种类型，这里检查的是主题配置。也可以在同一个请求中指定多个不同类型的资源

        DescribeConfigsResult configsResult = admin.describeConfigs(Collections.singleton(configResource));
        // 每个配置项都有一个isDefault()方法，可以让我们知道哪些配置被修改了。如果用户为主题配置了非默认值，或者修改了 broker 级别的配置，而创建的主题继承了 broker 的非默认配置，那么我们便能知道这个配置不是默认的
        Config configs = configsResult.all().get().get(configResource);// 打印非默认配置
        configs.entries().stream().filter(entry -> !entry.isDefault()).forEach(System.out::println);

        // 检查主题是否被压实ConfigEntry
        ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,TopicConfig.CLEANUP_POLICY_COMPACT); ////每个修改操作都由一个配置条目（配置的名字和值，此处名字是cleanup.policy，值是compacted）和操作类型组成
        if (!configs.entries().contains(compaction)) {    // 如果主题没有被压实，就将其压实
            Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
            configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET)); //为了修改配置，这里指定了需要修改的ConfigEntry和一组操作 ,SET（用于设置值）DELETE（用于删除值并重置为默认值）APPEND和SUBSTRACT。后两种只适用于List类型的配置，用于向列表中添加值或从列表中移除值
            Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
            alterConf.put(configResource, configOp);
            admin.incrementalAlterConfigs(alterConf).all().get();
        } else {
            System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
        }
    }


    private static void topManager(AdminClient admin) throws ExecutionException, InterruptedException {
        //主题管理
        // 1. 获得所有的主题名称,并进行打印
        ListTopicsResult topics = admin.listTopics(); //列出集群的所有主题
        Set<String> strings = topics.names().get();//  topics.name()返回的是一组主题名字的Future对象。当调用Future的get()方法时，执行线程将会等待，直到服务器返回一组主题名字或抛出超时异常
        strings.forEach(System.out::println);//在获得主题名字列表后进行展示

        //2. 校验主题是否存在,如果不存在，就创建一个.
        List<String> TOPIC_LIST = new ArrayList<>();
        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST); // 想要验证的主题名字作为参数。返回DescribeTopicResult对象，这个对象（主题名字到Future的映射）进行了包装成Map
        String TOPIC_NAME = "checkTopicName"; // 单独提取要校验的主题
        int NUM_PARTITIONS = 2; // 存在几个分区
        short REP_FACTOR = 3; // 存在几个副本
        try {// 调用get()得到想要的结果,在这里是一个TopicDescription对象。但服务器也可能无法正确处理请求——如果主题不存在，那么服务器就不会返回我们想要的结果。在这种情况下，服务器将返回一个错误，Future将抛出
            TopicDescription topicDescription = demoTopic.values().get(TOPIC_NAME).get(); // ExecutionException，这个异常是服务器返回的错误导致的
            System.out.println("Description of demo topic:" + topicDescription); // 如果主题存在，Future将返回一个TopicDescription对象，其中包含了主题的所有分区、分区首领所在的 broker、副本清单和同步副本清单。需要注意的是，这个对象并不包含主题的配置信息
            if (topicDescription.partitions().size() != NUM_PARTITIONS) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }
        } catch (ExecutionException e) {  // 对于大部分异常，提前退出
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) { // 检查ExecutionException的嵌套异常才能获取到 Kafka 返回的错误信息
                e.printStackTrace();
                throw e;
            }
            // 如果执行到这里，则说明主题不存在
            System.out.println("Topic " + TOPIC_NAME + " does not exist. Going to create it now");    // 需要注意的是，分区和副本数是可选的
            // 如果没有指定，那么将使用broker的默认配置 ,主题不存在，就创建一个新主题。在创建主题时，可以只指定主题名字，其他参数使用默认值。当然，也可以指定分区数量、副本数量和其他配置参数
            CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));
            // 检查主题是否已创建成功： 等待主题创建完成并验证结果
            if (newTopic.replicationFactor(TOPIC_NAME).get() != REP_FACTOR) {
                System.out.println("Topic has wrong number of REP_FACTOR.");
                System.exit(-1);
            }
        }


        //3. 删除主题
        // 检查主题是否已删除// 需要注意的是，由于删除是异步操作，这个时候主题可能还存在
        admin.deleteTopics(TOPIC_LIST).all().get();//调用deleteTopics，并传给它要删除的主题的名字，然后调用get()等待操作完成
        try {
            TopicDescription topicDescription = demoTopic.values().get(TOPIC_NAME).get();
            System.out.println("Topic " + TOPIC_NAME + " is still around");
        } catch (ExecutionException e) {
            System.out.println("Topic " + TOPIC_NAME + " is gone");
        }

        //4. 你正在向一个需要处理大量管理请求的服务器写入数据。在这种情况下，你并不希望在等待 Kafka 返回响应的同时阻塞服务器线程，而是希望服务器继续接受来自客户端的请求，
        // 然后将请求发送给 Kafka，等 Kafka 返回响应后再将响应发送给客户端。这个时候，KafkaFuture会非常有用

        Vertx vertx = Vertx.vertx(); // 用 Vert.x 创建一个简单的 HTTP 服务器。服务器在收到请求时会调用我们定义的requestHandler
        vertx.createHttpServer().requestHandler(request -> {
            String topic = request.getParam("topic"); // 请求当中包含了一个主题名字，我们将用这个主题的描述信息作为响应
            String timeout = request.getParam("timeout");
            int timeoutMs = NumberUtils.toInt(timeout, 1000);

            // 像往常一样调用AdminClient.describeTopics，并得到一个包装好的Future对象
            DescribeTopicsResult demoTopics = admin.describeTopics(Collections.singletonList(topic), new DescribeTopicsOptions().timeoutMs(timeoutMs));

            KafkaFuture<TopicDescription> topicDescriptionKafkaFuture = demoTopics.values().get(topic);
            topicDescriptionKafkaFuture.whenComplete( // 这里没有调用get()方法，而是构造了一个函数，Future在完成时会调用这个函数
                    new KafkaFuture.BiConsumer<TopicDescription, Throwable>() {
                        @Override
                        public void accept(final TopicDescription topicDescription, final Throwable throwable) {
                            if (throwable != null) { // 如果Future抛出异常，就将错误返回给 HTTP 客户端
                                request.response().end("Error trying to describe topic " + topic + " due to " + throwable.getMessage());
                            } else { // 如果Future顺利完成，就将主题描述信息返回给客户端
                                request.response().end(topicDescription.toString());
                            }
                        }
                    });
        }).listen(8080);
    }
}
