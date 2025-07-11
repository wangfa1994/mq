package com.wf.a03.a04partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器
 * 若键为Banana则放入最后一个分区，若键不为Banana则散列到其他分区
 * */
public class BananaPartitioner implements Partitioner {

    /**
     * Partitioner 接口包含了 configure、partition 和 close 这 3 个方法。这里只实现
     * partition 方法。不能在 partition 方法里硬编码客户的名字，而应该通过 configure 方法传进
     * 来。
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 分区信息列表
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //分区数量
        int numPartitions = partitions.size();

        if (keyBytes == null || !(key instanceof String)) { // 只接受字符串作为键，如果不是字符串，就抛出异常
            throw new InvalidRecordException("We expect all messages to have customer name as key");
        }

        if (((String)key).equals("Banana")) {
            return numPartitions - 1; // // Banana的记录总是被分配到最后一个分区
        }

        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1)); // // 其他记录被哈希到其他分区

    }

    @Override
    public void close() {

    }
}
