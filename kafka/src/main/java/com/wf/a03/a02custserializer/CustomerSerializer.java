package com.wf.a03.a02custserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 不需要配置什么
    }

    /**
     * Customer对象被序列化成：
     * 表示customerId的4字节整数
     * 表示customerName长度的4字节整数(如果customerName为空，则长度为0)
     * 表示customerName的N字节
     * @param topic topic associated with data
     * @param data typed data
     * @return
     */
    @Override
    public byte[] serialize(String topic, Customer data) {
        try{
            byte[] serializedName;
            int stringSize;
            if(data == null){
                return null;
            }else{
                if(data.getCustomerName() != null){
                    serializedName = data.getCustomerName().getBytes("utf-8");
                    stringSize = serializedName.length;
                } else{
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }
            // 第一个4个字节用于存储Id的值,第二个4个字节用于存储name字节数组的长度int值,第三个长度，用于存放username序列化之后的字节数组
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch(Exception e){
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }


    @Override
    public void close() {
        // 不需要关闭任何
    }
}
