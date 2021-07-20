package com.pengnn.version0;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Description Kafka消息 消费者
 * @Author Pengnan
 * @CreateTime 2021年07月18日 20:49:00
 */
/*
1、创建消费者实例，需要指定key、value的序列化方式，kafka的地址
2、订阅主题 subscribe
3、消费消息  poll
 */
public class Consumer {
    private static final String brokerList="localhost:9092";
    private static final String topic="topic-demo";
    private static final String groupId="group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers",brokerList);
        properties.put("group.id",groupId);
        //①创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //②订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        //③消费消息
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.key()+":"+record.value());
            }
        }

    }


}
