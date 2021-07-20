package com.pengnn.version0;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @Description kafka消息 生产者
 * @Author Pengnan
 * @CreateTime 2021年07月18日 20:49:00
 */

/*
 1、创建生产者实例，需要指定key、value的序列化方式，kafka的地址
 2、指定主题和消息内容 ProducerRecord
 3、发送消息 send
 4、关闭生产者
 */
public class Producer {
    private static final String brokerList="localhost:9092";
    private static final String topic="topic-demo";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers",brokerList);
        //①创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //②指定主题和消息内容
        while(true){
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line=null;
            while((line=reader.readLine())!=null){
                if("quit".equals(line)){
                    break;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                //③发送消息
                producer.send(record);
            }

        }

//        //④关闭
//        producer.close();
    }
}
