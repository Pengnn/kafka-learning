package com.pengnn.version1;

import com.pengnn.version1.Interceptor.ProducerInterceptorPrefix;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Description kafka消息 生产者——改进版
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

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG,10);//配置重试次数，解决可重试异常

        //①创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
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
                producer.send(record, new Callback() {//异步方法
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {//两个参数是互斥的
                        if(e!=null){
                            e.printStackTrace();
                        }else{
                            System.out.println(recordMetadata.topic()+"-"+recordMetadata.partition()+
                                    ":"+recordMetadata.offset());
                        }
                    }
                });
//                producer.send(record).get();//同步方式
//                producer.send(record);//“发后即忘”方式
            }
            break;
        }
        //④关闭
        producer.close();
    }
}
