package com.pengnn.version1.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Description 自定义生产者拦截器实现
 * @Author Pengnan
 * @CreateTime 2021年07月20日 20:37:00
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess=0;
    private volatile long sendFailure=0;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String newRecorder = "prefix1-" + record.value();
        return new ProducerRecord(record.topic(),record.key(),newRecorder);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception==null){
            sendSuccess++;//成功
        }else{
            sendFailure++;//失败
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("发送成功率="+(successRatio*100)+"%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
