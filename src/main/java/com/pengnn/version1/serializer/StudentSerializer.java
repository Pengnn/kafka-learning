package com.pengnn.version1.serializer;

import com.pengnn.version1.entity.Student;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @Description 自定义序列化器
 * @Author Pengnan
 * @CreateTime 2021年07月19日 19:24:00
 */
public class StudentSerializer implements Serializer<Student> {

    @Override
    public void configure(Map configs, boolean isKey) {

    }


    @Override
    public byte[] serialize(String topic, Student data) {
        if(data==null)return null;
        byte[] name;
        byte[] age=new byte[4];

        if(data.getName()!=null){
            name=data.getName().getBytes(StandardCharsets.UTF_8);
        }else {
            name=new byte[0];
        }
//        age[0]=(byte)(data.getAge()&0xff);
//        age[1]=(byte)((data.getAge()>>8)&0xff);
//        age[2]=(byte)((data.getAge()>>16)&0xff);
//        age[3]=(byte)((data.getAge()>>24)&0xff);

        ByteBuffer buffer = ByteBuffer.allocate(4+name.length+4);//name的长度+name+age
        buffer.putInt(name.length);
        buffer.put(name);
        buffer.putInt(data.getAge());
        return buffer.array();
    }

    @Override
    public void close() {

    }
}
