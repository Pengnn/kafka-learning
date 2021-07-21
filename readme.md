# kafka概念

## 作用和特点

Kafka 是**多分区、多副本且基于 ZooKeeper 协调的分布式消息系统**，目前 Kafka 已经定位为一个**分布式流式处理平台**，它以**高吞吐、可持久化、可水平扩展、支持流数据处理**等多种特性而被广泛使用。

==kafka的作用：==

- **消息系统：** Kafka 和传统的消息系统（也称作消息中间件）都具备`系统解耦、冗余存储、流量削峰、缓冲、异步通信、扩展性、可恢复性`等功能。与此同时，Kafka 还提供了大多数消息系统难以实现的**消息顺序性保障及回溯消费**的功能。
- **存储系统：** Kafka 把消息持久化到`磁盘`，相比于其他基于内存存储的系统而言，有效地降低了数据丢失的风险。也正是得益于 Kafka 的`消息持久化功能`和`多副本机制`，我们可以把 Kafka 作为`长期的数据存储系统`来使用，只需要`把对应的数据保留策略设置为“永久”`或`启用主题的日志压缩功能`即可。
- **流式处理平台：** Kafka 不仅为每个流行的`流式处理框架`提供了可靠的数据来源，还提供了一个完整的流式处理类库，比如窗口、连接、变换和聚合等各类操作

## 组成

一个典型的 Kafka 体系架构包括：

- 若干 Producer
  - `Producer`：生产者，也就是发送消息的一方，负责创建消息，然后将其投递到Kafka中。
- 若干 Broker
  - `broker`:服务代理节点，可以简单地看做一个独立的kafka服务节点或者kafka服务实例，一个或者多个broker组成一个kafka集群。
- 若干 Consumer
  - `Consumer`:消费者，就是接收消息的一方，负责连接到Kafka上并接收消息，进而进行相应的业务逻辑处理。
- 以及一个 ZooKeeper 集群

其中 ZooKeeper 是 Kafka 用来负责`集群元数据的管理、控制器的选举`等操作的。Producer 将消息发送到 Broker，Broker 负责将收到的消息存储到磁盘中，而 Consumer 负责从 Broker 订阅并消费消息。

- `Topic`：主题，kafka的消息以主题为单位进行归类，生产者负责将消息发给特定的主题（发送给kafka的每一条消息都要指定主题），而消费者负责订阅主题并且进行消费。一个主题可以跨越多个`broker`。
- `Partition`:分区，主题可以细分多个分区，一个分区只属于一个主题，同一个主题下的不同分区包含的消息是不同的，分区在存储层面可以看做是一个可追加的日志文件（Log），消息在被追加到分区日志文件的时候都会分配一个特定的偏移量（`offset`）。
- `offset`:偏移量，是消息在分区内的唯一标识，kafka通过它来保证消息在分区内的顺序性，offset并不跨越分区，也就是说kafka保证的是分区有序而不是主题有序。
- `Relpica`：多副本机制，Kafka为分区引入了多副本机制，通过增加副本数量可以`提升容灾能力`。
  - 同一分区的不同副本中保存的是相同的消息（在同一时刻，副本之间并非完全一样），副本之间是`“一主多从”`的关系，其中 **leader 副本负责处理读写请求，follower 副本只负责与 leader 副本的消息同步。副本处于不同的 broker 中，当 leader 副本出现故障时，从 follower 副本中重新选举新的 leader 副本对外提供服务。Kafka 通过`多副本机制`实现了`故障的自动转移`，当 Kafka 集群中某个 broker 失效时仍然能保证服务可用。**
  - `副本处于不同的broker中`

![图1-1 Kafka体系结构](https://test-bucket-1306185041.cos.ap-shanghai.myqcloud.com/img/20210718110938)

## 多副本机制

分区中的所有副本统称为` AR（Assigned Replicas）`。所有与 leader 副本保持一定程度同步的副本（包括 leader 副本在内）组成`ISR（In-Sync Replicas）`，ISR 集合是 AR 集合中的一个子集。消息会先发送到 leader 副本，然后 follower 副本才能从 leader 副本中`拉取消息进行同步`，同步期间内 follower 副本相对于 leader 副本而言会有一定程度的滞后。ISR所说的`“一定程度的同步”`是指可忍受的滞后范围，这个范围可以通过参数进行配置。与 leader 副本同步滞后过多的副本（不包括 leader 副本）组成 `OSR（Out-of-Sync Replicas）`，由此可见，`AR=ISR+OSR`。在正常情况下，所有的 follower 副本都应该与 leader 副本保持一定程度的同步，即 AR=ISR，OSR 集合为空。

**`leader` 副本负责维护和跟踪 ISR 集合中所有 follower 副本的滞后状态**，当 follower 副本落后太多或失效时，leader 副本会把它从 ISR 集合中剔除。如果 OSR 集合中有 follower 副本“追上”了 leader 副本，那么 leader 副本会把它从 OSR 集合转移至 ISR 集合。默认情况下，当 leader 副本发生故障时，只有在 ISR 集合中的副本才有资格被选举为新的 leader，而在 OSR 集合中的副本则没有任何机会（不过这个原则也可以通过修改相应的参数配置来改变）。

ISR 与 HW 和 LEO 也有紧密的关系:

- `HW`: High Watermark ，俗称高水位，标识了一个特定的消息偏移量（offset），消费者只能拉取到这个 offset 之前的消息。

- `LEO`:Log End Offset ,标识当前日志文件中`下一条待写入消息的 offset`，LEO 的大小相当于当前日志分区中最后一条消息的 offset 值加1。**分区 ISR 集合中的每个副本都会维护自身的 LEO，而 ISR 集合中最小的 LEO 即为分区的 HW，对消费者而言只能消费 HW 之前的消息。**

  在消息写入 leader 副本之后，follower 副本会发送拉取请求来拉取消息3和消息4以进行消息同步。

  ![图1-7 写入消息（情形3）](https://test-bucket-1306185041.cos.ap-shanghai.myqcloud.com/ img/20210718114324)

  在同步过程中，不同的 follower 副本的同步效率也不尽相同。如上图所示，在某一时刻 follower1 完全跟上了 leader 副本而 follower2 只同步了消息3，如此 leader 副本的 LEO 为5，follower1 的 LEO 为5，follower2 的 LEO 为4，那么当前分区的 HW 取最小值4，此时消费者可以消费到 offset 为0至3之间的消息。

Kafka 的复制机制既不是完全的同步复制，也不是单纯的异步复制。事实上，同步复制要求所有能工作的 follower 副本都复制完，这条消息才会被确认为已成功提交，这种复制方式极大地影响了性能。而在异步复制方式下，follower 副本异步地从 leader 副本中复制数据，数据只要被 leader 副本写入就被认为已经成功提交。在这种情况下，如果 follower 副本都还没有复制完而落后于 leader 副本，突然 leader 副本宕机，则会造成数据丢失。Kafka 使用的这种 ISR 的方式则有效地权衡了数据可靠性和性能之间的关系。具体看ack怎么设置，设置0立即返回，性能最好，可靠性最差；设置为1，leader回应就返回，设置为all，所有follower返回才算成功，可靠性最好，性能最差。一般会设置为1，默认的ack就是1，所以说kafka用这种方式权衡可靠性和性能。

## zookeeper

ZooKeeper 是安装 Kafka 集群的必要组件，**Kafka 通过 ZooKeeper 来实施对元数据信息的管理，包括集群、broker、主题、分区等内容。**

分布式应用程序可以基于 ZooKeeper 实现诸如数据发布/订阅、负载均衡、命名服务、分布式协调/通知、集群管理、Master 选举、配置维护等功能。

在 ZooKeeper 中共有3个角色：`leader、follower 和 observer`，同一时刻 ZooKeeper 集群中只会有一个 leader，其他的都是 follower 和 observer。observer 不参与投票，默认情况下 ZooKeeper 中只有 leader 和 follower 两个角色。

### 命令行

创建副本的时候，副本因子应该小于等于可用的broker数。

==创建主题==

`--zookeeper`指定了kafka连接的zookeeper的服务器地址

`-create`:创建主题的指令

`--topic`：主题

`--replication-factor`:副本因子，就是副本的个数

`--partitions`:分区数

创建主题topic-demo，副本因子是1，分区数是4：（副本因子的个数不能大于可用的broker）

`bin\windows\kafka-topics.bat --zookeeper localhost: 2181\kafka --create --topic topic-demo --replication-factor 1 --partitions 4` 

==查看主题的详细信息==

`bin\windows\kafka-topics.bat --zookeeper localhost: 2181\kafka --describe --topic topic-demo`

# 生产者消费者实例

## 生产者实例

要往 Kafka 中写入消息，首先要创建一个生产者客户端实例并设置一些配置参数，然后构建消息的 `ProducerRecord`对象，其中必须包含所要发往的`主题及消息的消息体`，进而再通过`生产者客户端实例`将消息发出，最后可以通过` close()`方法来关闭生产者客户端实例并回收相应的资源。

## 消费者实例

首先创建一个消费者客户端实例并配置相应的参数，然后订阅主题并消费。

### #》》问题：不同消费者客户端收到的消息是不一样的

它们的合集是总的消息。

**生产者**

```java
package com.pengnn;

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
```

**消费者**

```java
package com.pengnn;

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
```

# 生产者客户端开发

1. 配置生产者参数：序列化、kafka的地址
2. 创建生产者实例
3. 设置主题和消息内容
4. 发送消息
5. 关闭生产者实例

## 生产者参数配置

- `bootstrap.servers`：指定生产者客户端连接kafka集群的broker地址清单，可以设置多个地址中间以逗号隔开：`host1:port1,host2:port2`。并非需要所有的broker地址，因为生产者会从给定的broker查看到其它broker的信息，不过建议至少设置两个以上，因为当其中一个宕机后其它的broker仍然可以连接到kafka集群上。
- `key.serializer`、`value.serializer`，因为broker接收消息必须是字节数组，所以生产者发送消息前必须序列化，如果Key和value的类型是String,那么对应的序列化器必须是全类名的：`org,apache.kafka.common.serialization.StringSerializer`。

**简洁版：**

```java
Properties properties = new Properties();
properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
```

**关于线程安全：**

- KafkaProducer是线程安全的，可以在多个线程中共享的单个`KafkaProducer`实例
- 也可以把`KafkaProducer`实例进行池化来供线程调用

## 构建消息

`ProducerRecord`类的成员变量：

```
public class ProducerRecord<K, V> {
    private final String topic; //主题【必填】
    private final Integer partition; //分区号：消息要发送的分区号
    private final Headers headers; //消息头部
    private final K key; //键：可以计算分区号，让消息发送到特定的分区，同一个Key的消息会发送到同一个分区
    private final V value; //值【必填】
    private final Long timestamp; //消息的时间戳
    //省略其他成员方法和构造方法
}
```

- topic 和 partition 字段分别代表消息要发往的主题和分区号
- headers 字段是消息的头部，Kafka 0.11.x 版本才引入这个属性，它大多用来设定一些与应用相关的信息，如无需要也可以不用设置。
- key 是用来指定消息的键，它不仅是消息的附加信息，还可以用来计算分区号进而可以让消息发往特定的分区。同一个 key 的消息会被划分到同一个分区中
- 有 key 的消息还可以支持日志压缩的功能。value 是指消息体，一般不为空，如果为空则表示特定的消息—墓碑消息。timestamp 是指消息的时间戳，它有 CreateTime 和 LogAppendTime 两种类型，前者表示消息创建的时间，后者表示消息追加到日志文件的时间。

ProducerRecord对应多种构造方法：

```java
public ProducerRecord(String topic, Integer partition, Long timestamp,K key, V value, Iterable<Header> headers)
public ProducerRecord(String topic, Integer partition, Long timestamp,K key, V value)
public ProducerRecord(String topic, Integer partition, K key, V value,Iterable<Header> headers)
public ProducerRecord(String topic, Integer partition, K key, V value)
public ProducerRecord(String topic, K key, V value)
public ProducerRecord(String topic, V value)//【常用】
```

## 发送消息

创建生产者实例和构建消息之后，就可以开始发送消息了。发送消息主要有三种模式：

- 发后即忘（fire-and-forget）
- 同步（sync）
- 异步（async）

### **”发后即忘“方式：fire-and-forget**

就是直接调用`send(record)`方法，只管向Kafka发送消息而不管是否正确到达。一般情况下没什么问题，但是如果发生`不可重试异常`时会造成`消息丢失`。性能最高，可靠性最差。

### 同步方式：sync

其实`send(record)`的返回值是`Future`类型，可以调用它的`get()`方法来阻塞等待kafka的响应直到消息发送成功，或者发生异常。同步发送的方式可靠性高，要么消息被发送成功，要么发生异常。如果发生异常，则可以捕获并进行相应的处理，而不会像“发后即忘”的方式直接造成消息的丢失。不过同步发送的方式的性能会差很多，需要阻塞等待一条消息发送完之后才能发送下一条。

send() 方法之后调用 get() 方法可以获取一个` RecordMetadata `对象，在 RecordMetadata 对象里包含了消息的一些元数据信息，比如当前消息的主题、分区号、分区中的偏移量（offset）、时间戳等。如果在应用代码中需要这些信息，则可以使用这个方式。如果不需要不用接收返回值：producer.send(record).get() 的方式更省事。

```
try {
    Future<RecordMetadata> future = producer.send(record);
    RecordMetadata metadata = future.get();
    System.out.println(metadata.topic() + "-" +
            metadata.partition() + ":" + metadata.offset());
} catch (ExecutionException | InterruptedException e) {
    e.printStackTrace();
}
```

**==异常：==**

- **可重试异常：**比如 `NetworkException` 表示网络异常，可能是由于网络瞬时故障而导致的异常，可以通过重试解决；又比如` LeaderNotAvailableException` 表示分区的 leader 副本不可用，这个异常通常发生在 leader 副本下线而新的 leader 副本选举完成之前，重试之后可以重新恢复。

- **不可重试异常：**`RecordTooLargeException` 异常，暗示了所发送的消息太大，KafkaProducer 对此不会进行任何重试，直接抛出异常。

- **retries配置：**对于`可重试的异常`，如果配置了` retries 参数`，那么只要在规定的重试次数内自行恢复了，就不会抛出异常。retries 参数的默认值为0，配置方式参考如下：

  ```
  props.put(ProducerConfig.RETRIES_CONFIG, 10);
  ```

  如果重试了10次之后还没有恢复，那么仍会抛出异常，进而发送的外层逻辑就要处理这些异常了。

### 异步方式：async

一般是在 send() 方法里指定一个 `Callback 的回调函数`，Kafka 在返回响应时调用该函数来实现异步的发送确认。Kafka 有响应时就会回调，要么发送成功，要么抛出异常。

```java
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
```

- 回调函数的两个参数：`RecordMetadata`和`Exception`是互斥的，就是说有一个为null另一个不为null,根据判断结果执行响应的操作。

- 回调函数的调用也可以保证分区有序，比如

  ```java
  producer.send(record1, callback1); 
  producer.send(record2, callback2);
  ```

  对于同一个分区而言，如果消息 record1 于 record2 之前先发送，那么 KafkaProducer 就可以保证对应的 callback1 在 callback2 之前调用。

## 关闭

close() 方法会`阻塞等待`之前所有的发送请求完成后再关闭 KafkaProducer。与此同时，KafkaProducer 还提供了一个带超时时间的 close() 方法。如果调用了带超时时间 timeout 的 close() 方法，那么只会在等待 timeout 时间内来完成所有尚未完成的请求处理，然后强行退出。

# 序列化-分区器-拦截器

## 序列化

生产者需要序列化器`Serializer`把对象转化为字节数组通过网络发送给kafka，而消费者需要反序列器`Deserializer`把从kafka接收到的字节数组反序列化为响应的对象。序列化有多种方式：String类型、ByteArray、ByteBuffer、Bytes、Double、Integer、Long 等，它们都实现了`org.apache.kafka.common.serialization.Serializer`接口，此接口包含三个方法：

```java
public void configure(Map<String, ?> configs, boolean isKey)
public byte[] serialize(String topic, T data)
public void close()
```

1. `configure()`:配置当前类
2. `serialize()`:执行序列化操作
3. `close()`:关闭序列化器，一般情况下是空的，如果实现了这个方法必须保证幂等性。

以`Stringerializer`为例，`configuration()`配置了编码方式是UTF-8，`serialize()`序列化处理是把字符串转化为字节数组`return data.getBytes(encoding);`,`closer()`为空。

**自定义序列化器：**

```java
package com.pengnn.version1.entity;

/**
 * @Description 实体类
 * @Author Pengnan
 * @CreateTime 2021年07月19日 19:22:00
 */
public class Student {
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}

```



```java
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
```

## 分区器

如果消息`ProducerRecord`没有配置分区`Partition`，那么就要依赖`分区器`，根据key计算分区。

默认的分区器`org.apache.kafka.clients.producer.internals.DefaultPartitioner`,如果key不是null，根据Key取哈希后区分，具体逻辑：

```java
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster,int numPartitions) {
        if (keyBytes == null) {//如果 key 为 null，生成一个随机数与分区数取模定位分区，如果有可利用分区就在可用分区中定位，如果没有可用分区就在所有分区中找。
            return stickyPartitionCache.partition(topic, cluster);
        }
        // 如果Key不是Null,使用murmurHash2哈希算法计算分区
      //相同key的消息会被写入相同的分区
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
```

如果 key 不为 null，那么计算得到的分区号会是所有分区中的任意一个；如果 key 为 null 并且有可用分区时，那么计算得到的分区号仅为可用分区中的任意一个，

`stickyPartitionCache`中partitipon的逻辑是：生成一个随机数与分区数取模定位分区，如果有可利用分区就在可用分区中定位，如果没有可用分区就在所有分区中找。

```java
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
粘性分区行为	缓存
实现用于粘性分区行为的缓存的内部类。
缓存跟踪任何给定主题的当前粘性分区。
不应在外部使用此类。

 */
public class StickyPartitionCache {
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
		//如果part！=null，返回当前主题对应的缓存中的分区，每次都返回一样的结果，所以是“sticky partition”
		
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }
	//总的来说，如果key=null,
//检查主题的当前粘性分区是否未设置，或者触发新批处理的分区是否与需要更改的粘性分区匹配。
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
      
        if (oldPart == null || oldPart == prevPartition) {
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() < 1) {//如果没有可用分区
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();//生成一个随机数与分区数取模计算分区
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition;//如果可用分区只有一个，就分配到这一个可用分区
            } else {//如果可用分区有多个，就在可用分区中找，也是生成一个随机数然后取模计算的
                while (newPart == null || newPart.equals(oldPart)) {
                    int random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            //如果粘性分区为空或 prevPartition 与当前粘性分区匹配，则仅更改粘性分区。
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }
}
```

可用分区是根据领导者的可用性划分的，如果不存在领导者就是不可用分区。

## 拦截器

> 生产者拦截器的作用：`消息发送之前`做一些准备工作，如按照某个规则过滤不符合规则的消息，或者修改消息的内容；也可以在`发送回调逻辑前`处理一些定制性的需求，比如统计类工作。

### ProducerInterceptor 接口

主要是通过实现`org.apache.kafka.clients.producer. ProducerInterceptor `接口。

- `ProducerInterceptor`接口的作用和特点
  - **作用：**将生产者收到的记录发布到 Kafka 集群之前拦截（并可能改变）它们。
  - **共享配置命名空间：**拦截器实现需与其他拦截器和序列化器共享生产者配置命名空间，需要确保没有冲突。
  - **异常不会抛出只会记录：**ProducerInterceptor 方法抛出的异常将被捕获、记录，但不会进一步向上传播。也就是说如果用户使用错误的键和值类型参数配置拦截器，生产者不会抛出异常，只是记录错误。
  - **多线程：**ProducerInterceptor 回调可以从多个线程调用。拦截器实现必须确保线程安全。
- `ProducerInterceptor`接口包含的方法：
  1. ` public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);`
     - 在消息被序列化并分配分区之前（如果在 ProducerRecord 中未指定分区）调用，返回修改后的消息
     - 如果修改了key会影响到分区的计算以及broker端日志压缩功能
     - 此方法抛出的任何异常都将被调用者捕获并记录下来，但不会进一步向上传播。
     - 如果运行多个拦截器,`onSend()`将按照`ProducerConfig.INTERCEPTOR_CLASSES_CONFIG`  指定的顺序调用,配置的时候用逗号隔开
     - 列表中的第一个拦截器获取客户端传递过来的记录，后面的拦截器会传递上一个拦截器返回的记录，以此类推。不鼓励构建依赖于前一个拦截器输出的可变拦截器链，因为如果前一个拦截器由于异常而执行失败，那么这个拦截器也就跟着无法继续执行。
     - 在拦截链中，如果某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行。
  2. `public void onAcknowledgement(RecordMetadata metadata, Exception exception);`
     - **调用时机：**当发送到服务器的记录已被确认时，或者当发送记录在发送到服务器之前失败时，将调用此方法。优先于用户回调Callback 之前调用，在 KafkaProducer.send() 引发异常的情况下调用。
     - **会影响其它线程的发送速度：**此方法一般会在后台 I/O 线程中执行，因此实现应该尽量快。否则，来自其他线程的消息发送可能会延迟。
     - **参数**：`metadata `– 已发送记录的元数据（即分区和偏移量）。如果发生错误，元数据将只包含有效的主题和分区。如果 ProducerRecord 中没有给出 partition 并且在分配 partition 之前发生错误，则 partition 将设置为 `RecordMetadata.NO_PARTITION`。如果客户端将空记录传递给 KafkaProducer.send(ProducerRecord)，则`元数据可能为空`。`Exception` – 在处理此记录期间抛出的异常。如果没有发生错误，则为空。
  3. ` configure()` 方法获取生产者配置属性，如果生产者配置中未指定，则包括由 KafkaProducer 分配的 clientId。
  4. `public void close();`主要用于在关闭拦截器时执行一些资源的清理工作。

### 自定义生产者拦截器的实现

```java
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
```



