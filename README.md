# JMS-Redis
基于 Redis 实现的实验用 Java 消息服务，仅支持 JMS 2.0 的部分特性。

## 支持特性

* 支持 JMS 的基本消息类型
* 支持 AUTO_ACKNOWLEDGE、CLIENT_ACKNOWLEDGE、DUPS_OK_ACKNOWLEDGE 三种 SessionMode
* 非持久化的消息通过 Redis 的 publish 特性发送
* 所有的 Queue 和 Topic 都可以被多个消费者监听，不支持互斥消费行为
* 所以 createSharedConsumer 的 API 用于创建监听非持久化消息的消息消费者了

## 不支持特性

* 不支持 JMS 1.x 消息发送/接收相关的 API
* 不支持事务
* 不支持设置 DisableMessageID，消息默认会生成一个消息ID，格式为`ID:Base64压缩后的UUID`
* 不支持 MessageSelector
* 暂不支持优先级队列
* 暂不支持延迟发送 (DeliveryDelay)

## 使用方式

```java
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import com.ltsoft.jms.JmsConnectionFactory;

import javax.jms.*;

public class Example {

    public static void main(String[] args) throws Exception {
        RedissonClient client = Redisson.create();
        ConnectionFactory factory = new JmsConnectionFactory(client, "ClientId");

        try (JMSContext context = factory.createContext()) {
            Queue queue = context.createQueue("Queue");
            JMSProducer producer = context.createProducer();
            producer.send(queue, "Queue Message");

            JMSConsumer consumer = context.createConsumer(queue);
            Message message = consumer.receiveNoWait();
            System.out.println(message.getBody(String.class));
        }
        
        client.shutdown();
        System.exit(0);
    }
}
```

## 关于序列化

默认使用 Java 自带的序列化工具实现序列化支持，并允许使用 Java 的 SPI 机制进行扩展。如果需要替换默认实现，通过 SPI 扩展 `com.ltsoft.jms.util.Serializer` 接口即可。
