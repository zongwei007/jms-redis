# JMS-Redis
基于 Redis 实现的实验用 Java 消息服务，仅支持 JMS 2.0 的部分特性

## 支持特性

* 支持 JMS 的基本消息类型
* 支持 AUTO_ACKNOWLEDGE、CLIENT_ACKNOWLEDGE、DUPS_OK_ACKNOWLEDGE 三种 SessionMode
* 非持久化的消息通过 Jedis 的 publish API 发送
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
import redis.clients.jedis.JedisPool;
import com.ltsoft.jms.JmsConnectionFactory;

import javax.jms.*;

public class Example {

    public static void main(String[] args) throws Exception {
        JedisPool jedisPool = new JedisPool();
        ConnectionFactory factory = new JmsConnectionFactory(jedisPool, "ClientId");

        try (JMSContext context = factory.createContext()) {
            Queue queue = context.createQueue("Queue");
            JMSProducer producer = context.createProducer();
            producer.send(context.createQueue("Queue"), "Queue Message");

            JMSConsumer consumer = context.createConsumer(queue);
            Message message = consumer.receiveNoWait();
            System.out.println(message.getBody(String.class));
        }
        
        jedisPool.close();
        System.exit(0);
    }
}
```
