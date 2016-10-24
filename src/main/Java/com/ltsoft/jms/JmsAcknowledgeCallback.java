package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.jms.JMSException;
import javax.jms.Topic;
import java.util.function.Consumer;

import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * Created by zongw on 2016/10/12.
 */
public class JmsAcknowledgeCallback implements Consumer<JmsMessage> {

    private final JMSConsumerImpl consumer;
    private final JMSContextImpl context;

    public JmsAcknowledgeCallback(JMSConsumerImpl consumer) {
        this.consumer = consumer;
        this.context = consumer.context();
    }

    @Override
    public void accept(JmsMessage message) {
        try (Jedis client = context.pool().getResource()) {
            byte[] propsKey = getDestinationPropsKey(message.getJMSDestination(), message.getJMSMessageID());
            Pipeline pipe = client.pipelined();
            if (message.getJMSDestination() instanceof Topic) {
                String itemConsumersKey = getTopicItemConsumersKey(message.getJMSDestination(), message.getJMSMessageID());

                client.srem(itemConsumersKey, context.getClientID());
                long len = client.scard(itemConsumersKey);
                if (len > 0) {
                    consumer.consume(message);
                    return;
                }

                pipe.del(itemConsumersKey);
            }

            pipe.del(propsKey);
            pipe.del(getDestinationBodyKey(message.getJMSDestination(), message.getJMSMessageID()));
            pipe.sync();

            //从消息消费列表中移除
            consumer.consume(message);
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }
}
