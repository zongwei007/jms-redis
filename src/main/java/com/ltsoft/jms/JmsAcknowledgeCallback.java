package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.jms.JMSException;
import javax.jms.Topic;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * 消息消费回调
 */
public class JmsAcknowledgeCallback implements Consumer<JmsMessage> {

    private static final Logger LOGGER = Logger.getLogger(JmsAcknowledgeCallback.class.getName());

    private final JmsConsumerImpl consumer;
    private final JmsContextImpl context;

    public JmsAcknowledgeCallback(JmsConsumerImpl consumer) {
        this.consumer = consumer;
        this.context = consumer.context();
    }

    @Override
    public void accept(JmsMessage message) {
        try (Jedis client = context.pool().getResource()) {
            String messageId = message.getJMSMessageID();

            byte[] propsKey = getDestinationPropsKey(message.getJMSDestination(), messageId);
            Pipeline pipe = client.pipelined();
            if (message.getJMSDestination() instanceof Topic) {
                String itemConsumersKey = getTopicItemConsumersKey(message.getJMSDestination(), messageId);

                client.srem(itemConsumersKey, context.getClientID());
                long len = client.scard(itemConsumersKey);
                if (len > 0) {
                    consumer.consume(message);

                    LOGGER.finest(() -> String.format(
                            "Message '%s' of client '%s' in '%s' is consumed",
                            messageId, context.getClientID(), consumer.getDestination()
                    ));
                    return;
                }

                pipe.del(itemConsumersKey);
            }

            pipe.del(propsKey);
            pipe.del(getDestinationBodyKey(message.getJMSDestination(), messageId));
            pipe.sync();

            //从消息消费列表中移除
            consumer.consume(message);

            LOGGER.finest(() -> String.format(
                    "Message '%s' of client '%s' in '%s' is consumed",
                    messageId, context.getClientID(), consumer.getDestination()
            ));
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }
}
