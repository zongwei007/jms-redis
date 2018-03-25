package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;

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
        RedissonClient client = context.client();

        try {
            String messageId = message.getJMSMessageID();
            String propsKey = getDestinationPropsKey(message.getJMSDestination(), messageId);
            String bodyKey = getDestinationBodyKey(message.getJMSDestination(), messageId);

            RBatch batch = client.createBatch().atomic();
            if (message.getJMSDestination() instanceof Topic) {
                String itemConsumersKey = getTopicItemConsumersKey(message.getJMSDestination(), messageId);

                client.getSet(itemConsumersKey).remove(context.getClientID());
                long len = client.getSet(itemConsumersKey).size();
                if (len > 0) {
                    consumer.consume(message);

                    LOGGER.finest(() -> String.format(
                            "Message '%s' of client '%s' in '%s' is consumed",
                            messageId, context.getClientID(), consumer.getDestination()
                    ));
                    return;
                }

                batch.getKeys().deleteAsync(itemConsumersKey);
            }

            batch.getKeys().deleteAsync(propsKey, bodyKey);
            batch.execute();

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
