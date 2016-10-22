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

    private JMSContextImpl context;

    public JmsAcknowledgeCallback(JMSContextImpl context) {
        this.context = context;
    }

    @Override
    public void accept(JmsMessage message) {
        try (Jedis jedis = context.pool().getResource()) {
            byte[] itemKey = getDestinationPropsKey(message.getJMSDestination(), message.getJMSMessageID());
            Pipeline pipe = jedis.pipelined();
            if (message.getJMSDestination() instanceof Topic) {
                String itemConsumersKey = getTopicItemConsumersKey(message.getJMSDestination(), message.getJMSMessageID());

                jedis.srem(itemConsumersKey, context.getClientID());
                long len = jedis.scard(itemConsumersKey);
                if (len > 0) {
                    pipe.clear();
                    return;
                }

                pipe.del(itemConsumersKey);
            }

            pipe.del(itemKey);
            pipe.del(getDestinationBodyKey(message.getJMSDestination(), message.getJMSMessageID()));
            pipe.sync();
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }
}
