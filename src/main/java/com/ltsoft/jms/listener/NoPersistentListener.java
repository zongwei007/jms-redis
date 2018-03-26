package com.ltsoft.jms.listener;

import com.ltsoft.jms.JmsConsumerImpl;
import com.ltsoft.jms.JmsContextImpl;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.message.JmsMessageHelper;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.ByteArrayCodec;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ltsoft.jms.util.KeyHelper.getDestinationKey;

/**
 * 监听 Redis 的消息发布
 */
public class NoPersistentListener implements MessageListener<byte[]>, Listener {

    private static final Logger LOGGER = Logger.getLogger(NoPersistentListener.class.getName());

    private final JmsContextImpl context;
    private final String clientID;
    private final javax.jms.MessageListener listener;
    private final Destination destination;
    private final boolean noLocal;
    private RTopic<byte[]> topic;

    public NoPersistentListener(JmsConsumerImpl consumer) {
        this.context = consumer.context();
        this.clientID = context.getClientID();
        this.listener = consumer.getMessageListener();
        this.destination = consumer.getDestination();
        this.noLocal = consumer.isNoLocal();
    }

    @Override
    public void onMessage(String channel, byte[] msg) {
        JmsMessage message = null;
        try {
            message = JmsMessageHelper.fromBytes(msg);
            if (noLocal && Objects.equals(message.getJMSXMessageFrom(), clientID)) {
                return;
            }
        } catch (JMSException e) {
            LOGGER.log(Level.WARNING, "NoPersistentListener can not read message from property", e);
        }

        if (message != null) {
            message.setReadOnly(true);

            listener.onMessage(message);
        }
    }

    @Override
    public void start() {
        RedissonClient client = context.client();
        this.topic = client.getTopic(getDestinationKey(destination), ByteArrayCodec.INSTANCE);

        topic.addListener(this);

        LOGGER.finest(() -> String.format("Client '%s' is listening to '%s'", clientID, destination));
    }

    @Override
    public void stop() {
        if (topic != null) {
            topic.removeListener(this);
        }
        LOGGER.finest(() -> String.format("Client '%s' stop listening to '%s'", clientID, destination));
    }
}
