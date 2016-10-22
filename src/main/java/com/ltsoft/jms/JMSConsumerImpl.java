package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.listener.Listener;
import com.ltsoft.jms.listener.NoPersistentListener;
import com.ltsoft.jms.listener.PersistentListener;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.message.JmsMessageHelper;
import redis.clients.jedis.Jedis;

import javax.jms.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.ltsoft.jms.util.KeyHelper.*;
import static com.ltsoft.jms.util.ThreadPool.scheduledPool;

/**
 * Created by zongw on 2016/9/25.
 */
public class JMSConsumerImpl implements JMSConsumer {

    private static final Duration BACKUP_DURATION = Duration.ofMinutes(1);
    private static final long DELAY = Duration.ofHours(1).getSeconds();

    private final JMSContextImpl context;
    private final Destination destination;
    private final boolean noLocal;
    private final boolean durable;
    private final boolean shared;

    private MessageListener messageListener;
    private String subscriptionName;

    private Listener listener;

    private boolean started = false;

    private ScheduledFuture<?> pingThread;

    public JMSConsumerImpl(JMSContextImpl context, Destination destination, boolean noLocal, boolean durable, boolean shared) {
        this.context = context;
        this.destination = destination;
        this.noLocal = noLocal;
        this.durable = durable;
        this.shared = shared;
    }

    public Destination getDestination() {
        return destination;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    @Override
    public String getMessageSelector() {
        return null;
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        this.messageListener = listener;

        if (durable) {
            this.listener = new PersistentListener(this);
        } else {
            this.listener = new NoPersistentListener(context, this);
        }

        if (context.getAutoStart()) {
            this.listener.start();
        }
    }

    private String getMessageListKey() {
        String key = null;
        if (destination instanceof Queue) {
            key = getDestinationKey(destination);
        } else if (destination instanceof Topic) {
            key = getTopicConsumerListKey(destination, context.getClientID());
        }
        return key;
    }

    private Message readMessage(String messageId, Supplier<Message> another) {

        byte[] itemKey = getDestinationPropsKey(destination, messageId);
        try (Jedis client = context.pool().getResource()) {
            Map<String, byte[]> props = JmsMessageHelper.toStringKey(client.hgetAll(itemKey));
            if (props == null) {
                //消息有可能已过期
                client.close();
                return another.get();
            }

            JmsMessage message = JmsMessageHelper.fromMap(props);
            message.setJMSMessageID(messageId);

            if (durable) {
                message.setAcknowledgeCallback(new JmsAcknowledgeCallback(context));
            }

            if (noLocal && Objects.equals(message.getJMSXMessageFrom(), context.getClientID())) {
                //仅处理非本机消息
                client.close();
                message.acknowledge();
                return another.get();
            }

            byte[] body = client.get(getDestinationBodyKey(destination, messageId));
            if (body != null) {
                message.setBody(body);
            }

            message.setReadOnly(true);

            if (JMSContext.AUTO_ACKNOWLEDGE == context.getSessionMode()
                    || DeliveryMode.NON_PERSISTENT == message.getJMSDeliveryMode()) {
                message.acknowledge();
            }

            return message;
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    @Override
    public Message receive() {
        return receive(0);
    }

    @Override
    public Message receive(long timeout) {
        String key = getMessageListKey();
        String backupKey = getDestinationBackupKey(destination, context.getClientID(), BACKUP_DURATION);

        try (Jedis client = context.pool().getResource()) {
            return readMessage(client.brpoplpush(key, backupKey, (int) timeout), () -> receive(timeout));
        }
    }

    @Override
    public Message receiveNoWait() {
        String key = getMessageListKey();
        String backupKey = getDestinationBackupKey(destination, context.getClientID(), BACKUP_DURATION);

        try (Jedis client = context.pool().getResource()) {
            String messageId = client.rpoplpush(key, backupKey);
            if (messageId != null) {
                return readMessage(messageId, this::receiveNoWait);
            }
        }
        return null;
    }

    /**
     * 更新 Redis 中的消费者过期时间
     */
    private void ping() {
        try (Jedis client = context.pool().getResource()) {
            client.zadd(getTopicConsumersKey(destination), Instant.now().getEpochSecond(), context.getClientID());
        }
    }

    /**
     * 在 Redis 中注册消费者，并定时更新过期时间
     */
    private void register() {
        if (durable) {
            this.pingThread = scheduledPool().scheduleWithFixedDelay(this::ping, 0, DELAY, TimeUnit.SECONDS);
        }
    }

    /**
     * 从 Redis 中注销消费者
     */
    private void unregistered() {
        try (Jedis client = context.pool().getResource()) {
            if (pingThread != null) {
                client.zrem(getTopicConsumersKey(destination), context.getClientID());
                pingThread.cancel(true);
            }
        }
    }

    void start() {
        if (!started) {
            if (listener != null) {
                listener.start();
            }
            register();
            this.started = true;
        }
    }

    @Override
    public void close() {
        if (started) {
            if (listener != null) {
                listener.stop();
            }
            unregistered();
            this.started = false;
        }
    }

    private <T> T receiveBody(Class<T> c, Message message) {
        try {
            if (message != null) {
                return message.getBody(c);
            }
            return null;
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        return receiveBody(c, receive());
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        return receiveBody(c, receive(timeout));
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        return receiveBody(c, receiveNoWait());
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public JMSConsumerImpl setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }
}
