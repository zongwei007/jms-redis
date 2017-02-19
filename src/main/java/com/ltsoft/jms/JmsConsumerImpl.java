package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.listener.Listener;
import com.ltsoft.jms.listener.NoPersistentListener;
import com.ltsoft.jms.listener.PersistentListener;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.message.JmsMessageHelper;
import redis.clients.jedis.Jedis;

import javax.jms.*;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * 消费者实现
 */
public class JmsConsumerImpl implements JMSConsumer {

    private static final Logger LOGGER = Logger.getLogger(JmsConsumerImpl.class.getName());

    private final JmsContextImpl context;
    private final Destination destination;
    private final boolean noLocal;
    private final boolean durable;
    private final boolean shared;

    private MessageListener messageListener;
    private String subscriptionName;
    private Consumer<JmsMessage> onReceive;

    private Listener listener;

    private boolean started = false;

    private ScheduledFuture<?> pingThread;
    private ScheduledFuture<?> cleanThread;

    private Map<String, JmsMessage> consumingMessages = new ConcurrentHashMap<>();

    public JmsConsumerImpl(JmsContextImpl context, Destination destination, boolean noLocal, boolean durable, boolean shared) {
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

    public JmsContextImpl context() {
        return context;
    }

    String getSubscriptionName() {
        return subscriptionName;
    }

    JmsConsumerImpl setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }

    void onReceive(Consumer<JmsMessage> onReceive) {
        this.onReceive = onReceive;
    }

    private void consuming(JmsMessage message) {
        try {
            consumingMessages.put(message.getJMSMessageID(), message);
            if (onReceive != null) {
                onReceive.accept(message);
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    void consume(JmsMessage message) {
        try {
            consumingMessages.remove(message.getJMSMessageID());
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    void consumeAll() {
        consumingMessages.values().forEach(message -> {
            try {
                message.acknowledge();
            } catch (JMSException e) {
                throw JMSExceptionSupport.wrap(e);
            }
        });
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
            this.listener = new NoPersistentListener(this);
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

    /**
     * 按消息 ID 读取消息体
     *
     * @param messageId 消息 ID
     * @return JMS 消息
     */
    private Message readMessage(String messageId) {

        byte[] propsKey = getDestinationPropsKey(destination, messageId);
        try (Jedis client = context.pool().getResource()) {
            Map<String, byte[]> props = JmsMessageHelper.toStringKey(client.hgetAll(propsKey));
            if (props == null) {
                //消息有可能已过期
                return null;
            }

            JmsMessage message = JmsMessageHelper.fromMap(props);
            message.setJMSMessageID(messageId);

            if (durable) {
                message.setAcknowledgeCallback(new JmsAcknowledgeCallback(this));
            }

            if (noLocal && Objects.equals(message.getJMSXMessageFrom(), context.getClientID())) {
                //仅处理非本机消息
                message.acknowledge();
                return null;
            }

            byte[] body = client.get(getDestinationBodyKey(destination, messageId));
            if (body != null) {
                message.setBody(body);
            }

            message.setReadOnly(true);
            consuming(message);

            LOGGER.finest(() -> String.format(
                    "Consumer of client '%s' receive a message: %s from '%s'",
                    context.getClientID(), message, destination
            ));

            if (JMSContext.AUTO_ACKNOWLEDGE == context.getSessionMode()
                    || DeliveryMode.NON_PERSISTENT == message.getJMSDeliveryMode()) {
                message.acknowledge();
            }

            return message;
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    private static final String DESTINATION_IS_NO_PERSISTENT = "Destination is no persistent，use setMessageListener pls.";

    @Override
    public Message receive() {
        return receive(0);
    }

    @Override
    public Message receive(long timeout) {
        if (!durable) {
            throw new JMSRuntimeException(DESTINATION_IS_NO_PERSISTENT);
        }

        String key = getMessageListKey();
        String backupKey = getDestinationBackupKey(destination, context.getClientID());

        try (Jedis client = context.pool().getResource()) {
            return readMessage(client.brpoplpush(key, backupKey, (int) timeout));
        }
    }

    @Override
    public Message receiveNoWait() {
        if (!durable) {
            throw new JMSRuntimeException(DESTINATION_IS_NO_PERSISTENT);
        }

        String key = getMessageListKey();
        String backupKey = getDestinationBackupKey(destination, context.getClientID());

        try (Jedis client = context.pool().getResource()) {
            String messageId = client.rpoplpush(key, backupKey);
            if (messageId != null) {
                return readMessage(messageId);
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
     * 清理 Redis 中的已消费消息备份队列
     */
    private void cleanBackup() {
        String backupKey = getDestinationBackupKey(destination, context.getClientID());
        try (Jedis client = context.pool().getResource()) {
            do {
                String messageId = client.lindex(backupKey, -1);
                if (messageId != null && !consumingMessages.containsKey(messageId)) {
                    boolean remoteExist = false;
                    if (destination instanceof Topic) {
                        remoteExist = client.sismember(getTopicItemConsumersKey(destination, messageId), context.getClientID());
                    } else if (destination instanceof Queue) {
                        remoteExist = client.exists(getDestinationPropsKey(destination, messageId));
                    }
                    if (remoteExist) {
                        client.rpoplpush(backupKey, getMessageListKey());
                    } else {
                        client.rpop(backupKey);
                    }
                } else {
                    // 备份队列为空或备份队列队尾元素正在消费中，可以认为备份队列中无历史记录
                    break;
                }
            } while (true);
        }
    }

    /**
     * 在 Redis 中注册消费者，并定时更新过期时间及清理消费队列
     */
    private void register() {
        if (durable) {
            JmsConfig config = context.config();

            this.pingThread = context.scheduledPool().scheduleWithFixedDelay(
                    this::ping, 0, config.getConsumerExpire().getSeconds(), TimeUnit.SECONDS);

            this.cleanThread = context.scheduledPool().scheduleWithFixedDelay(
                    this::cleanBackup, 0, config.getBackDuration().getSeconds(), TimeUnit.SECONDS);
        }
    }

    /**
     * 从 Redis 中注销消费者
     */
    private void unregistered() {
        try (Jedis client = context.pool().getResource()) {
            if (pingThread != null) {
                client.zrem(getTopicConsumersKey(destination), context.getClientID());
                pingThread.cancel(false);
                cleanThread.cancel(false);
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

    @Override
    public String toString() {
        return "JmsConsumerImpl{" +
                "noLocal=" + noLocal +
                ", durable=" + durable +
                ", shared=" + shared +
                ", subscriptionName='" + subscriptionName + '\'' +
                ", destination='" + destination + '\'' +
                '}';
    }
}
