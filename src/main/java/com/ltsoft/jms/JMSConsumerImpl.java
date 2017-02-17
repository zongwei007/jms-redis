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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.ltsoft.jms.util.KeyHelper.*;
import static com.ltsoft.jms.util.ThreadPool.scheduledPool;

/**
 * 消费者实现
 */
public class JMSConsumerImpl implements JMSConsumer {

    //TODO 读取配置
    private static final long BACKUP_DURATION = Duration.ofMinutes(10).getSeconds();
    private static final long DELAY = Duration.ofHours(1).getSeconds();

    private final JMSContextImpl context;
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

    JMSContextImpl context() {
        return context;
    }

    String getSubscriptionName() {
        return subscriptionName;
    }

    JMSConsumerImpl setSubscriptionName(String subscriptionName) {
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

    /**
     * 按消息 ID 读取消息体
     *
     * @param messageId 消息 ID
     * @param another   如消息无效，获取另一条消息的方式
     * @return JMS 消息
     */
    private Message readMessage(String messageId, Supplier<Message> another) {

        byte[] propsKey = getDestinationPropsKey(destination, messageId);
        try (Jedis client = context.pool().getResource()) {
            Map<String, byte[]> props = JmsMessageHelper.toStringKey(client.hgetAll(propsKey));
            if (props == null) {
                //消息有可能已过期
                client.close();
                return another.get();
            }

            JmsMessage message = JmsMessageHelper.fromMap(props);
            message.setJMSMessageID(messageId);

            if (durable) {
                message.setAcknowledgeCallback(new JmsAcknowledgeCallback(this));
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
            consuming(message);

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
            return readMessage(client.brpoplpush(key, backupKey, (int) timeout), () -> receive(timeout));
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
     * 清理 Redis 中的已消费消息备份队列
     */
    private void cleanBackup() {
        String backupKey = getDestinationBackupKey(destination, context.getClientID());
        try (Jedis client = context.pool().getResource()) {
            String messageId = client.rpop(backupKey);
            // TODO 再实现一层备份
            if (!consumingMessages.containsKey(messageId)) {
                boolean remoteExist = false;
                if (destination instanceof Topic) {
                    remoteExist = client.sismember(getTopicItemConsumersKey(destination, messageId), context.getClientID());
                } else if (destination instanceof Queue) {
                    remoteExist = client.exists(getDestinationPropsKey(destination, messageId));
                }
                if (remoteExist) {
                    client.lpush(getMessageListKey(), messageId);
                }
            }
        }
    }

    /**
     * 在 Redis 中注册消费者，并定时更新过期时间及清理消费队列
     */
    private void register() {
        if (durable) {
            this.pingThread = scheduledPool().scheduleWithFixedDelay(this::ping, 0, DELAY, TimeUnit.SECONDS);
            this.cleanThread = scheduledPool().scheduleWithFixedDelay(this::cleanBackup, 0, BACKUP_DURATION, TimeUnit.SECONDS);
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
}
