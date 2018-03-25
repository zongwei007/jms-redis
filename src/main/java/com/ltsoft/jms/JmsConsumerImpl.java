package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.listener.Listener;
import com.ltsoft.jms.listener.NoPersistentListener;
import com.ltsoft.jms.listener.PersistentListener;
import com.ltsoft.jms.message.JmsMessage;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDeque;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;

import javax.jms.*;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.ltsoft.jms.message.JmsMessageHelper.fromMap;
import static com.ltsoft.jms.message.JmsMessageHelper.toStringKey;
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

    private Map<String, ConsumingMessage> consumingMessages = new ConcurrentHashMap<>();

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
        JmsConfig config = context.config();

        try {
            Instant expireAt = Instant.now().plus(config.getConsumingTimeout());

            consumingMessages.put(message.getJMSMessageID(), new ConsumingMessage(message, expireAt));
            if (onReceive != null) {
                onReceive.accept(message);
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
    }

    void consume(JmsMessage message) {
        try {
            if (message != null) {
                consumingMessages.remove(message.getJMSMessageID());
            }
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
        if (Objects.isNull(messageId)) {
            return null;
        }

        RedissonClient client = context.client();
        String propsKey = getDestinationPropsKey(destination, messageId);

        try {
            Map<String, byte[]> props = toStringKey(client.<byte[], byte[]>getMap(propsKey, ByteArrayCodec.INSTANCE).readAllMap());
            if (props.size() == 0) {
                //消息有可能已过期
                return null;
            }

            JmsMessage message = fromMap(props);
            message.setJMSMessageID(messageId);

            if (durable) {
                message.setAcknowledgeCallback(new JmsAcknowledgeCallback(this));
            }

            if (noLocal && Objects.equals(message.getJMSXMessageFrom(), context.getClientID())) {
                //仅处理非本机消息
                message.acknowledge();
                return null;
            }

            byte[] body = client.<byte[]>getBucket(getDestinationBodyKey(destination, messageId), ByteArrayCodec.INSTANCE).get();
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

        try {
            RBlockingDeque<String> blockingDeque = context.client().getBlockingDeque(key, StringCodec.INSTANCE);
            return readMessage(blockingDeque.pollLastAndOfferFirstTo(backupKey, timeout, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException("", e);//TODO
        }
    }

    @Override
    public Message receiveNoWait() {
        if (!durable) {
            throw new JMSRuntimeException(DESTINATION_IS_NO_PERSISTENT);
        }

        String key = getMessageListKey();
        String backupKey = getDestinationBackupKey(destination, context.getClientID());

        RDeque<String> deque = context.client().getDeque(key, StringCodec.INSTANCE);
        return readMessage(deque.pollLastAndOfferFirstTo(backupKey));
    }

    /**
     * 更新 Redis 中的消费者过期时间
     */
    private void ping() {
        RScoredSortedSet<String> scoredSortedSet = context.client().getScoredSortedSet(getTopicConsumersKey(destination), StringCodec.INSTANCE);
        scoredSortedSet.add(Instant.now().getEpochSecond(), context.getClientID());
    }

    /**
     * 清理 Redis 中的已消费消息备份队列
     */
    private void cleanBackup() {
        RedissonClient client = context.client();
        String backupKey = getDestinationBackupKey(destination, context.getClientID());
        Instant now = Instant.now();
        do {
            @SuppressWarnings("ConstantConditions")
            String messageId = client.<String>getList(backupKey, StringCodec.INSTANCE).get(-1);
            if (messageId != null) {
                ConsumingMessage consumingMessage = consumingMessages.get(messageId);
                if (Objects.isNull(consumingMessage) || now.isAfter(consumingMessage.getTimeoutAt())) {

                    boolean remoteExist = false;
                    if (destination instanceof Topic) {
                        remoteExist = client.getSet(getTopicItemConsumersKey(destination, messageId), StringCodec.INSTANCE).contains(context.getClientID());
                    } else if (destination instanceof Queue) {
                        remoteExist = client.getKeys().countExists(getDestinationPropsKey(destination, messageId)) > 0;
                    }
                    if (remoteExist) {
                        client.getDeque(backupKey, StringCodec.INSTANCE).pollLastAndOfferFirstTo(getMessageListKey());
                    } else {
                        client.getDeque(backupKey, StringCodec.INSTANCE).pollLast();
                    }

                    if (Objects.nonNull(consumingMessage)) {
                        consumingMessages.remove(messageId);
                    }
                }
            } else {
                // 备份队列为空或备份队列队尾元素正在消费中，可以认为备份队列中无历史记录
                break;
            }
        } while (true);
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
        RedissonClient client = context.client();
        if (pingThread != null) {
            client.getScoredSortedSet(getTopicConsumersKey(destination), StringCodec.INSTANCE).remove(context.getClientID());
            pingThread.cancel(false);
            cleanThread.cancel(false);
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
