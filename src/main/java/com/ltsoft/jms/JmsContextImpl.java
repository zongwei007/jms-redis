package com.ltsoft.jms;

import com.ltsoft.jms.destination.JmsQueue;
import com.ltsoft.jms.destination.JmsTemporaryQueue;
import com.ltsoft.jms.destination.JmsTemporaryTopic;
import com.ltsoft.jms.destination.JmsTopic;
import com.ltsoft.jms.message.JmsMessageFactory;
import com.ltsoft.jms.util.ThreadPool;
import org.redisson.api.RedissonClient;

import javax.jms.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * JMS 操作上下文
 */
public class JmsContextImpl implements JMSContext {

    private static final Logger LOGGER = Logger.getLogger(JmsContextImpl.class.getName());

    private final String clientId;

    private final RedissonClient client;

    private final JmsConfig jmsConfig;

    private final int sessionMode;

    private final JmsMessageFactory messageFactory;

    private final ThreadPool threadPool;

    private ExceptionListener exceptionListener;

    private boolean autoStart = true;
    private List<JmsConsumerImpl> consumers = new ArrayList<>();

    private AtomicInteger messageCount = new AtomicInteger();

    public JmsContextImpl(String clientId, RedissonClient client, JmsConfig jmsConfig, ThreadPool threadPool, int sessionMode) {
        this(clientId, client, jmsConfig, threadPool, sessionMode, new JmsMessageFactory());
    }

    private JmsContextImpl(String clientId, RedissonClient client, JmsConfig jmsConfig, ThreadPool threadPool, int sessionMode, JmsMessageFactory messageFactory) {
        this.clientId = clientId;
        this.client = client;
        this.jmsConfig = jmsConfig;
        this.sessionMode = sessionMode;
        this.messageFactory = messageFactory;
        this.threadPool = threadPool;
    }

    /**
     * @return Redisson 客户端
     */
    public RedissonClient client() {
        return client;
    }

    /**
     * @return JMS 配置
     */
    public JmsConfig config() {
        return jmsConfig;
    }

    /**
     * @return 执行线程池
     */
    public ExecutorService cachedPool() {
        return threadPool.cachedPool();
    }

    /**
     * @return 定时调度线程池
     */
    public ScheduledExecutorService scheduledPool() {
        return threadPool.scheduledPool();
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return new JmsContextImpl(clientId, client, jmsConfig, threadPool, sessionMode, messageFactory);
    }

    @Override
    public JMSProducer createProducer() {
        return new JmsProducerImpl(this);
    }

    @Override
    public String getClientID() {
        return clientId;
    }

    @Override
    public void setClientID(String clientID) {
        throw new InvalidClientIDRuntimeException("Client ID is readOnly");
    }

    @Override
    public ConnectionMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        this.exceptionListener = listener;
    }

    @Override
    public void start() {
        consumers.forEach(JmsConsumerImpl::start);
    }

    @Override
    public void stop() {
        consumers.forEach(JmsConsumerImpl::close);
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public void close() {
        acknowledge();
        stop();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return messageFactory.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return messageFactory.createMapMessage();
    }

    @Override
    public Message createMessage() {
        return messageFactory.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return messageFactory.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        return messageFactory.createObjectMessage(object);
    }

    @Override
    public StreamMessage createStreamMessage() {
        return messageFactory.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return messageFactory.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) {
        return messageFactory.createTextMessage(text);
    }

    @Override
    public boolean getTransacted() {
        return false;
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recover() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return createConsumer(destination, null);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        return createNamedConsumer(destination, null, noLocal, true, true);
    }

    @Override
    public Queue createQueue(String queueName) {
        return new JmsQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        return new JmsTopic(topicName);
    }

    private JMSConsumer createNamedConsumer(Destination destination, String name, boolean noLocal, boolean durable, boolean shared) {
        JmsConsumerImpl consumer = new JmsConsumerImpl(this, destination, noLocal, durable, shared);

        if (name != null && consumers.stream().anyMatch(item -> Objects.equals(name, item.getSubscriptionName()))) {
            if (!shared) {
                throw new JMSRuntimeException(String.format("Consumer %s is exist", name));
            }
            consumer.setSubscriptionName(name);
        }

        if (getAutoStart()) {
            consumer.start();
        }

        if (JMSContext.DUPS_OK_ACKNOWLEDGE == sessionMode) {
            consumer.onReceive(message -> {
                int count = messageCount.incrementAndGet();
                if (count > jmsConfig.getDupsCount()) {
                    acknowledge();
                    messageCount.set(0);
                }
            });
        }

        consumers.add(consumer);

        LOGGER.finest(() -> String.format("Client %s create %s", getClientID(), consumer.toString()));

        return consumer;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return createNamedConsumer(topic, name, noLocal, true, false);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return createNamedConsumer(topic, name, false, true, true);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return createNamedConsumer(topic, sharedSubscriptionName, false, false, true);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return new JmsQueueBrowserImpl(queue, this);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return new JmsTemporaryQueue(client);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return new JmsTemporaryTopic(client);
    }

    @Override
    public void unsubscribe(String name) {

        LOGGER.finest(() -> String.format("Client %s unsubscribe named '%s' consumers", getClientID(), name));

        consumers.stream()
                .filter(consumer -> Objects.equals(name, consumer.getSubscriptionName()))
                .peek(JmsConsumerImpl::close)
                .forEach(consumer -> consumers.remove(consumer));
    }

    @Override
    public void acknowledge() {
        consumers.forEach(JmsConsumerImpl::consumeAll);
    }
}
