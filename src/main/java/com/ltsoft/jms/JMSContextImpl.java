package com.ltsoft.jms;

import com.ltsoft.jms.destination.JmsQueue;
import com.ltsoft.jms.destination.JmsTemporaryQueue;
import com.ltsoft.jms.destination.JmsTemporaryTopic;
import com.ltsoft.jms.destination.JmsTopic;
import com.ltsoft.jms.message.JmsMessageFactory;
import redis.clients.jedis.JedisPool;

import javax.jms.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by zongw on 2016/9/5.
 */
public class JMSContextImpl implements JMSContext {

    private final String clientId;
    private final JedisPool jedisPool;
    private final int sessionMode;
    private final JmsMessageFactory messageFactory;

    private ExceptionListener exceptionListener;

    private boolean autoStart = true;
    private List<JMSConsumerImpl> consumers = new ArrayList<>();

    public JMSContextImpl(String clientId, JedisPool jedisPool, int sessionMode) {
        this(clientId, jedisPool, sessionMode, new JmsMessageFactory());
    }

    private JMSContextImpl(String clientId, JedisPool jedisPool, int sessionMode, JmsMessageFactory messageFactory) {
        this.clientId = clientId;
        this.jedisPool = jedisPool;
        this.sessionMode = sessionMode;
        this.messageFactory = messageFactory;
    }

    public JedisPool pool() {
        return jedisPool;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return new JMSContextImpl(clientId, jedisPool, sessionMode, messageFactory);
    }

    @Override
    public JMSProducer createProducer() {
        return new JMSProducerImpl(this);
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
        consumers.forEach(JMSConsumerImpl::start);
    }

    @Override
    public void stop() {
        consumers.forEach(JMSConsumerImpl::close);
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
        this.acknowledge();
        this.stop();
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
        JMSConsumerImpl consumer = new JMSConsumerImpl(this, destination, noLocal, true, true);
        if (getAutoStart()) {
            consumer.start();
        }
        consumers.add(consumer);

        return consumer;
    }

    @Override
    public Queue createQueue(String queueName) {
        return new JmsQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        return new JmsTopic(topicName);
    }

    private JMSConsumer createNamedTopicConsumer(Topic topic, String name, boolean noLocal, boolean durable, boolean shared) {
        if (consumers.stream().anyMatch(consumer -> Objects.equals(name, consumer.getSubscriptionName()))) {
            if (!shared) {
                throw new JMSRuntimeException(String.format("Consumer %s is exist", name));
            }
        }

        JMSConsumerImpl consumer = new JMSConsumerImpl(this, topic, noLocal, durable, shared).setSubscriptionName(name);
        if (getAutoStart()) {
            consumer.start();
        }
        consumers.add(consumer);

        return consumer;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return createNamedTopicConsumer(topic, name, noLocal, true, false);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return createNamedTopicConsumer(topic, name, false, true, true);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return createNamedTopicConsumer(topic, sharedSubscriptionName, false, false, true);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return new JMSQueueBrowserImpl(queue, this);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return new JmsTemporaryQueue(jedisPool);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return new JmsTemporaryTopic(jedisPool);
    }

    @Override
    public void unsubscribe(String name) {
        consumers.stream()
                .filter(consumer -> Objects.equals(name, consumer.getSubscriptionName()))
                .peek(JMSConsumerImpl::close)
                .forEach(consumer -> consumers.remove(consumer));
    }

    @Override
    public void acknowledge() {
        consumers.forEach(JMSConsumerImpl::consumeAll);
    }
}
