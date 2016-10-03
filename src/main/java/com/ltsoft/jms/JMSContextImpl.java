package com.ltsoft.jms;

import com.ltsoft.jms.destination.JmsQueue;
import com.ltsoft.jms.destination.JmsTemporaryQueue;
import com.ltsoft.jms.destination.JmsTemporaryTopic;
import com.ltsoft.jms.destination.JmsTopic;
import com.ltsoft.jms.message.JmsMessageFactory;
import redis.clients.jedis.JedisPool;

import javax.jms.*;
import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by zongw on 2016/9/5.
 */
public class JMSContextImpl implements JMSContext {

    private final String clientId;
    private final JedisPool jedisPool;
    private final int sessionMode;
    private final JmsMessageFactory messageFactory;

    private ExceptionListener exceptionListener;

    public JMSContextImpl(String clientId, JedisPool jedisPool, int sessionMode) {
        this(clientId, jedisPool, sessionMode, new JmsMessageFactory());
    }

    private JMSContextImpl(String clientId, JedisPool jedisPool, int sessionMode, JmsMessageFactory messageFactory) {
        this.clientId = clientId;
        this.jedisPool = jedisPool;
        this.sessionMode = sessionMode;
        this.messageFactory = messageFactory;
    }

    JedisPool pool() {
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
        //TODO
    }

    @Override
    public void stop() {
        //TODO
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        //TODO
    }

    @Override
    public boolean getAutoStart() {
        //TODO
        return true;
    }

    @Override
    public void close() {
        //TODO
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
        return new JMSConsumerImpl(clientId, jedisPool, sessionMode, destination, noLocal);
    }

    @Override
    public Queue createQueue(String queueName) {
        return new JmsQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        return new JmsTopic(topicName);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return null;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return new JMSQueueBrowserImpl(queue, jedisPool);
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
        //TODO
    }

    @Override
    public void acknowledge() {
        //TODO
    }
}
