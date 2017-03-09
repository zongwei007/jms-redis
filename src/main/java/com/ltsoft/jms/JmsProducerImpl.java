package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.util.MessageProperty;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.jms.*;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.ltsoft.jms.message.JmsMessageHelper.*;
import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * JMS 消息提供者
 */
public class JmsProducerImpl implements JMSProducer {

    private static final Logger LOGGER = Logger.getLogger(JmsProducerImpl.class.getName());

    private final JmsContextImpl context;

    private boolean disableMessageTimestamp = false;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority;
    private long timeToLive;
    private CompletionListener completionListener;
    private String correlationID;
    private Destination replyTo;

    private MessageProperty property = new MessageProperty();

    public JmsProducerImpl(JmsContextImpl context) {
        this.context = context;
    }

    private JmsProducerImpl sendMessage(Destination destination, JmsMessage message) throws JMSException {
        try (Jedis client = context.pool().getResource()) {
            if (destination instanceof Topic) {
                byte[] propsKey = getDestinationPropsKey(destination, message.getJMSMessageID());
                byte[] bodyKey = getDestinationBodyKey(destination, message.getJMSMessageID());
                byte[] body = message.getBody();
                long now = Instant.now().getEpochSecond();
                long expire = context.config().getConsumerExpire().getSeconds();

                if (DeliveryMode.PERSISTENT == deliveryMode) {
                    //获取频道相关的所有订阅者
                    Set<String> consumers = client.zrangeByScore(getTopicConsumersKey(destination), now - expire, now + expire);
                    String consumersKey = getTopicItemConsumersKey(destination, message.getJMSMessageID());

                    Pipeline pipe = client.pipelined();
                    pipe.multi();
                    pipe.hmset(propsKey, toBytesKey(toMap(message)));
                    if (body != null) {
                        pipe.set(bodyKey, body);
                    }
                    pipe.sadd(consumersKey, consumers.toArray(new String[consumers.size()]));
                    for (String consumerId : consumers) {
                        pipe.lpush(getTopicConsumerListKey(destination, consumerId), message.getJMSMessageID());
                    }
                    pipe.exec();
                    if (timeToLive > 0) {
                        pipe.pexpire(propsKey, timeToLive);
                        pipe.pexpire(bodyKey, timeToLive);
                        pipe.pexpire(consumersKey, timeToLive);
                    }
                    pipe.sync();
                } else {
                    client.publish(getDestinationKey(destination).getBytes(), toBytes(message));
                }
            } else if (destination instanceof Queue) {
                byte[] propsKey = getDestinationPropsKey(destination, message.getJMSMessageID());
                byte[] bodyKey = getDestinationBodyKey(destination, message.getJMSMessageID());
                byte[] body = message.getBody();

                Pipeline pipe = client.pipelined();
                pipe.multi();
                pipe.hmset(propsKey, toBytesKey(toMap(message)));
                if (body != null) {
                    pipe.set(bodyKey, body);
                }
                pipe.lpush(getDestinationKey(destination), message.getJMSMessageID());
                pipe.exec();
                if (timeToLive > 0) {
                    pipe.pexpire(propsKey, timeToLive);
                    pipe.pexpire(bodyKey, timeToLive);
                }
                pipe.sync();
            } else {
                throw new JMSException("不支持的目的类型");
            }
        }

        LOGGER.finest(() -> String.format(
                "Client '%s' send %s: %s, timeToLive: %s",
                context.getClientID(), message.getClass().getSimpleName(), message, timeToLive
        ));

        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        try {
            long timestamp = System.currentTimeMillis();
            if (!disableMessageTimestamp) {
                message.setJMSTimestamp(timestamp);
            }
            message.setJMSMessageID(getMessageId());
            message.setJMSDestination(destination);
            message.setJMSDeliveryMode(deliveryMode);
            if (timeToLive > 0) {
                message.setJMSExpiration(timestamp + timeToLive);
            }
            message.setJMSPriority(priority);
            message.setJMSReplyTo(replyTo);
            message.setJMSCorrelationID(correlationID);

            JmsMessage item = (JmsMessage) message;
            item.setJMSXMessageFrom(context.getClientID());

            item.mergeProperties(property);

            if (completionListener != null) {
                context.cachedPool().execute(() -> {
                    try {
                        sendMessage(destination, item);
                        completionListener.onCompletion(message);
                    } catch (Exception e) {
                        completionListener.onException(message, e);
                    }
                });
            } else {
                sendMessage(destination, item);
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }

        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        return send(destination, context.createTextMessage(body));
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        MapMessage message = context.createMapMessage();
        body.forEach((key, val) -> {
            try {
                message.setObject(key, val);
            } catch (JMSException e) {
                throw JMSExceptionSupport.wrap(e);
            }
        });
        return send(destination, message);
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        StreamMessage message = context.createStreamMessage();
        try {
            message.writeBytes(body);
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }
        return send(destination, message);
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        return send(destination, context.createObjectMessage(body));
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getDisableMessageID() {
        return false;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        this.disableMessageTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDeliveryDelay() {
        return 0;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        return setProperty(name, Boolean.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        return setProperty(name, Byte.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        return setProperty(name, Short.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        return setProperty(name, Integer.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        return setProperty(name, Long.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        return setProperty(name, Float.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        return setProperty(name, Double.valueOf(value));
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        return setProperty(name, (Object) value);
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        property.setProperty(name, value);
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        property.clearProperties();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return property.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return property.getBooleanProperty(name);
    }

    @Override
    public byte getByteProperty(String name) {
        return property.getByteProperty(name);
    }

    @Override
    public short getShortProperty(String name) {
        return property.getShortProperty(name);
    }

    @Override
    public int getIntProperty(String name) {
        return property.getIntProperty(name);
    }

    @Override
    public long getLongProperty(String name) {
        return property.getLongProperty(name);
    }

    @Override
    public float getFloatProperty(String name) {
        return property.getFloatProperty(name);
    }

    @Override
    public double getDoubleProperty(String name) {
        return property.getDoubleProperty(name);
    }

    @Override
    public String getStringProperty(String name) {
        return property.getStringProperty(name);
    }

    @Override
    public Object getObjectProperty(String name) {
        return property.getObjectProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return property.getPropertyNames();
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        return setJMSCorrelationID(new String(correlationID));
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        if (getJMSCorrelationID() == null) {
            return null;
        }
        return getJMSCorrelationID().getBytes();
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        this.correlationID = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationID;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getJMSType() {
        return null;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }
}