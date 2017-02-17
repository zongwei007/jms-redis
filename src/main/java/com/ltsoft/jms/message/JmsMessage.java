package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageProperty;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeConversionSupport;
import com.ltsoft.jms.util.TypeSerializeSupport;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 普通消息，无消息体
 */
public class JmsMessage implements Message {

    private String messageId;
    private long timestamp;
    private String correlationID;
    private Destination replyTo;
    private Destination destination;
    private int deliveryMode;
    private boolean redelivered;
    private long expiration;
    private long deliveryTime;
    private int priority;

    private String messageFrom;

    private MessageProperty property = new MessageProperty();

    private Consumer<JmsMessage> acknowledgeCallback;
    private boolean readOnly;

    @Override
    public String getJMSMessageID() throws JMSException {
        return messageId;
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        this.messageId = id;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return Optional.ofNullable(getJMSCorrelationID()).map(String::getBytes).orElse(null);
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        setJMSCorrelationID(new String(correlationID));
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        this.correlationID = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return correlationID;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Basic.name();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        this.expiration = expiration;
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return deliveryTime;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        this.deliveryTime = deliveryTime;
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        this.priority = priority;
    }

    public void setJMSXMessageFrom(String messageFrom) {
        this.messageFrom = messageFrom;
    }

    public String getJMSXMessageFrom() {
        return messageFrom;
    }

    @Override
    public void clearProperties() throws JMSException {
        property.clearProperties();
    }

    public void mergeProperties(MessageProperty props) {
        property.mergeProperty(props);
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return property.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return property.getBooleanProperty(name);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return property.getByteProperty(name);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return property.getShortProperty(name);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return property.getIntProperty(name);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return property.getLongProperty(name);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return property.getFloatProperty(name);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return property.getDoubleProperty(name);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return property.getStringProperty(name);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return property.getObjectProperty(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return Collections.enumeration(property.getPropertyNames());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        property.setProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.accept(this);
            } catch (Exception e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void clearBody() throws JMSException {
        //do nothing
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        Object body = getBody(Object.class);
        if (TypeConversionSupport.can(body, c)) {
            try {
                TypeConversionSupport.convert(body, c);
                return true;
            } catch (Exception e) {
                //do nothing
            }
        }
        return false;
    }

    public byte[] getBody() throws JMSException {
        return TypeSerializeSupport.serialize(getBody(Object.class));
    }

    public void setBody(byte[] bodyBytes) throws JMSException {
        throw new JMSException("Not Implemented");
    }

    public void setAcknowledgeCallback(Consumer<JmsMessage> callback) {
        this.acknowledgeCallback = callback;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    void checkReadOnly() throws JMSException {
        if (readOnly) {
            throw new MessageNotWriteableException("Message is read-only");
        }
    }
}
