package com.ltsoft.jms.message;

import com.ltsoft.jms.util.TypeConversionSupport;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import java.util.*;
import java.util.function.Supplier;


public class JmsMessage implements Message {

    private String messageId;
    private long timestamp;
    private String correlationID;
    private Destination replyTo;
    private Destination destination;
    private int deliveryMode;
    private boolean redelivered;
    private String type;
    private long expiration;
    private long deliveryTime;
    private int priority;

    private Map<String, Object> properties = new HashMap<>();

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
        return type;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        this.type = type;
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

    @Override
    public void clearProperties() throws JMSException {
        this.properties.clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    private <T> Optional<T> getProperty(String name, Class<T> type) throws JMSException {
        try {
            return Optional.ofNullable(getObjectProperty(name))
                    .map(val -> TypeConversionSupport.convert(val, type));
        } catch (Exception e) {
            throw new MessageFormatException(String.format("Get property %s fail：%s", name, e.getMessage()));
        }
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return getProperty(name, Boolean.class).orElse(false);
    }

    private Supplier<NumberFormatException> numberFormatException(String name) {
        return () -> new NumberFormatException("property " + name + " is null");
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return getProperty(name, Byte.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return getProperty(name, Short.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return getProperty(name, Integer.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return getProperty(name, Long.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return getProperty(name, Float.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return getProperty(name, Double.class).orElseThrow(numberFormatException(name));
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return getProperty(name, String.class).orElse(null);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return properties.get(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return Collections.enumeration(properties.keySet());
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

    private static void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof Character ||
                value instanceof String ||
                value == null;

        if (!valid) {
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkValidObject(value);
        properties.put(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {

    }

    @Override
    public void clearBody() throws JMSException {

    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return false;
    }
}
