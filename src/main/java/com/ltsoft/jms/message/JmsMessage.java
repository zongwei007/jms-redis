package com.ltsoft.jms.message;

import com.ltsoft.jms.util.TypeConversionSupport;

import javax.jms.*;
import java.util.Enumeration;
import java.util.Optional;


public class JmsMessage implements Message {

    @Override
    public String getJMSMessageID() throws JMSException {
        return null;
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {

    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return JmsMessageHeader.getHeader(this, JmsMessageHeader.JMS_TIMESTAMP, Long.class).orElse(0L);
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        JmsMessageHeader.setHeader(this, JmsMessageHeader.JMS_TIMESTAMP, timestamp);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return new byte[0];
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {

    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {

    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return null;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return null;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {

    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return null;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {

    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return JmsMessageHeader.getHeader(this, JmsMessageHeader.JMS_DELIVERY_MODE, Integer.class).orElse(JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        JmsMessageHeader.setHeader(this, JmsMessageHeader.JMS_DELIVERY_MODE, deliveryMode);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return false;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {

    }

    @Override
    public String getJMSType() throws JMSException {
        return null;
    }

    @Override
    public void setJMSType(String type) throws JMSException {

    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {

    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {

    }

    @Override
    public int getJMSPriority() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {

    }

    @Override
    public void clearProperties() throws JMSException {

    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return false;
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Boolean.class);
        } else {
            return false;
        }
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Byte.class);
        } else {
            throw new NumberFormatException("property " + name + " was null");
        }
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Short.class);
        } else {
            throw new NumberFormatException("property " + name + " was null");
        }
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Integer.class);
        } else {
            throw new NumberFormatException("property " + name + " was null");
        }
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Long.class);
        } else {
            throw new NumberFormatException("property " + name + " was null");
        }
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Float.class);
        } else {
            throw new NullPointerException("property " + name + " was null");
        }
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, Double.class);
        } else {
            throw new NullPointerException("property " + name + " was null");
        }
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        if (propertyExists(name)) {
            return getProperty(name, String.class);
        } else {
            return null;
        }
    }

    <T> T getProperty(String name, Class<T> type) throws JMSException {
        Object value = getObjectProperty(name);
        return Optional.of(value)
                .map(item -> TypeConversionSupport.convert(item, type))
                .orElseThrow(() -> new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a " + type.getSimpleName()));
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
    public Object getObjectProperty(String name) throws JMSException {
        return null;
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return null;
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {

    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {

    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {

    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {

    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {

    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {

    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {

    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {

    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {

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
