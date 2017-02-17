package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageProperty;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeSerializeSupport;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.util.*;

/**
 * Map 消息
 */
public class JmsMapMessage extends JmsMessage implements MapMessage {

    private final MapMessageBody body = new MapMessageBody();

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return body.getBooleanProperty(name);
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return body.getByteProperty(name);
    }

    @Override
    public short getShort(String name) throws JMSException {
        return body.getShortProperty(name);
    }

    @Override
    public char getChar(String name) throws JMSException {
        return body.getCharacter(name);
    }

    @Override
    public int getInt(String name) throws JMSException {
        return body.getIntProperty(name);
    }

    @Override
    public long getLong(String name) throws JMSException {
        return body.getLongProperty(name);
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return body.getFloatProperty(name);
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return body.getDoubleProperty(name);
    }

    @Override
    public String getString(String name) throws JMSException {
        return body.getStringProperty(name);
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return body.getBytes(name);
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return body.getObjectProperty(name);
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return Collections.enumeration(body.getPropertyNames());
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        setObject(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        body.setBytes(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        setBytes(name, Arrays.copyOfRange(value, offset, length));
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        body.setProperty(name, value);
    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return body.propertyExists(name);
    }

    @Override
    public void clearBody() throws JMSException {
        body.clearProperties();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (c.isAssignableFrom(HashMap.class)) {
            return (T) body.getMap();
        } else if (Map.class.isAssignableFrom(c)) {
            try {
                T map = c.newInstance();
                ((Map) map).putAll(body.getMap());
                return map;
            } catch (InstantiationException | IllegalAccessException e) {
                throw JMSExceptionSupport.create(e);
            }
        }

        throw new JMSException("can't not cast body to " + c.getSimpleName());
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return Map.class.isAssignableFrom(c);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setBody(byte[] bytes) throws JMSException {
        body.merge(TypeSerializeSupport.deserialize(bytes, HashMap.class));
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Map.name();
    }

    private class MapMessageBody extends MessageProperty {

        void setBytes(String name, byte[] bytes) {
            if (bytes != null) {
                properties.put(name, bytes);
            }
        }

        byte[] getBytes(String name) {
            return getProperty(name, byte[].class).orElse(null);
        }

        void merge(Map<String, Object> data) {
            properties.putAll(data);
        }

        HashMap<String, Object> getMap() {
            return properties;
        }
    }
}
