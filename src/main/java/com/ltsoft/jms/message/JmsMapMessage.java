package com.ltsoft.jms.message;

import com.ltsoft.jms.util.MessageType;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.util.Enumeration;

/**
 * Created by zongw on 2016/9/7.
 */
public class JmsMapMessage extends JmsMessage implements MapMessage {
    @Override
    public boolean getBoolean(String name) throws JMSException {
        return false;
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return 0;
    }

    @Override
    public short getShort(String name) throws JMSException {
        return 0;
    }

    @Override
    public char getChar(String name) throws JMSException {
        return 0;
    }

    @Override
    public int getInt(String name) throws JMSException {
        return 0;
    }

    @Override
    public long getLong(String name) throws JMSException {
        return 0;
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return 0;
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return 0;
    }

    @Override
    public String getString(String name) throws JMSException {
        return null;
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return new byte[0];
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return null;
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return null;
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {

    }

    @Override
    public void setByte(String name, byte value) throws JMSException {

    }

    @Override
    public void setShort(String name, short value) throws JMSException {

    }

    @Override
    public void setChar(String name, char value) throws JMSException {

    }

    @Override
    public void setInt(String name, int value) throws JMSException {

    }

    @Override
    public void setLong(String name, long value) throws JMSException {

    }

    @Override
    public void setFloat(String name, float value) throws JMSException {

    }

    @Override
    public void setDouble(String name, double value) throws JMSException {

    }

    @Override
    public void setString(String name, String value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {

    }

    @Override
    public void setObject(String name, Object value) throws JMSException {

    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return false;
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Map.name();
    }
}
