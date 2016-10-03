package com.ltsoft.jms.message;

import com.ltsoft.jms.util.MessageType;

import javax.jms.JMSException;
import javax.jms.StreamMessage;

/**
 * Created by zongw on 2016/9/7.
 */
public class JmsStreamMessage extends JmsMessage implements StreamMessage {
    @Override
    public boolean readBoolean() throws JMSException {
        return false;
    }

    @Override
    public byte readByte() throws JMSException {
        return 0;
    }

    @Override
    public short readShort() throws JMSException {
        return 0;
    }

    @Override
    public char readChar() throws JMSException {
        return 0;
    }

    @Override
    public int readInt() throws JMSException {
        return 0;
    }

    @Override
    public long readLong() throws JMSException {
        return 0;
    }

    @Override
    public float readFloat() throws JMSException {
        return 0;
    }

    @Override
    public double readDouble() throws JMSException {
        return 0;
    }

    @Override
    public String readString() throws JMSException {
        return null;
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return 0;
    }

    @Override
    public Object readObject() throws JMSException {
        return null;
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {

    }

    @Override
    public void writeByte(byte value) throws JMSException {

    }

    @Override
    public void writeShort(short value) throws JMSException {

    }

    @Override
    public void writeChar(char value) throws JMSException {

    }

    @Override
    public void writeInt(int value) throws JMSException {

    }

    @Override
    public void writeLong(long value) throws JMSException {

    }

    @Override
    public void writeFloat(float value) throws JMSException {

    }

    @Override
    public void writeDouble(double value) throws JMSException {

    }

    @Override
    public void writeString(String value) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {

    }

    @Override
    public void writeObject(Object value) throws JMSException {

    }

    @Override
    public void reset() throws JMSException {

    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Stream.name();
    }
}
