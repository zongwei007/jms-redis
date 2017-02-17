package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageType;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import java.io.*;

/**
 * 字节数组消息
 */
public class JmsBytesMessage extends JmsMessage implements BytesMessage {

    private byte[] body = new byte[0];
    private ByteArrayInputStream bodyIs;
    private ByteArrayOutputStream bodyOs;
    private DataOutputStream dataOut;
    private DataInputStream dataIn;

    private void initializeWriting() throws JMSException {
        checkReadOnly();
        if (this.dataOut == null) {
            this.bodyOs = new ByteArrayOutputStream();
            this.dataOut = new DataOutputStream(bodyOs);
        }
    }

    private void initializeReading() throws JMSException {
        if (dataIn == null) {
            this.bodyIs = new ByteArrayInputStream(body);
            dataIn = new DataInputStream(bodyIs);
        }
    }

    @Override
    public long getBodyLength() throws JMSException {
        initializeReading();
        return bodyOs != null ? bodyOs.size() : body.length;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        initializeReading();
        try {
            return dataIn.readBoolean();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        initializeReading();
        try {
            return dataIn.readByte();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        initializeReading();
        try {
            return dataIn.readUnsignedByte();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        initializeReading();
        try {
            return dataIn.readShort();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        initializeReading();
        try {
            return dataIn.readUnsignedShort();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        initializeReading();
        try {
            return dataIn.readChar();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        initializeReading();
        try {
            return dataIn.readInt();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        initializeReading();
        try {
            return dataIn.readLong();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        initializeReading();
        try {
            return dataIn.readFloat();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        initializeReading();
        try {
            return dataIn.readDouble();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        initializeReading();
        try {
            return dataIn.readUTF();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        initializeReading();
        try {
            return dataIn.read(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        initializeReading();
        try {
            return dataIn.read(value, 0, length);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeByte(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeShort(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeChar(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeInt(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeLong(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeFloat(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeDouble(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeUTF(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        initializeWriting();
        try {
            dataOut.write(value);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        try {
            dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException();
        }
        initializeWriting();
        if (value instanceof Boolean) {
            writeBoolean((Boolean) value);
        } else if (value instanceof Character) {
            writeChar((Character) value);
        } else if (value instanceof Byte) {
            writeByte((Byte) value);
        } else if (value instanceof Short) {
            writeShort((Short) value);
        } else if (value instanceof Integer) {
            writeInt((Integer) value);
        } else if (value instanceof Long) {
            writeLong((Long) value);
        } else if (value instanceof Float) {
            writeFloat((Float) value);
        } else if (value instanceof Double) {
            writeDouble((Double) value);
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass().getSimpleName());
        }
    }

    @Override
    public void reset() throws JMSException {
        try {
            if (bodyIs != null) {
                bodyIs.close();
                this.bodyIs = null;
            }
            if (bodyOs != null) {
                this.bodyOs.close();
                this.bodyOs = null;
            }
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
        this.dataIn = null;
        this.dataOut = null;
    }

    @Override
    public void clearBody() throws JMSException {
        checkReadOnly();
        reset();
        this.body = new byte[0];
    }

    @Override
    public byte[] getBody() throws JMSException {
        return bodyOs != null ? bodyOs.toByteArray() : body;
    }

    @Override
    public void setBody(byte[] bodyBytes) throws JMSException {
        checkReadOnly();
        this.body = bodyBytes;
        reset();
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Bytes.name();
    }
}
