package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeConversionSupport;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;
import java.io.*;
import java.util.Arrays;

import static com.ltsoft.jms.message.JmsMessageHelper.TYPE_TO_CODE;


/**
 * 字节流消息
 */
public class JmsStreamMessage extends JmsMessage implements StreamMessage {

    private byte[] body = new byte[0];
    private ByteArrayInputStream bodyIs;
    private ByteArrayOutputStream bodyOs;
    private DataOutputStream dataOut;
    private DataInputStream dataIn;

    private int bytesRemind = 0;

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
    public boolean readBoolean() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Boolean.class);
    }

    @Override
    public byte readByte() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Byte.class);
    }

    @Override
    public short readShort() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Short.class);
    }

    @Override
    public char readChar() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Character.class);
    }

    @Override
    public int readInt() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Integer.class);
    }

    @Override
    public long readLong() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Long.class);
    }

    @Override
    public float readFloat() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Float.class);
    }

    @Override
    public double readDouble() throws JMSException {
        return TypeConversionSupport.convert(readObject(), Double.class);
    }

    @Override
    public String readString() throws JMSException {
        return TypeConversionSupport.convert(readObject(), String.class);
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        initializeReading();
        try {
            if (bytesRemind == 0) {
                int type = dataIn.readByte();
                if (type != 0x90) {
                    throw new MessageFormatException("Object in steam is a " + readObjectByType(type).getClass().getSimpleName());
                }
                this.bytesRemind = dataIn.readInt();
            }

            if (bytesRemind < value.length) {
                this.bytesRemind = 0;
                return dataIn.read(value, 0, bytesRemind);
            } else {
                int read = dataIn.read(value);
                this.bytesRemind = bytesRemind - read;
                return read;
            }
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    private Object readObjectByType(int type) throws JMSException, IOException {
        switch (type) {
            case 0xc0:
                return dataIn.readBoolean();
            case 0xca:
                return dataIn.readFloat();
            case 0xcb:
                return dataIn.readDouble();
            case 0xd0:
                return dataIn.readShort();
            case 0xd1:
                return dataIn.readInt();
            case 0xd2:
                return dataIn.readLong();
            case 0xd8:
                return dataIn.readByte();
            case 0xd9:
                return dataIn.readUTF();
            case 0x90:
                int len = dataIn.readInt();
                byte[] bytes = new byte[len];
                return dataIn.read(bytes);
        }
        throw new MessageFormatException("Unknown data type");
    }

    @Override
    public Object readObject() throws JMSException {
        initializeReading();
        try {
            return readObjectByType(dataIn.readByte());
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeString(String value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        writeObject(value);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        writeBytes(Arrays.copyOfRange(value, offset, length));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeObject(Object value) throws JMSException {
        initializeWriting();
        try {
            dataOut.writeByte(TYPE_TO_CODE.get(value.getClass()));
            if (value instanceof Boolean) {
                dataOut.writeBoolean((Boolean) value);
            } else if (value instanceof Character) {
                dataOut.writeChar((Character) value);
            } else if (value instanceof Byte) {
                dataOut.writeByte((Byte) value);
            } else if (value instanceof Short) {
                dataOut.writeShort((Short) value);
            } else if (value instanceof Integer) {
                dataOut.writeInt((Integer) value);
            } else if (value instanceof Long) {
                dataOut.writeLong((Long) value);
            } else if (value instanceof Float) {
                dataOut.writeFloat((Float) value);
            } else if (value instanceof Double) {
                dataOut.writeDouble((Double) value);
            } else if (value instanceof String) {
                dataOut.writeUTF((String) value);
            } else if (value instanceof byte[]) {
                byte[] bytes = (byte[]) value;
                dataOut.writeInt(bytes.length);
                dataOut.write(bytes);
            } else {
                throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass().getSimpleName());
            }
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
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
        return MessageType.Stream.name();
    }
}
