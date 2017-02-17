package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeConversionSupport;
import com.ltsoft.jms.util.TypeSerializeSupport;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

/**
 * 可序列化对象消息
 */
public class JmsObjectMessage extends JmsMessage implements ObjectMessage {

    private Serializable obj;

    @Override
    public void setObject(Serializable object) throws JMSException {
        this.obj = object;
    }

    @Override
    public Serializable getObject() throws JMSException {
        return obj;
    }

    @Override
    public void clearBody() throws JMSException {
        this.obj = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        try {
            return TypeConversionSupport.convert(obj, c);
        } catch (IllegalArgumentException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void setBody(byte[] bodyBytes) throws JMSException {
        setObject(TypeSerializeSupport.deserialize(bodyBytes, Serializable.class));
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Object.name();
    }
}
