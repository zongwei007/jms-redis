package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeConversionSupport;
import com.ltsoft.jms.util.TypeSerializeSupport;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * Created by zongw on 2016/9/7.
 */
public class JmsTextMessage extends JmsMessage implements TextMessage {

    private String text;

    @Override
    public void setText(String string) throws JMSException {
        this.text = string;
    }

    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public void clearBody() throws JMSException {
        this.text = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        try {
            return TypeConversionSupport.convert(text, c);
        } catch (IllegalArgumentException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public void setBody(byte[] bodyBytes) throws JMSException {
        setText(TypeSerializeSupport.deserialize(bodyBytes, String.class));
    }

    @Override
    public String getJMSType() throws JMSException {
        return MessageType.Text.name();
    }
}
