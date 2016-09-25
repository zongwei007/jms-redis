package com.ltsoft.jms.message;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

/**
 * Created by zongw on 2016/9/7.
 */
public class JmsObjectMessage extends JmsMessage implements ObjectMessage {
    @Override
    public void setObject(Serializable object) throws JMSException {

    }

    @Override
    public Serializable getObject() throws JMSException {
        return null;
    }
}
