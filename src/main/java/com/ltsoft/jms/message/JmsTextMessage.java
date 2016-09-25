package com.ltsoft.jms.message;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * Created by zongw on 2016/9/7.
 */
public class JmsTextMessage extends JmsMessage implements TextMessage {
    @Override
    public void setText(String string) throws JMSException {

    }

    @Override
    public String getText() throws JMSException {
        return null;
    }
}
