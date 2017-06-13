package com.ltsoft.jms;

import com.ltsoft.jms.message.JmsMessage;

import javax.jms.JMSException;
import java.time.Instant;

/**
 * 消息消费记录
 */
public class ConsumingMessage {

    private JmsMessage message;

    private Instant timeoutAt;

    ConsumingMessage(JmsMessage message, Instant timeoutAt) {
        this.message = message;
        this.timeoutAt = timeoutAt;
    }

    void acknowledge() throws JMSException {
        message.acknowledge();
    }

    public JmsMessage getMessage() {
        return message;
    }

    public Instant getTimeoutAt() {
        return timeoutAt;
    }
}
