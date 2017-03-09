package com.ltsoft.jms.destination;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * 队列
 */
public class JmsQueue extends JmsDestination implements Queue {

    static final String QUEUE = "QUEUE";

    private String queueName;

    public JmsQueue(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String getQueueName() throws JMSException {
        return queueName;
    }

    @Override
    public String toString() {
        return QUEUE + ":" + queueName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JmsQueue queue = (JmsQueue) o;

        return queueName.equals(queue.queueName);
    }

    @Override
    public int hashCode() {
        return queueName.hashCode();
    }
}
