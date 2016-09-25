package com.ltsoft.message.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * 队列
 */
@JsonTypeName("q")
public class QueueImpl implements Queue {

    private String queueName;

    @JsonCreator
    public QueueImpl(@JsonProperty("name") String queueName) {
        this.queueName = queueName;
    }

    @JsonProperty("name")
    @Override
    public String getQueueName() throws JMSException {
        return queueName;
    }

    @Override
    public String toString() {
        return "QUEUE:" + queueName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueImpl queue = (QueueImpl) o;

        return queueName.equals(queue.queueName);

    }

    @Override
    public int hashCode() {
        return queueName.hashCode();
    }
}
