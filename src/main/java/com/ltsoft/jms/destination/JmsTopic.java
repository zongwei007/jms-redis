package com.ltsoft.jms.destination;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * 订阅主题
 */
public class JmsTopic extends JmsDestination implements Topic {

    static final String TOPIC = "TOPIC";

    private String topicName;

    public JmsTopic(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    @Override
    public String toString() {
        return TOPIC + ":" + topicName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JmsTopic topic = (JmsTopic) o;

        return topicName.equals(topic.topicName);
    }

    @Override
    public int hashCode() {
        return topicName.hashCode();
    }
}
