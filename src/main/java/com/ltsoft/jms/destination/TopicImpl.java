package com.ltsoft.message.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * 订阅主题
 */
@JsonTypeName("t")
public class TopicImpl implements Topic {

    private String topicName;

    @JsonCreator
    public TopicImpl(@JsonProperty("name") String topicName) {
        this.topicName = topicName;
    }

    @JsonProperty("name")
    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    @Override
    public String toString() {
        return "TOPIC:" + topicName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicImpl topic = (TopicImpl) o;

        return topicName.equals(topic.topicName);

    }

    @Override
    public int hashCode() {
        return topicName.hashCode();
    }
}
