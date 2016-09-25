package com.ltsoft.message.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * 临时主题
 */
@JsonTypeName("tt")
public class TemporaryTopicImpl extends TopicImpl implements TemporaryTopic {

    @JsonCreator
    public TemporaryTopicImpl() {
        super("TEMPORARY_TOPIC:" + Math.round(Math.random() * 100000000));
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
