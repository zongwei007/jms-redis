package com.ltsoft.message.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

/**
 * 临时队列信息
 */
@JsonTypeName("tq")
public class TemporaryQueueImpl extends QueueImpl implements TemporaryQueue {

    @JsonCreator
    public TemporaryQueueImpl() {
        super("TEMPORARY_QUEUE:" + Math.round(Math.random() * 100000000));
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
