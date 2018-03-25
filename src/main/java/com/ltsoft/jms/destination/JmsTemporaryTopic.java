package com.ltsoft.jms.destination;

import org.redisson.api.RedissonClient;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * 临时主题
 */
public class JmsTemporaryTopic extends JmsTopic implements TemporaryTopic {

    private final RedissonClient client;

    public JmsTemporaryTopic(RedissonClient client) {
        super("TEMPORARY_TOPIC:" + Math.round(Math.random() * 100000000));
        this.client = client;
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
