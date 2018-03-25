package com.ltsoft.jms.destination;

import org.redisson.api.RedissonClient;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

/**
 * 临时队列信息
 */
public class JmsTemporaryQueue extends JmsQueue implements TemporaryQueue {

    private final RedissonClient client;

    public JmsTemporaryQueue(RedissonClient client) {
        super("TEMPORARY_QUEUE:" + Math.round(Math.random() * 100000000));
        this.client = client;
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
