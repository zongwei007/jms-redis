package com.ltsoft.jms.destination;

import redis.clients.jedis.JedisPool;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

/**
 * 临时队列信息
 */
public class JmsTemporaryQueue extends JmsQueue implements TemporaryQueue {

    private final JedisPool jedisPool;

    public JmsTemporaryQueue(JedisPool jedisPool) {
        super("TEMPORARY_QUEUE:" + Math.round(Math.random() * 100000000));
        this.jedisPool = jedisPool;
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
