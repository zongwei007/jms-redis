package com.ltsoft.jms.destination;

import redis.clients.jedis.JedisPool;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * 临时主题
 */
public class JmsTemporaryTopic extends JmsTopic implements TemporaryTopic {

    private final JedisPool jedisPool;

    public JmsTemporaryTopic(JedisPool jedisPool) {
        super("TEMPORARY_TOPIC:" + Math.round(Math.random() * 100000000));
        this.jedisPool = jedisPool;
    }

    @Override
    public void delete() throws JMSException {
        //TODO 实现delete
    }
}
