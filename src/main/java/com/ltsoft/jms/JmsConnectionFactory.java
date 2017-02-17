package com.ltsoft.jms;

import redis.clients.jedis.JedisPool;

import javax.jms.*;
import java.text.MessageFormat;
import java.util.Arrays;

import static javax.jms.JMSContext.*;

/**
 * JMS 连接工厂的实现
 */
public class JmsConnectionFactory implements ConnectionFactory {

    private JedisPool jedisPool;
    private String clientId;

    /**
     * 公共构造函数
     */
    public JmsConnectionFactory() {
        //do nothing
    }

    /**
     * 基于 JedisPool 和 clientId 构造
     *
     * @param jedisPool Jedis 连接池
     * @param clientId  客户端 ID
     */
    public JmsConnectionFactory(JedisPool jedisPool, String clientId) {
        setJedisPool(jedisPool);
        setClientId(clientId);
    }

    @Override
    public Connection createConnection() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public JMSContext createContext() {
        return createContext(AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        if (!Arrays.asList(AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE).contains(sessionMode)) {
            throw new JMSRuntimeException(MessageFormat.format("Unsupported sessionMode: {0}", sessionMode));
        }

        return new JMSContextImpl(clientId, jedisPool, sessionMode);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new UnsupportedOperationException();
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
