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

    private JmsConfig jmsConfig;

    /**
     * 公共构造函数
     */
    public JmsConnectionFactory() {
        //do nothing
    }

    /**
     * 基于 JedisPool 和 clientId 构建
     *
     * @param clientId  客户端 ID
     * @param jedisPool Jedis 连接池
     */
    public JmsConnectionFactory(String clientId, JedisPool jedisPool) {
        this(clientId, jedisPool, new JmsConfig());
    }

    /**
     * 基于 JedisPool、clientId 和配置信息构建
     *
     * @param jedisPool Jedis 连接池
     * @param clientId  客户端 ID
     * @param jmsConfig 运行配置
     */
    public JmsConnectionFactory(String clientId, JedisPool jedisPool, JmsConfig jmsConfig) {
        setJedisPool(jedisPool);
        setClientId(clientId);
        setJmsConfig(jmsConfig);
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

        return new JMSContextImpl(clientId, jedisPool, jmsConfig, sessionMode);
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

    public void setJmsConfig(JmsConfig jmsConfig) {
        this.jmsConfig = jmsConfig;
    }
}
