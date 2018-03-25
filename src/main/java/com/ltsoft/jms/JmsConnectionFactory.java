package com.ltsoft.jms;

import com.ltsoft.jms.util.ThreadPool;
import org.redisson.api.RedissonClient;

import javax.jms.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;
import static javax.jms.JMSContext.*;

/**
 * JMS 连接工厂的实现
 */
public class JmsConnectionFactory implements ConnectionFactory {

    private static final Logger LOGGER = Logger.getLogger(JmsConnectionFactory.class.getName());

    private RedissonClient client;

    private String clientId;

    private JmsConfig jmsConfig;

    private ThreadPool threadPool;

    /**
     * 公共构造函数
     */
    public JmsConnectionFactory() {
        //do nothing
    }

    /**
     * 基于 RedissonClient 和 clientId 构建
     *
     * @param clientId 客户端 ID
     * @param client   Redisson 客户端
     */
    public JmsConnectionFactory(String clientId, RedissonClient client) {
        this(clientId, client, new JmsConfig());
    }

    /**
     * 基于 RedissonClient、clientId 和配置信息构建
     *
     * @param clientId  客户端 ID
     * @param client    Redisson 客户端
     * @param jmsConfig 运行配置
     */
    public JmsConnectionFactory(String clientId, RedissonClient client, JmsConfig jmsConfig) {
        setRedissonClient(client);
        setClientId(clientId);
        setJmsConfig(jmsConfig);
    }

    private synchronized void initThreadPool() {
        if (jmsConfig == null) {
            this.jmsConfig = new JmsConfig();
        }
        if (threadPool == null) {
            this.threadPool = new ThreadPool(jmsConfig);
        }
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

        if (threadPool == null) {
            initThreadPool();
        }

        LOGGER.finest(() -> String.format("Create JMSContext with clientId: %s, sessionMode: %s", clientId, sessionMode));

        return new JmsContextImpl(requireNonNull(clientId), requireNonNull(client), jmsConfig, threadPool, sessionMode);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new UnsupportedOperationException();
    }

    public void setRedissonClient(RedissonClient client) {
        this.client = client;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setJmsConfig(JmsConfig jmsConfig) {
        this.jmsConfig = jmsConfig;
    }

    public void close() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }
}
