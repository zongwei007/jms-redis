package com.ltsoft.jms;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import java.io.InputStream;
import java.util.logging.LogManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 消息连接工厂测试
 */
public class JmsConnectionFactoryTest {

    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private RedissonClient client;

    @BeforeAll
    public static void beforeClass() throws Exception {
        InputStream is = JmsConnectionFactoryTest.class.getResourceAsStream("/logging.properties");
        LogManager.getLogManager().readConfiguration(is);
    }

    @BeforeEach
    public void setUp() throws Exception {
        client = Redisson.create();
    }

    @AfterEach
    public void tearDown() {
        client.shutdown();
    }

    @Test
    public void createConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(UnsupportedOperationException.class, factory::createConnection);
    }

    @Test
    public void createConnectionUserAndPassword() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(UnsupportedOperationException.class, () -> factory.createConnection(USER, PASSWORD));
    }

    @Test
    public void createContext() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setClientId("Client");
        factory.setRedissonClient(client);
        factory.setJmsConfig(new JmsConfig());

        JMSContext context = factory.createContext();

        assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());
    }

    @Test
    public void createContextSessionMode() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(JMSRuntimeException.class, () -> factory.createContext(JMSContext.SESSION_TRANSACTED));
    }

    @Test
    public void createContextUnsupportedSessionMode() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(JMSRuntimeException.class, () -> factory.createContext(10));
    }

    @Test
    public void createContextUserAndPassword() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(UnsupportedOperationException.class, () -> factory.createContext(USER, PASSWORD));
    }

    @Test
    public void createContextUserAndPasswordAndSessionModel() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();

        assertThrows(UnsupportedOperationException.class, () -> factory.createContext(USER, PASSWORD, 0));
    }

}
