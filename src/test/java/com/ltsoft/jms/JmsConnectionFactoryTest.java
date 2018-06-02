package com.ltsoft.jms;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import java.io.InputStream;
import java.util.logging.LogManager;

import static org.junit.Assert.assertEquals;


/**
 * 消息连接工厂测试
 */
public class JmsConnectionFactoryTest {

    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private RedissonClient client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        InputStream is = JmsConnectionFactoryTest.class.getResourceAsStream("/logging.properties");
        LogManager.getLogManager().readConfiguration(is);
    }

    @Before
    public void setUp() throws Exception {
        client = Redisson.create();
    }

    @After
    public void tearDown() {
        client.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createConnection();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createConnectionUserAndPassword() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createConnection(USER, PASSWORD);
    }

    @Test
    public void createContext() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setClientId("Client");
        factory.setRedissonClient(client);
        factory.setJmsConfig(new JmsConfig());

        JMSContext context = factory.createContext();

        assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Test(expected = JMSRuntimeException.class)
    public void createContextSessionMode() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createContext(JMSContext.SESSION_TRANSACTED);
    }

    @Test(expected = JMSRuntimeException.class)
    public void createContextUnsupportedSessionMode() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createContext(10);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createContextUserAndPassword() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createContext(USER, PASSWORD);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createContextUserAndPasswordAndSessionModel() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.createContext(USER, PASSWORD, 0);
    }

}
