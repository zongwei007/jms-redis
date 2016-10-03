package com.ltsoft.jms;

import org.junit.Test;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;

import static org.junit.Assert.assertEquals;


public class JmsConnectionFactoryTest {

    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";

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
        JMSContext context = factory.createContext();

        assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void createContextSessionMode() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        JMSContext context = factory.createContext(JMSContext.SESSION_TRANSACTED);

        assertEquals(context.getSessionMode(), JMSContext.SESSION_TRANSACTED);
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
