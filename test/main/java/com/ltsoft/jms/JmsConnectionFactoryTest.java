package com.ltsoft.jms;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;

import static org.junit.Assert.assertEquals;


public class JmsConnectionFactoryTest {

    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

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
        JMSContext context = factory.createContext();

        assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void createContextSessionMode() throws Exception {

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
