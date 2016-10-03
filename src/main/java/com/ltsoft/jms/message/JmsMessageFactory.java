package com.ltsoft.jms.message;

import com.ltsoft.jms.exception.JMSExceptionSupport;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Interface that a Provider should implement to provide a Provider
 * Specific JmsMessage implementation that optimizes the exchange of
 * message properties and payload between the JMS Message API and the
 * underlying Provider Message implementations.
 */
public class JmsMessageFactory {

    /**
     * Creates an instance of a basic JmsMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsMessage instance.
     */
    public JmsMessage createMessage() {
        return new JmsMessage();
    }

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param text The value to initially assign to the Message body, or null if empty to start.
     * @return a newly created and initialized JmsTextMessage instance.
     * if the provider cannot create the message for some reason.
     */
    public JmsTextMessage createTextMessage(String text) {
        JmsTextMessage message = new JmsTextMessage();

        try {
            if (text != null) {
                message.setText(text);
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }

        return message;
    }

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     */
    public JmsTextMessage createTextMessage() {
        return createTextMessage(null);
    }

    /**
     * Creates an instance of a basic JmsBytesMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     */
    public JmsBytesMessage createBytesMessage() {
        return new JmsBytesMessage();
    }

    /**
     * Creates an instance of a basic JmsMapMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     */
    public JmsMapMessage createMapMessage() {
        return new JmsMapMessage();
    }

    /**
     * Creates an instance of a basic JmsStreamMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     */
    public JmsStreamMessage createStreamMessage() {
        return new JmsStreamMessage();
    }

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param obj The value to initially assign to the Message body, or null if empty to start.
     * @return a newly created and initialized JmsObjectMessage instance.
     */
    public JmsObjectMessage createObjectMessage(Serializable obj) {
        JmsObjectMessage message = new JmsObjectMessage();

        try {
            if (obj != null) {
                message.setObject(obj);
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.wrap(e);
        }

        return message;
    }

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsObjectMessage instance.
     */
    public JmsObjectMessage createObjectMessage() {
        return createObjectMessage(null);
    }

}
