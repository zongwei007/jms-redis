package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.message.JmsMessageHelper;
import redis.clients.jedis.Jedis;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * 队列浏览器
 */
public class JMSQueueBrowserImpl implements QueueBrowser, AutoCloseable {
    private final Queue queue;
    private final Jedis client;

    JMSQueueBrowserImpl(Queue queue, JMSContextImpl context) {
        this.queue = queue;
        this.client = context.pool().getResource();
    }

    @Override
    public Queue getQueue() throws JMSException {
        return queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return null;
    }

    @Override
    public Enumeration getEnumeration() throws JMSException {
        return new QueueEnumeration(queue, client);
    }

    @Override
    public void close() throws JMSException {
        client.close();
    }

    private class QueueEnumeration implements Enumeration {

        private final Queue queue;
        private final Jedis client;
        private final Iterator<String> keyIterator;

        QueueEnumeration(Queue queue, Jedis client) {
            this.queue = queue;
            this.client = client;
            this.keyIterator = client.lrange(getDestinationKey(queue), 0, -1).iterator();
        }

        @Override
        public boolean hasMoreElements() {
            return keyIterator.hasNext();
        }

        @Override
        public Object nextElement() {
            String messageId = keyIterator.next();
            byte[] propsKey = getDestinationPropsKey(queue, messageId);
            try {
                Map<String, byte[]> props = JmsMessageHelper.toStringKey(client.hgetAll(propsKey));
                if (props == null) {
                    //消息有可能已过期
                    client.close();
                    return nextElement();
                }

                JmsMessage message = JmsMessageHelper.fromMap(props);
                message.setJMSMessageID(messageId);

                byte[] body = client.get(getDestinationBodyKey(queue, messageId));
                if (body != null) {
                    message.setBody(body);
                }

                return message;
            } catch (JMSException e) {
                throw JMSExceptionSupport.wrap(e);
            }
        }
    }
}
