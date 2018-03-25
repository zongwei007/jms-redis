package com.ltsoft.jms;

import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.message.JmsMessage;
import org.redisson.api.RedissonClient;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

import static com.ltsoft.jms.message.JmsMessageHelper.fromMap;
import static com.ltsoft.jms.util.KeyHelper.*;

/**
 * 队列浏览器
 */
public class JmsQueueBrowserImpl implements QueueBrowser, AutoCloseable {
    private final Queue queue;
    private final RedissonClient client;

    JmsQueueBrowserImpl(Queue queue, JmsContextImpl context) {
        this.queue = queue;
        this.client = context.client();
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
        //do nothing
    }

    private class QueueEnumeration implements Enumeration {

        private final Queue queue;
        private final RedissonClient client;
        private final Iterator<String> keyIterator;

        QueueEnumeration(Queue queue, RedissonClient client) {
            this.queue = queue;
            this.client = client;
            this.keyIterator = client.<String>getList(getDestinationKey(queue)).readAll().iterator();
        }

        @Override
        public boolean hasMoreElements() {
            return keyIterator.hasNext();
        }

        @Override
        public Object nextElement() {
            String messageId = keyIterator.next();
            String propsKey = getDestinationPropsKey(queue, messageId);
            try {

                Map<String, byte[]> props = client.<String, byte[]>getMap(propsKey).readAllMap();
                if (props.size() == 0) {
                    //消息有可能已过期
                    return null;
                }

                JmsMessage message = fromMap(props);
                message.setJMSMessageID(messageId);

                byte[] body = client.<byte[]>getBucket(getDestinationBodyKey(queue, messageId)).get();
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
