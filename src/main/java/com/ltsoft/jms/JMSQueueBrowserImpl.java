package com.ltsoft.jms;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * 队列浏览器
 */
public class JMSQueueBrowserImpl implements QueueBrowser {
    private final JedisPool pool;
    private final Queue queue;

    JMSQueueBrowserImpl(Queue queue, JedisPool jedisPool) {
        this.queue = queue;
        this.pool = jedisPool;
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
        return new QueueEnumeration(queue);
    }

    @Override
    public void close() throws JMSException {

    }

    public class QueueEnumeration implements Enumeration {

        private final String queueKey;
        private final Iterator<String> keyIterator;

        QueueEnumeration(Queue queue) {
            this.queueKey = queue.toString();
            try (Jedis client = pool.getResource()) {
                this.keyIterator = client.lrange(queueKey, 0, -1).iterator();
            }
        }

        @Override
        public boolean hasMoreElements() {
            return keyIterator.hasNext();
        }

        @Override
        public Object nextElement() {
//            String messageId = keyIterator.next();
//            byte[] messageKey = getDestinationPropsKey(queue, messageId);
//            try (Jedis client = pool.getResource()) {
//                byte[] bytes = client.get(messageKey);
//                return bytes == null ? null : getObjectMapper().readValue(bytes, Message.class);
//            } catch (IOException e) {
//                throw new JMSRuntimeException(e.getMessage(), "", e);
//            }
            return null;
        }
    }
}
