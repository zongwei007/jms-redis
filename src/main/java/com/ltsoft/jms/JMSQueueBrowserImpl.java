package com.ltsoft.message;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.Iterator;

import static com.ltsoft.message.util.KeyUtil.getDestinationKey;

/**
 * 队列浏览器
 */
public class JMSQueueBrowserImpl implements QueueBrowser {
    private final JedisPool pool;
    private final Queue queue;
    private final String messageSelector;

    JMSQueueBrowserImpl(JedisPool jedisPool, Queue queue, String messageSelector) {
        this.pool = jedisPool;
        this.queue = queue;
        this.messageSelector = messageSelector;
    }

    @Override
    public Queue getQueue() throws JMSException {
        return queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return messageSelector;
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
            this.queueKey = getDestinationKey(queue);
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
//            byte[] messageKey = getDestinationItemKey(queue, messageId);
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
