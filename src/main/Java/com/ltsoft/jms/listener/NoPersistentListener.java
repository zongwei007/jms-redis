package com.ltsoft.jms.listener;

import com.ltsoft.jms.JMSConsumerImpl;
import com.ltsoft.jms.JMSContextImpl;
import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.message.JmsMessageHelper;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import java.util.Objects;

import static com.ltsoft.jms.util.KeyHelper.getDestinationKey;
import static com.ltsoft.jms.util.ThreadPool.cachedPool;

/**
 * 监听 Redis 的消息发布
 */
public class NoPersistentListener extends BinaryJedisPubSub implements Listener {


    private final JMSContextImpl context;
    private final String clientID;
    private final MessageListener listener;
    private final Destination destination;
    private final boolean noLocal;

    public NoPersistentListener(JMSContextImpl context, JMSConsumerImpl consumer) {
        this.context = context;
        this.clientID = context.getClientID();
        this.listener = consumer.getMessageListener();
        this.destination = consumer.getDestination();
        this.noLocal = consumer.isNoLocal();
    }

    @Override
    public void onMessage(byte[] channel, byte[] bytes) {
        try {
            JmsMessage message = JmsMessageHelper.fromBytes(bytes);
            if (noLocal && Objects.equals(message.getJMSXMessageFrom(), clientID)) {
                return;
            }

            message.setReadOnly(true);

            listener.onMessage(message);
        } catch (JMSException e) {
            //TODO LOGGER
            //LOGGER.error("NoPersistentListener can not read message from property", e);
        }
    }

    @Override
    public void start() {
        cachedPool().execute(() -> {
            try (Jedis client = context.pool().getResource()) {
                client.subscribe(this, getDestinationKey(destination).getBytes());
                // subscribe/unsubscribe 会将 client 的 pipelinedCommands 计数器增长
                // 导致连接在被 pipe 使用时由于与预期计数不符造成 Read timed out 异常
                // 重置计数器以避免此类情况
                client.getClient().resetPipelinedCount();
            }
        });
    }

    @Override
    public void stop() {
        this.unsubscribe();
    }
}
