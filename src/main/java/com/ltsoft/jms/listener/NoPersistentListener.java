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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ltsoft.jms.util.KeyHelper.getDestinationKey;
import static com.ltsoft.jms.util.ThreadPool.cachedPool;
import static com.ltsoft.jms.util.ThreadPool.scheduledPool;

/**
 * 监听 Redis 的消息发布
 */
public class NoPersistentListener extends BinaryJedisPubSub implements Listener {

    private Logger logger = Logger.getLogger(getClass().getName());

    private final JMSContextImpl context;
    private final String clientID;
    private final MessageListener listener;
    private final Destination destination;
    private final boolean noLocal;
    private ScheduledFuture<?> pingSchedule;

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
            logger.log(Level.WARNING, "NoPersistentListener can not read message from property", e);
        }
    }

    @Override
    public void start() {
        cachedPool().execute(() -> {
            try (Jedis client = context.pool().getResource()) {
                client.subscribe(this, getDestinationKey(destination).getBytes());
                // subscribe/unsubscribe 会使 client 的 pipelinedCommands 计数器增长
                // 导致连接在被 pipe 使用时由于与预期计数不符，造成 Read timed out 异常
                // 这个问题在 JedisPubSub 中已修复，BinaryJedisPubSub 中却没有……
                // 另外，JedisPubSub 中还添加了 ping 的支持；同样的，BinaryJedisPubSub 中也没有……
                // 解决方法是：使用定制版本的 Jedis :(
            }
        });

        //TODO 读取配置
        this.pingSchedule = scheduledPool().scheduleAtFixedRate(this::ping, 30, 30, TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        this.unsubscribe();
        pingSchedule.cancel(false);
    }
}
