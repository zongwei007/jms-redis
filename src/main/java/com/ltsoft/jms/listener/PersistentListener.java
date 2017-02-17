package com.ltsoft.jms.listener;


import com.ltsoft.jms.JMSConsumerImpl;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.time.Duration;

import static com.ltsoft.jms.util.ThreadPool.cachedPool;


/**
 * 阻塞式从队列获取消息，实现监听功能
 */
public class PersistentListener implements Listener {

    private static final int DURATION = (int) Duration.ofMinutes(1).getSeconds();

    private final JMSConsumerImpl consumer;
    private final MessageListener listener;
    private boolean listening = true;

    public PersistentListener(JMSConsumerImpl consumer) {
        this.consumer = consumer;
        this.listener = consumer.getMessageListener();
    }

    @Override
    public void start() {
        cachedPool().execute(() -> {
            do {
                Message message = consumer.receive(DURATION);
                if (message != null) {
                    listener.onMessage(message);
                }
            } while (listening);
        });
    }

    @Override
    public void stop() {
        this.listening = false;
    }
}
