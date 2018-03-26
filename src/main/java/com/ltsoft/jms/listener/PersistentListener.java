package com.ltsoft.jms.listener;


import com.ltsoft.jms.JmsConsumerImpl;
import com.ltsoft.jms.JmsContextImpl;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * 阻塞式从队列获取消息，实现监听功能
 */
public class PersistentListener implements Listener {

    private static final Logger LOGGER = Logger.getLogger(PersistentListener.class.getName());

    private static final long DURATION = Duration.ofMinutes(1).toMillis();

    private final JmsContextImpl context;
    private final JmsConsumerImpl consumer;
    private final MessageListener listener;
    private boolean listening = true;

    public PersistentListener(JmsConsumerImpl consumer) {
        this.context = consumer.context();
        this.consumer = consumer;
        this.listener = consumer.getMessageListener();
    }

    @Override
    public void start() {
        context.cachedPool().execute(() -> {
            do {
                Message message = null;
                try {
                    message = consumer.receive(DURATION);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "PersistentListener can not read message from property", e);
                }

                if (message != null) {
                    listener.onMessage(message);
                }
            } while (listening);

            LOGGER.finest(() -> String.format("Client '%s' listener of '%s' is exist", context.getClientID(), consumer.getDestination()));
        });

        LOGGER.finest(() -> String.format("Client '%s' is listening to '%s'", context.getClientID(), consumer.getDestination()));
    }

    @Override
    public void stop() {
        this.listening = false;

        LOGGER.finest(() -> String.format("Client '%s' stop listening to '%s'", context.getClientID(), consumer.getDestination()));
    }
}
