package com.ltsoft.jms.destination;

import javax.jms.Destination;

import static com.ltsoft.jms.destination.JmsQueue.QUEUE;
import static com.ltsoft.jms.destination.JmsTopic.TOPIC;

/**
 * JMS 消息地址
 */
public abstract class JmsDestination implements Destination {

    public static JmsDestination valueOf(String value) {
        int pos = value.indexOf(':');
        String type = value.substring(0, pos);
        String name = value.substring(pos + 1);

        JmsDestination destination = null;
        switch (type) {
            case QUEUE:
                destination = new JmsQueue(name);
                break;
            case TOPIC:
                destination = new JmsTopic(name);
                break;
        }
        return destination;
    }
}
