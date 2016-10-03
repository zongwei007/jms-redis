package com.ltsoft.jms.destination;

import javax.jms.Destination;

/**
 * Created by zongw on 2016/9/26.
 */
public abstract class JmsDestination implements Destination {

    public static JmsDestination valueOf(String value) {
        String[] str = value.split(":");
        JmsDestination destination = null;
        switch (str[0]) {
            case "QUEUE":
                destination = new JmsQueue(str[1]);
                break;
            case "TOPIC":
                destination = new JmsTopic(str[1]);
                break;
        }
        return destination;
    }
}
