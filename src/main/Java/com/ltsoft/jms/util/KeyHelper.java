package com.ltsoft.jms.util;

import javax.jms.Destination;

/**
 * Created by zongw on 2016/10/3.
 */
public class KeyHelper {

    private static String DELIMITER = ":";

    private static String PREFIX = "MESSAGE";

    /**
     * 消息目标地址
     *
     * @param destination 消息目标
     * @return 消息目标 Key
     */
    public static String getDestinationKey(Destination destination) {
        return String.join(DELIMITER, PREFIX, destination.toString());
    }

    /**
     * 消息实体地址
     *
     * @param destination 消息目标
     * @param messageId   消息 ID
     * @return 消息实体 Key
     */
    public static byte[] getDestinationPropsKey(Destination destination, String messageId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), messageId).getBytes();
    }

    /**
     * 消息内容地址
     *
     * @param destination 消息目标
     * @param messageId   消息ID
     * @return 消息内容 Key
     */
    public static byte[] getDestinationBodyKey(Destination destination, String messageId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), messageId, "BODY").getBytes();
    }

}
