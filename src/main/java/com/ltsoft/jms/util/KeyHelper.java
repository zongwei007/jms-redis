package com.ltsoft.jms.util;

import javax.jms.Destination;

/**
 * Redis Key 构建工具
 */
public class KeyHelper {

    private static final String DELIMITER = ":";

    private static final String PREFIX = "MESSAGE";
    private static final String CONSUMERS = "CONSUMERS";
    private static final String ID_BACKUP = "MSG_ID_BACKUP";

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

    /**
     * 消息目标消费者列表地址
     *
     * @param destination 消息目标
     * @return 消息目标消费者列表 Key
     */
    public static String getTopicConsumersKey(Destination destination) {
        return String.join(DELIMITER, PREFIX, destination.toString(), CONSUMERS);
    }

    /**
     * 消息实体消费者地址
     *
     * @param destination 消息目标
     * @param messageId   消息 ID
     * @return 消息实体消费者 Key
     */
    public static String getTopicItemConsumersKey(Destination destination, String messageId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), messageId, CONSUMERS);
    }

    /**
     * 消息目标消费者地址
     *
     * @param destination 消息目标
     * @param clientId    消费者 ID
     * @return 消息目标消费者地址
     */
    public static String getTopicConsumerListKey(Destination destination, String clientId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), clientId);
    }

    /**
     * 消息 ID 备份
     *
     * @param destination 消息目标
     * @param clientId    消费者ID
     * @return 消息 ID 备份地址
     */
    public static String getDestinationBackupKey(Destination destination, String clientId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), ID_BACKUP, clientId);
    }
}
