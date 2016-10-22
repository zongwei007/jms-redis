package com.ltsoft.jms.util;

import javax.jms.Destination;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

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

    /**
     * 消息目标消费者列表地址
     *
     * @param destination 消息目标
     * @return 消息目标消费者列表 Key
     */
    public static String getTopicConsumersKey(Destination destination) {
        return String.join(DELIMITER, PREFIX, destination.toString(), "CONSUMERS");
    }

    /**
     * 消息实体消费者地址
     *
     * @param destination 消息目标
     * @param messageId   消息 ID
     * @return 消息实体消费者 Key
     */
    public static String getTopicItemConsumersKey(Destination destination, String messageId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), messageId, "CONSUMERS");
    }

    /**
     * 消息目标消费者地址
     *
     * @param destination 消息目标
     * @param instanceId  消费者 ID
     * @return 消息目标消费者地址
     */
    public static String getTopicConsumerListKey(Destination destination, String instanceId) {
        return String.join(DELIMITER, PREFIX, destination.toString(), instanceId);
    }

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmss");

    public static String getBackupTimeFix(Duration backupDuration) {
        long seconds = backupDuration.getSeconds();
        return TIME_FORMATTER.format(LocalTime.ofSecondOfDay(LocalTime.now().toSecondOfDay() / seconds * seconds));
    }

    public static String getDestinationBackupPattern(String instanceId, String pattern) {
        return String.join(DELIMITER, PREFIX, "BACKUP", instanceId, pattern);
    }

    public static String getDestinationBackupKey(Destination destination, String instanceId, Duration backupDuration) {
        return getDestinationBackupPattern(instanceId, destination.toString() + ":" + getBackupTimeFix(backupDuration));
    }

}
