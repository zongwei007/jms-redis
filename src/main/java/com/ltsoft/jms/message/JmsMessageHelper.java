package com.ltsoft.jms.message;

import com.ltsoft.jms.destination.JmsDestination;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeConversionSupport;

import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by zongwei on 16-7-19.
 */
public final class JmsMessageHelper {

    private static final String ID_PREFIX = "ID:";

    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_TYPE = "JMSType";

    public static final String JMSX_BODY = "JMSXBody";

    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    public static final String JMSX_GROUPID = "JMSXGroupID";
    public static final String JMSX_GROUPSEQ = "JMSXGroupSeq";
    public static final String JMSX_USERID = "JMSXUserID";

    private static final Map<Class, Byte> TYPE_TO_CODE = new HashMap<>();
    private static final Map<Byte, Class> CODE_TO_TYPE = new HashMap<>();

    static {
        TYPE_TO_CODE.put(Boolean.class, (byte) 0xc0);

        TYPE_TO_CODE.put(Float.class, (byte) 0xca);
        TYPE_TO_CODE.put(Double.class, (byte) 0xcb);

        TYPE_TO_CODE.put(Short.class, (byte) 0xd0);
        TYPE_TO_CODE.put(Integer.class, (byte) 0xd1);
        TYPE_TO_CODE.put(Long.class, (byte) 0xd2);

        TYPE_TO_CODE.put(Byte.class, (byte) 0xd8);
        TYPE_TO_CODE.put(String.class, (byte) 0xd9);

        TYPE_TO_CODE.forEach((type, code) -> CODE_TO_TYPE.put(code, type));
    }

    private JmsMessageHelper() {
        //禁用构造函数
    }

    private static String compressUUID(UUID uuid) {
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.putLong(uuid.getMostSignificantBits());
        bytes.putLong(uuid.getLeastSignificantBits());

        return Base64.getUrlEncoder().encodeToString(bytes.array()).substring(0, 22);
    }

    private static UUID decompressUUID(String compressedUuid) {
        if (compressedUuid.length() != 22) {
            throw new IllegalArgumentException("Invalid uuid!");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(compressedUuid + "=="));
        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }

    public static String getMessageId() {
        return ID_PREFIX + compressUUID(UUID.randomUUID());
    }

    public static UUID decodeMessageId(String messageId) {
        if (messageId.length() != 25) {
            throw new IllegalArgumentException("Invalid MessageId!");
        }

        return decompressUUID(messageId.substring(ID_PREFIX.length()));
    }

    public static Map<byte[], byte[]> toMap(JmsMessage message) throws JMSException {
        Map<byte[], byte[]> map = new HashMap<>();

        if (message.getJMSCorrelationID() != null) {
            map.put(JMS_CORRELATION_ID.getBytes(), message.getJMSCorrelationIDAsBytes());
        }


        map.put(JMS_DELIVERY_MODE.getBytes(), String.valueOf(message.getJMSDeliveryMode()).getBytes());
        if (message.getJMSDestination() != null) {
            map.put(JMS_DESTINATION.getBytes(), String.valueOf(message.getJMSDestination()).getBytes());
        }
        if (message.getJMSExpiration() > 0) {
            map.put(JMS_EXPIRATION.getBytes(), String.valueOf(message.getJMSExpiration()).getBytes());
        }
        map.put(JMS_PRIORITY.getBytes(), String.valueOf(message.getJMSPriority()).getBytes());
        map.put(JMS_REDELIVERED.getBytes(), String.valueOf(message.getJMSRedelivered()).getBytes());
        if (message.getJMSReplyTo() != null) {
            map.put(JMS_REPLY_TO.getBytes(), String.valueOf(message.getJMSReplyTo()).getBytes());
        }
        if (message.getJMSTimestamp() > 0) {
            map.put(JMS_TIMESTAMP.getBytes(), String.valueOf(message.getJMSTimestamp()).getBytes());
        }
        map.put(JMS_TYPE.getBytes(), message.getJMSType().getBytes());

        Class<byte[]> toClass = byte[].class;
        Enumeration<String> enumeration = message.getPropertyNames();
        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();

            Object property = message.getObjectProperty(name);
            byte[] data = TypeConversionSupport.convert(property, toClass);
            byte[] result = new byte[data.length + 1];
            result[0] = TYPE_TO_CODE.get(property.getClass());
            System.arraycopy(data, 0, result, 1, data.length);

            map.put(name.getBytes(), result);
        }

        return map;
    }

    @SuppressWarnings("unchecked")
    public static <T extends JmsMessage> T fromMap(Map<byte[], byte[]> propBytes) throws JMSException {
        byte[] typeKey = JMS_TYPE.getBytes();
        if (propBytes.containsKey(typeKey)) {
            JmsMessage message = null;
            switch (MessageType.valueOf(new String(propBytes.get(typeKey)))) {
                case Basic:
                    message = new JmsMessage();
                    break;
                case Bytes:
                    message = new JmsBytesMessage();
                    break;
                case Map:
                    message = new JmsMapMessage();
                    break;
                case Stream:
                    message = new JmsStreamMessage();
                    break;
                case Object:
                    message = new JmsObjectMessage();
                    break;
                case Text:
                    message = new JmsTextMessage();
                    break;
            }

            for (Map.Entry<byte[], byte[]> entry : propBytes.entrySet()) {
                String key = new String(entry.getKey());
                String value = new String(entry.getValue());
                switch (key) {
                    case JMS_CORRELATION_ID:
                        message.setJMSCorrelationID(value);
                        break;
                    case JMS_DELIVERY_MODE:
                        message.setJMSDeliveryMode(Integer.valueOf(value));
                        break;
                    case JMS_DESTINATION:
                        message.setJMSDestination(JmsDestination.valueOf(value));
                        break;
                    case JMS_EXPIRATION:
                        message.setJMSExpiration(Long.valueOf(value));
                        break;
                    case JMS_PRIORITY:
                        message.setJMSPriority(Integer.valueOf(value));
                        break;
                    case JMS_REDELIVERED:
                        message.setJMSRedelivered(Boolean.valueOf(value));
                        break;
                    case JMS_REPLY_TO:
                        message.setJMSReplyTo(JmsDestination.valueOf(value));
                        break;
                    case JMS_TIMESTAMP:
                        message.setJMSTimestamp(Long.valueOf(value));
                        break;
                    case JMS_TYPE:
                        break;
                    default:
                        byte[] bytes = entry.getValue();
                        byte[] data = Arrays.copyOfRange(bytes, 1, bytes.length);
                        Class type = CODE_TO_TYPE.get(bytes[0]);
                        message.setObjectProperty(key, TypeConversionSupport.convert(data, type));
                }
            }


            return (T) message;
        }

        throw new MessageNotReadableException("Unknown message type");
    }

}

