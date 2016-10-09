package com.ltsoft.jms.message;

import com.ltsoft.jms.destination.JmsDestination;
import com.ltsoft.jms.exception.JMSExceptionSupport;
import com.ltsoft.jms.util.MessageType;
import com.ltsoft.jms.util.TypeSerializeSupport;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotReadableException;
import java.io.*;
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

    private static final Byte NULL_CODE = (byte) 0xc0;

    static {
        TYPE_TO_CODE.put(Boolean.class, (byte) 0xc0);

        TYPE_TO_CODE.put(Float.class, (byte) 0xca);
        TYPE_TO_CODE.put(Double.class, (byte) 0xcb);

        TYPE_TO_CODE.put(Short.class, (byte) 0xd0);
        TYPE_TO_CODE.put(Integer.class, (byte) 0xd1);
        TYPE_TO_CODE.put(Long.class, (byte) 0xd2);

        TYPE_TO_CODE.put(Byte.class, (byte) 0xd8);
        TYPE_TO_CODE.put(String.class, (byte) 0xd9);

        TYPE_TO_CODE.put(byte[].class, (byte) 0x90);

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

    public static Map<String, byte[]> toStringKey(Map<byte[], byte[]> source) {
        Map<String, byte[]> result = new HashMap<>();
        source.forEach((key, value) -> result.put(new String(key), value));
        return result;
    }

    public static Map<byte[], byte[]> toBytesKey(Map<String, byte[]> source) {
        Map<byte[], byte[]> result = new HashMap<>();
        source.forEach((key, value) -> result.put(key.getBytes(), value));
        return result;
    }

    public static Map<String, byte[]> toMap(JmsMessage message) throws JMSException {
        Map<String, byte[]> map = new HashMap<>();

        if (message.getJMSCorrelationID() != null) {
            map.put(JMS_CORRELATION_ID, message.getJMSCorrelationIDAsBytes());
        }

        map.put(JMS_DELIVERY_MODE, String.valueOf(message.getJMSDeliveryMode()).getBytes());
        if (message.getJMSDestination() != null) {
            map.put(JMS_DESTINATION, String.valueOf(message.getJMSDestination()).getBytes());
        }
        if (message.getJMSExpiration() > 0) {
            map.put(JMS_EXPIRATION, String.valueOf(message.getJMSExpiration()).getBytes());
        }
        map.put(JMS_PRIORITY, String.valueOf(message.getJMSPriority()).getBytes());
        map.put(JMS_REDELIVERED, String.valueOf(message.getJMSRedelivered()).getBytes());
        if (message.getJMSReplyTo() != null) {
            map.put(JMS_REPLY_TO, String.valueOf(message.getJMSReplyTo()).getBytes());
        }
        if (message.getJMSTimestamp() > 0) {
            map.put(JMS_TIMESTAMP, String.valueOf(message.getJMSTimestamp()).getBytes());
        }
        map.put(JMS_TYPE, message.getJMSType().getBytes());

        Enumeration<String> enumeration = message.getPropertyNames();
        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();

            byte[] result;
            Object property = message.getObjectProperty(name);
            if (property != null) {
                byte[] data = TypeSerializeSupport.serialize(property);
                result = new byte[data.length + 1];
                result[0] = TYPE_TO_CODE.get(property.getClass());
                System.arraycopy(data, 0, result, 1, data.length);
            } else {
                result = new byte[]{NULL_CODE};
            }

            map.put(name, result);
        }

        return map;
    }

    @SuppressWarnings("unchecked")
    public static <T extends JmsMessage> T fromMap(Map<String, byte[]> propBytes) throws JMSException {
        if (propBytes.containsKey(JMS_TYPE)) {
            JmsMessage message = null;
            MessageType messageType = MessageType.valueOf(new String(propBytes.get(JMS_TYPE)));
            switch (messageType) {
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

            for (Map.Entry<String, byte[]> entry : propBytes.entrySet()) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                switch (key) {
                    case JMS_CORRELATION_ID:
                        message.setJMSCorrelationID(new String(value));
                        break;
                    case JMS_DELIVERY_MODE:
                        message.setJMSDeliveryMode(Integer.valueOf(new String(value)));
                        break;
                    case JMS_DESTINATION:
                        message.setJMSDestination(JmsDestination.valueOf(new String(value)));
                        break;
                    case JMS_EXPIRATION:
                        message.setJMSExpiration(Long.valueOf(new String(value)));
                        break;
                    case JMS_PRIORITY:
                        message.setJMSPriority(Integer.valueOf(new String(value)));
                        break;
                    case JMS_REDELIVERED:
                        message.setJMSRedelivered(Boolean.valueOf(new String(value)));
                        break;
                    case JMS_REPLY_TO:
                        message.setJMSReplyTo(JmsDestination.valueOf(new String(value)));
                        break;
                    case JMS_TIMESTAMP:
                        message.setJMSTimestamp(Long.valueOf(new String(value)));
                        break;
                    case JMSX_BODY:
                        message.setBody(value);
                        break;
                    case JMS_TYPE:
                        break;
                    default:
                        byte[] bytes = entry.getValue();
                        byte[] data = Arrays.copyOfRange(bytes, 1, bytes.length);
                        Class type = CODE_TO_TYPE.get(bytes[0]);
                        if (type != null) {
                            message.setObjectProperty(key, TypeSerializeSupport.deserialize(data, type));
                        } else if (NULL_CODE.equals(bytes[0])) {
                            message.setObjectProperty(key, null);
                        }

                }
            }


            return (T) message;
        }

        throw new MessageNotReadableException("Unknown message type");
    }

    public static byte[] toBytes(JmsMessage message) throws JMSException {
        Map<String, byte[]> map = toMap(message);

        map.put(JMSX_BODY, message.getBody());

        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             DataOutputStream writer = new DataOutputStream(os)) {

            for (Map.Entry<String, byte[]> entry : map.entrySet()) {
                byte[] key = entry.getKey().getBytes();
                byte[] value = entry.getValue();
                writer.writeInt(key.length);
                writer.write(key);
                writer.writeInt(value.length);
                writer.write(value);
            }

            return os.toByteArray();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    public static <T extends JmsMessage> T fromBytes(byte[] bytes) throws JMSException {
        Map<String, byte[]> map = new HashMap<>();

        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes);
             DataInputStream reader = new DataInputStream(is)) {

            while (true) {
                int keyLen;
                try {
                    keyLen = reader.readInt();
                } catch (EOFException e) {
                    break;
                }
                byte[] key = new byte[keyLen];
                if (reader.read(key) == -1) {
                    throw new MessageEOFException("read key fail");
                }
                int valueLen = reader.readInt();
                byte[] value = new byte[valueLen];
                if (reader.read(value) == -1) {
                    throw new MessageEOFException("read value fail");
                }
                map.put(new String(key), value);
            }
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }

        return fromMap(map);
    }
}

