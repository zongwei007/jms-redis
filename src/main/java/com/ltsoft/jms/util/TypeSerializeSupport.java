package com.ltsoft.jms.util;

import com.ltsoft.jms.destination.*;

import javax.jms.Destination;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * 序列化工具类
 */
public final class TypeSerializeSupport {

    private TypeSerializeSupport() {
        //禁用构造函数
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final byte[] TRUE = {(byte) 0xc3};
    private static final byte[] FALSE = {(byte) 0xc2};

    //JMS 序列化转换
    private static final HashMap<ConversionKey, Function<?, ?>> CONVERSION_MAP = new HashMap<>();

    private static Serializer serializer;

    private static Function<Object, byte[]> serializeConversion = (Object value) -> serializer.serialize(value);
    private static Function<byte[], Object> deserializeConversion = (byte[] value) -> serializer.deserialize(value);

    static {
        //加载序列化工具
        ServiceLoader<Serializer> serviceLoader = ServiceLoader.load(Serializer.class);
        Iterator<Serializer> serviceIterator = serviceLoader.iterator();
        if (serviceIterator.hasNext()) {
            serializer = serviceIterator.next();
        } else {
            serializer = new DefaultSerializer();
        }

        //序列化
        CONVERSION_MAP.put(new ConversionKey(Boolean.class, byte[].class), (Boolean value) -> value ? TRUE : FALSE);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, byte[].class), (Byte value) -> new byte[]{value});
        CONVERSION_MAP.put(new ConversionKey(Short.class, byte[].class), (Short value) -> ByteBuffer.allocate(4).putShort(value).array());
        CONVERSION_MAP.put(new ConversionKey(Character.class, byte[].class), (Character value) -> ByteBuffer.allocate(4).putChar(value).array());
        CONVERSION_MAP.put(new ConversionKey(Integer.class, byte[].class), (Integer value) -> ByteBuffer.allocate(4).putInt(value).array());
        CONVERSION_MAP.put(new ConversionKey(Long.class, byte[].class), (Long value) -> ByteBuffer.allocate(8).putLong(value).array());
        CONVERSION_MAP.put(new ConversionKey(Float.class, byte[].class), (Float value) -> ByteBuffer.allocate(4).putFloat(value).array());
        CONVERSION_MAP.put(new ConversionKey(Double.class, byte[].class), (Double value) -> ByteBuffer.allocate(8).putDouble(value).array());
        CONVERSION_MAP.put(new ConversionKey(String.class, byte[].class), (String value) -> value.getBytes(UTF8));

        CONVERSION_MAP.put(new ConversionKey(Serializable.class, byte[].class), serializeConversion);

        Function<Destination, byte[]> destinationConverter = (Destination value) -> value.toString().getBytes(UTF8);
        CONVERSION_MAP.put(new ConversionKey(JmsQueue.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTopic.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTemporaryQueue.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTemporaryTopic.class, byte[].class), destinationConverter);

        //反序列化
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Boolean.class), (byte[] value) -> value.length == 1 && value[0] == TRUE[0]);
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Byte.class), (byte[] value) -> value[0]);
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Short.class), (byte[] value) -> ByteBuffer.wrap(value).getShort());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Character.class), (byte[] value) -> ByteBuffer.wrap(value).getChar());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Integer.class), (byte[] value) -> ByteBuffer.wrap(value).getInt());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Long.class), (byte[] value) -> ByteBuffer.wrap(value).getLong());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Float.class), (byte[] value) -> ByteBuffer.wrap(value).getFloat());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Double.class), (byte[] value) -> ByteBuffer.wrap(value).getDouble());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, String.class), (byte[] value) -> new String(value, UTF8));

        CONVERSION_MAP.put(new ConversionKey(byte[].class, Serializable.class), deserializeConversion);

        CONVERSION_MAP.put(new ConversionKey(byte[].class, Destination.class), (byte[] value) -> JmsDestination.valueOf(new String(value, UTF8)));
    }

    @SuppressWarnings("unchecked")
    public static byte[] serialize(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> fromClass = value.getClass();
        if (fromClass == byte[].class) {
            return (byte[]) value;
        }

        Function<Object, byte[]> c = (Function<Object, byte[]>) CONVERSION_MAP.get(new ConversionKey(fromClass, byte[].class));
        if (c == null) {
            if (value instanceof Serializable) {
                return serializeConversion.apply(value);
            }
            throw new IllegalArgumentException(String.format("Can not serialize %s to byte[]", fromClass.getName()));
        }

        return c.apply(value);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] value, Class<T> toClass) {
        if (value == null) {
            return null;
        }

        if (toClass == byte[].class || toClass == Object.class) {
            return (T) value;
        }

        Function<byte[], T> c = (Function<byte[], T>) CONVERSION_MAP.get(new ConversionKey(byte[].class, toClass));
        if (c == null) {
            if (Serializable.class.isAssignableFrom(toClass)) {
                return (T) deserializeConversion.apply(value);
            }
            throw new IllegalArgumentException(String.format("Can not deserialize byte[] to %s", toClass.getName()));
        }

        return c.apply(value);
    }

}
