/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ltsoft.jms.util;

import com.ltsoft.jms.destination.*;

import javax.jms.Destination;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;

import static java.util.Objects.requireNonNull;

/**
 * from https://github.com/apache/qpid-jms/blob/master/qpid-jms-client/src/main/java/org/apache/qpid/jms/util/TypeConversionSupport.java
 */
public final class TypeConversionSupport {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final byte[] TRUE = {(byte) 0xc3};
    private static final byte[] FALSE = {(byte) 0xc2};

    private TypeConversionSupport() {
    }

    private static class ConversionKey {
        final Class<?> from;
        final Class<?> to;
        final int hashCode;

        ConversionKey(Class<?> from, Class<?> to) {
            this.from = from;
            this.to = to;
            this.hashCode = from.hashCode() ^ (to.hashCode() << 1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || o.getClass() != this.getClass()) {
                return false;
            }

            ConversionKey x = (ConversionKey) o;
            return x.from == from && x.to == to;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    @FunctionalInterface
    interface Converter<F, T> {
        T convert(F value);
    }

    private static final HashMap<ConversionKey, Converter<?, ?>> CONVERSION_MAP = new HashMap<>();

    static {

        Converter<Object, String> toStringConverter = Object::toString;
        CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);

        CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), (Converter<String, Boolean>) Boolean::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), (Converter<String, Byte>) Byte::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), (Converter<String, Short>) Short::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), (Converter<String, Integer>) Integer::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), (Converter<String, Long>) Long::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), (Converter<String, Float>) Float::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), (Converter<String, Double>) Double::valueOf);

        Converter<Number, Long> longConverter = Number::longValue;

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), (Converter<Date, Long>) Date::getTime);

        Converter<Number, Integer> intConverter = Number::intValue;
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), (Converter<Byte, Short>) Byte::shortValue);

        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), (Converter<Float, Double>) Float::doubleValue);

        CONVERSION_MAP.put(new ConversionKey(Boolean.class, byte[].class), (Boolean value) -> value ? TRUE : FALSE);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, byte[].class), (Byte value) -> new byte[]{value});
        CONVERSION_MAP.put(new ConversionKey(Short.class, byte[].class), (Short value) -> ByteBuffer.allocate(4).putShort(value).array());
        CONVERSION_MAP.put(new ConversionKey(Integer.class, byte[].class), (Integer value) -> ByteBuffer.allocate(4).putInt(value).array());
        CONVERSION_MAP.put(new ConversionKey(Long.class, byte[].class), (Long value) -> ByteBuffer.allocate(8).putLong(value).array());
        CONVERSION_MAP.put(new ConversionKey(Float.class, byte[].class), (Float value) -> ByteBuffer.allocate(4).putFloat(value).array());
        CONVERSION_MAP.put(new ConversionKey(Double.class, byte[].class), (Double value) -> ByteBuffer.allocate(8).putDouble(value).array());
        CONVERSION_MAP.put(new ConversionKey(String.class, byte[].class), (String value) -> value.getBytes(UTF8));

        Converter<Destination, byte[]> destinationConverter = (Destination value) -> value.toString().getBytes(UTF8);
        CONVERSION_MAP.put(new ConversionKey(JmsQueue.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTopic.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTemporaryQueue.class, byte[].class), destinationConverter);
        CONVERSION_MAP.put(new ConversionKey(JmsTemporaryTopic.class, byte[].class), destinationConverter);

        CONVERSION_MAP.put(new ConversionKey(byte[].class, Boolean.class), (byte[] value) -> value.length == 1 && value[0] == TRUE[0]);
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Byte.class), (byte[] value) -> value[0]);
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Short.class), (byte[] value) -> ByteBuffer.wrap(value).getShort());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Integer.class), (byte[] value) -> ByteBuffer.wrap(value).getInt());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Long.class), (byte[] value) -> ByteBuffer.wrap(value).getLong());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Float.class), (byte[] value) -> ByteBuffer.wrap(value).getFloat());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, Double.class), (byte[] value) -> ByteBuffer.wrap(value).getDouble());
        CONVERSION_MAP.put(new ConversionKey(byte[].class, String.class), (byte[] value) -> new String(value, UTF8));

        CONVERSION_MAP.put(new ConversionKey(byte[].class, Destination.class), (byte[] value) -> JmsDestination.valueOf(new String(value, UTF8)));
    }

    @SuppressWarnings("unchecked")
    public static <T> T convert(Object value, Class<T> toClass) {

        if (requireNonNull(value).getClass() == requireNonNull(toClass)) {
            return (T) value;
        }

        Class<?> fromClass = value.getClass();

        Converter<Object, T> c = (Converter<Object, T>) CONVERSION_MAP.get(new ConversionKey(fromClass, toClass));
        if (c == null) {
            throw new IllegalArgumentException(String.format("No converter for %s to %s", value.getClass().getName(), toClass.getSimpleName()));
        }

        return c.convert(value);
    }
}
