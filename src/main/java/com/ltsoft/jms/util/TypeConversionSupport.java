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

import java.util.Date;
import java.util.HashMap;

/**
 * from https://github.com/apache/qpid-jms/blob/master/qpid-jms-client/src/main/java/org/apache/qpid/jms/util/TypeConversionSupport.java
 */
public final class TypeConversionSupport {

    private TypeConversionSupport() {}

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
    interface Converter<T> {
        T convert(Object value);
    }

    private static final HashMap<ConversionKey, Converter<?>> CONVERSION_MAP = new HashMap<>();

    static {
        Converter<String> toStringConverter = Object::toString;

        CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);

        CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), value -> Boolean.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), value -> Byte.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), value -> Short.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), value -> Integer.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), value -> Long.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), value -> Float.valueOf((String) value));
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), value -> Double.valueOf((String) value));

        Converter<Long> longConverter = value -> ((Number) value).longValue();

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), value -> ((Date) value).getTime());

        Converter<Integer> intConverter = value -> ((Number) value).intValue();
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), value -> ((Number) value).shortValue());

        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), value -> ((Number) value).doubleValue());
    }

    @SuppressWarnings("unchecked")
    public static <T> T convert(Object value, Class<T> toClass) {

        assert value != null && toClass != null;

        if (value.getClass() == toClass) {
            return (T) value;
        }

        Class<?> fromClass = value.getClass();

        if (fromClass.isPrimitive()) {
            fromClass = convertPrimitiveTypeToWrapperType(fromClass);
        }

        if (toClass.isPrimitive()) {
            toClass = (Class<T>) convertPrimitiveTypeToWrapperType(toClass);
        }

        Converter<T> c = (Converter<T>) CONVERSION_MAP.get(new ConversionKey(fromClass, toClass));
        if (c == null) {
            return null;
        }

        return c.convert(value);
    }

    private static Class<?> convertPrimitiveTypeToWrapperType(Class<?> type) {
        Class<?> rc = type;
        if (type.isPrimitive()) {
            if (type == int.class) {
                rc = Integer.class;
            } else if (type == long.class) {
                rc = Long.class;
            } else if (type == double.class) {
                rc = Double.class;
            } else if (type == float.class) {
                rc = Float.class;
            } else if (type == short.class) {
                rc = Short.class;
            } else if (type == byte.class) {
                rc = Byte.class;
            } else if (type == boolean.class) {
                rc = Boolean.class;
            }
        }

        return rc;
    }
}