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

import java.util.HashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * <pre>
 * |        | boolean byte short char int long float double String byte[]
 * |----------------------------------------------------------------------
 * |boolean |    X                                            X
 * |byte    |          X     X         X   X                  X
 * |short   |                X         X   X                  X
 * |char    |                     X                           X
 * |int     |                          X   X                  X
 * |long    |                              X                  X
 * |float   |                                    X     X      X
 * |double  |                                          X      X
 * |String  |    X     X     X         X   X     X     X      X
 * |byte[]  |                                                        X
 * |----------------------------------------------------------------------
 * </pre>
 */
public final class TypeConversionSupport {

    private TypeConversionSupport() {
    }

    //JMS 基本类型转换
    private static final HashMap<ConversionKey, Function<?, ?>> CONVERSION_MAP = new HashMap<>();

    static {

        Function<Object, String> toStringConverter = Object::toString;
        Function<Number, Long> longConvert = Number::longValue;

        //Boolean
        CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
        //Byte
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), (Function<Byte, Short>) Byte::shortValue);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), (Function<Byte, Integer>) Byte::intValue);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), (Function<Byte, Long>) Byte::longValue);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
        //Short
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), (Function<Number, Integer>) Number::intValue);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConvert);
        CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
        //Character
        CONVERSION_MAP.put(new ConversionKey(Character.class, String.class), toStringConverter);
        //Integer
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConvert);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
        //Long
        CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
        //Float
        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), (Function<Number, Double>) Number::doubleValue);
        CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
        //Double
        CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);
        //String
        CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), (Function<String, Boolean>) Boolean::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), (Function<String, Byte>) Byte::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), (Function<String, Short>) Short::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), (Function<String, Integer>) Integer::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), (Function<String, Long>) Long::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), (Function<String, Float>) Float::valueOf);
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), (Function<String, Double>) Double::valueOf);
    }

    /**
     * @param value
     * @param toClass 转换的类型
     * @return
     */
    public static boolean can(Object value, Class<?> toClass) {
        if (value == null) {
            return false;
        }

        if (value.getClass() == requireNonNull(toClass)) {
            return true;
        }

        Class<?> fromClass = value.getClass();

        return CONVERSION_MAP.containsKey(new ConversionKey(fromClass, toClass));
    }

    @SuppressWarnings("unchecked")
    public static <T> T convert(Object value, Class<T> toClass) {
        if (value == null) {
            return null;
        }

        if (value.getClass() == requireNonNull(toClass) || toClass == Object.class) {
            return (T) value;
        }

        Class<?> fromClass = value.getClass();

        Function<Object, T> c = (Function<Object, T>) CONVERSION_MAP.get(new ConversionKey(fromClass, toClass));
        if (c == null) {
            throw new IllegalArgumentException(String.format("No converter for %s to %s", fromClass.getName(), toClass.getSimpleName()));
        }

        return c.apply(value);
    }
}
