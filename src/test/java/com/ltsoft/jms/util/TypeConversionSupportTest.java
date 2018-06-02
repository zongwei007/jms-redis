package com.ltsoft.jms.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 类型转换测试
 */
public class TypeConversionSupportTest {

    @Test
    public void testNullValue() throws Exception {
        TypeConversionSupport.convert(null, String.class);
    }

    @Test
    public void testNullType() throws Exception {
        assertThrows(NullPointerException.class, () -> TypeConversionSupport.convert("foo", null));
    }

    @Test
    public void testNoConverter() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> TypeConversionSupport.convert("foo", Integer.class));
    }

    @ParameterizedTest
    @MethodSource("valueProvider")
    public void testSomeType(Object param) throws Exception {
        assertDoesNotThrow(() -> TypeConversionSupport.convert(param, String.class));
    }

    static Stream valueProvider() {
        return Stream.of(true, (byte) 'a', (short) 10, 20, 30L, 2.2F, 3.3D, "foo");
    }
}
