package com.ltsoft.jms.util;

import org.junit.Test;

/**
 * 类型转换测试
 */
public class TypeConversionSupportTest {

    @Test
    public void testNullValue() throws Exception {
        TypeConversionSupport.convert(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testNullType() throws Exception {
        TypeConversionSupport.convert("foo", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoConverter() throws Exception {
        TypeConversionSupport.convert("foo", Integer.class);
    }

    @Test
    public void testSomeType() throws Exception {
        TypeConversionSupport.convert(true, String.class);
        TypeConversionSupport.convert((byte) 'a', String.class);
        TypeConversionSupport.convert((short) 10, String.class);
        TypeConversionSupport.convert(20, String.class);
        TypeConversionSupport.convert(30L, String.class);
        TypeConversionSupport.convert(2.2F, String.class);
        TypeConversionSupport.convert(3.3D, String.class);
        TypeConversionSupport.convert("foo", String.class);
    }
}
