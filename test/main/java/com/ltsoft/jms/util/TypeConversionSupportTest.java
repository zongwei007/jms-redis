package com.ltsoft.jms.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TypeConversionSupportTest {

    @Test(expected = NullPointerException.class)
    public void testNullValue() throws Exception {
        TypeConversionSupport.convert(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testNullType() throws Exception {
        TypeConversionSupport.convert("foo", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoConverter() throws Exception {
        TypeConversionSupport.convert("foo", Object.class);
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

    @Test
    public void testToBytes() throws Exception {
        byte[] bBytes = TypeConversionSupport.convert(true, byte[].class);
        byte[] sBytes = TypeConversionSupport.convert((short) 10, byte[].class);
        byte[] iBytes = TypeConversionSupport.convert(20, byte[].class);
        byte[] lBytes = TypeConversionSupport.convert(30L, byte[].class);
        byte[] fBytes = TypeConversionSupport.convert(2.2F, byte[].class);
        byte[] dBytes = TypeConversionSupport.convert(3.3D, byte[].class);
        byte[] ssBytes = TypeConversionSupport.convert("中文内容", byte[].class);

        assertEquals(TypeConversionSupport.convert(bBytes, Boolean.class), true);
        assertEquals(TypeConversionSupport.convert(sBytes, Short.class).shortValue(), (short) 10);
        assertEquals(TypeConversionSupport.convert(iBytes, Integer.class).intValue(), 20);
        assertEquals(TypeConversionSupport.convert(lBytes, Long.class).longValue(), 30);
        assertEquals(TypeConversionSupport.convert(fBytes, Float.class), 2.2, 1);
        assertEquals(TypeConversionSupport.convert(dBytes, Double.class), 3.3, 1);
        assertEquals(TypeConversionSupport.convert(ssBytes, String.class), "中文内容");
    }

}
