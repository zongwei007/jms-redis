package com.ltsoft.jms.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 类型序列化测试
 */
public class TypeSerializeSupportTest {

    @Test
    public void testToBytes() throws Exception {
        byte[] bBytes = TypeSerializeSupport.serialize(true);
        byte[] sBytes = TypeSerializeSupport.serialize((short) 10);
        byte[] iBytes = TypeSerializeSupport.serialize(20);
        byte[] lBytes = TypeSerializeSupport.serialize(30L);
        byte[] fBytes = TypeSerializeSupport.serialize(2.2F);
        byte[] dBytes = TypeSerializeSupport.serialize(3.3D);
        byte[] ssBytes = TypeSerializeSupport.serialize("中文内容");

        assertEquals(TypeSerializeSupport.deserialize(bBytes, Boolean.class), true);
        assertEquals(TypeSerializeSupport.deserialize(sBytes, Short.class).shortValue(), (short) 10);
        assertEquals(TypeSerializeSupport.deserialize(iBytes, Integer.class).intValue(), 20);
        assertEquals(TypeSerializeSupport.deserialize(lBytes, Long.class).longValue(), 30);
        assertEquals(TypeSerializeSupport.deserialize(fBytes, Float.class), 2.2, 1);
        assertEquals(TypeSerializeSupport.deserialize(dBytes, Double.class), 3.3, 1);
        assertEquals(TypeSerializeSupport.deserialize(ssBytes, String.class), "中文内容");
    }

}
