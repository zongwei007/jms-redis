package com.ltsoft.jms.util;

public interface Serializer {

    byte[] serialize(Object source);

    <T> T deserialize(byte[] source);

}
