package com.ltsoft.jms.message;

import com.ltsoft.jms.destination.JmsQueue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JmsMessageTest {

    @Test
    public void test() throws Exception {
        JmsMessage message = new JmsMessage();
        message.setJMSReplyTo(new JmsQueue("test1"));
        message.setJMSDestination(new JmsQueue("test2"));

        message.setBooleanProperty("bool", true);
        message.setByteProperty("byte", Byte.valueOf("1"));
        message.setDoubleProperty("double", 10.10);
        message.setFloatProperty("float", 20.20f);
        message.setIntProperty("int", 30);
        message.setLongProperty("long", 40);
        message.setShortProperty("short", (short) 50);
        message.setStringProperty("string", "abc");
        message.setObjectProperty("obj", "some value");

        Map<String, byte[]> props = JmsMessageHelper.toMap(message);

        JmsMessage result = JmsMessageHelper.fromMap(props);

        assertAll(
                () -> assertTrue(result.getJMSReplyTo() instanceof JmsQueue),
                () -> assertTrue(result.getJMSDestination() instanceof JmsQueue),
                () -> assertTrue(result.getBooleanProperty("bool")),
                () -> assertEquals((byte) Byte.valueOf("1"), result.getByteProperty("byte")),
                () -> assertEquals(10.10, result.getDoubleProperty("double")),
                () -> assertEquals(20.20f, result.getFloatProperty("float")),
                () -> assertEquals(30, result.getIntProperty("int")),
                () -> assertEquals(40, result.getLongProperty("long")),
                () -> assertEquals((short) 50, result.getShortProperty("short")),
                () -> assertEquals("abc", result.getStringProperty("string")),
                () -> assertNotNull(result.getObjectProperty("obj")),
                () -> assertTrue(result.getObjectProperty("obj") instanceof String)
        );
    }
}
