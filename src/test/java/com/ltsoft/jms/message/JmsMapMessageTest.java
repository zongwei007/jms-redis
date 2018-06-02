package com.ltsoft.jms.message;

import com.ltsoft.jms.type.StringType;
import com.ltsoft.jms.util.MessageType;
import org.junit.jupiter.api.Test;

import javax.jms.MapMessage;
import javax.jms.MessageFormatRuntimeException;
import java.util.Arrays;
import java.util.Map;

import static com.ltsoft.jms.message.JmsMessageHelper.getMessageId;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Map 消息类型测试
 */
public class JmsMapMessageTest {

    @Test
    public void testSerializeAndDeserialize() throws Exception {

        JmsMapMessage message = new JmsMapMessage();
        message.setJMSMessageID(getMessageId());
        message.setInt("int", 5);

        byte[] bytes = JmsMessageHelper.toBytes(message);

        message = JmsMessageHelper.fromBytes(bytes);

        assertTrue(message.getObject("int") instanceof Integer);
    }

    @Test
    public void testBoolean() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setBoolean("bool", true);
        message.setString("str", "true");
        message.setInt("int", 0);

        assertAll(
                () -> assertTrue(message.getBoolean("bool")),
                () -> assertTrue(message.getBoolean("str")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getBoolean("int"))
        );
    }

    @Test
    public void testByte() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setByte("byte", Byte.valueOf("10"));
        message.setString("str", "20");
        message.setDouble("double", 4);

        assertAll(
                () -> assertEquals((byte) 10, message.getByte("byte")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getByte("double"))
        );
    }

    @Test
    public void testShort() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setShort("short", Short.valueOf("10"));
        message.setString("str", "20");

        assertAll(
                () -> assertEquals((short) 10, message.getShort("short")),
                () -> assertEquals((short) 20, message.getShort("str"))
        );
    }

    @Test
    public void testChar() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setChar("char", 'c');
        message.setString("str", "foo");

        assertAll(
                () -> assertEquals('c', message.getChar("char")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getChar("str"))
        );
    }

    @Test
    public void testInt() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setInt("integer", 10);
        message.setString("str", "20");
        message.setDouble("double", 30.5);

        assertAll(
                () -> assertEquals(10, message.getInt("integer")),
                () -> assertEquals(20, message.getInt("str")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getInt("double"))
        );
    }

    @Test
    public void testLong() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setLong("long", 300);
        message.setInt("integer", 400);
        message.setString("str", "500");
        message.setDouble("double", 600.65);

        assertAll(
                () -> assertEquals(300L, message.getLong("long")),
                () -> assertEquals(400L, message.getLong("integer")),
                () -> assertEquals(500L, message.getLong("str")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getLong("double"))
        );
    }

    @Test
    public void testFloat() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setFloat("float", 20.3f);
        message.setString("str", "30.5");
        message.setInt("integer", 40);
        message.setDouble("double", 50.6);

        assertAll(
                () -> assertEquals(20.3f, message.getFloat("float")),
                () -> assertEquals(30.5f, message.getFloat("str")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getFloat("integer")),
                () -> assertThrows(MessageFormatRuntimeException.class, () -> message.getFloat("double"))
        );
    }

    @Test
    public void testDouble() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setDouble("double", 23.5);
        message.setFloat("float", 33.5f);
        message.setString("str", "44.5");

        assertAll(
                () -> assertEquals(23.5, message.getDouble("double")),
                () -> assertEquals(33.5, message.getDouble("float")),
                () -> assertEquals(44.5, message.getDouble("str"))
        );
    }

    @Test
    public void testString() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setString("str", "foo");
        message.setInt("integer", 10);
        message.setBoolean("bool", true);

        assertAll(
                () -> assertEquals("foo", message.getString("str")),
                () -> assertEquals("10", message.getString("integer")),
                () -> assertEquals("true", message.getString("bool"))
        );
    }

    @Test
    public void testBytes() throws Exception {
        byte[] strBytes = "It is a bytes".getBytes();
        JmsMapMessage message = new JmsMapMessage();
        message.setJMSMessageID(getMessageId());
        message.setBytes("bytes", strBytes);
        message.setBytes("bytes-len", strBytes, 0, 2);

        byte[] bytes = JmsMessageHelper.toBytes(message);
        JmsMapMessage result = JmsMessageHelper.fromBytes(bytes);

        assertAll(
                () -> assertArrayEquals(strBytes, result.getBytes("bytes")),
                () -> assertArrayEquals("It".getBytes(), result.getBytes("bytes-len"))
        );
    }

    @Test
    public void testObject() throws Exception {
        JmsMapMessage message = new JmsMapMessage();
        message.setObject("obj1", "type1");
        message.setObject("obj2", 20);

        byte[] bytes = message.getBody();

        JmsMapMessage result = new JmsMapMessage();
        result.setBody(bytes);

        assertAll(
                () -> assertEquals("type1", result.getObject("obj1")),
                () -> assertEquals(20, result.getObject("obj2"))
        );
    }

    @Test
    public void getBody() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setObject("integer", 20);

        assertEquals(20, message.getBody(Map.class).get("integer"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getMapNames() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setDouble("double", 2);
        message.setString("str", "foo");

        assertIterableEquals(((Map<String, Object>) message.getBody(Map.class)).keySet(), Arrays.asList("str", "double"));
    }

    @Test
    public void itemExists() throws Exception {
        JmsMapMessage message = new JmsMapMessage();
        message.setJMSMessageID(getMessageId());
        message.setDouble("double", 5);
        message.setString("foo", null);

        byte[] bytes = JmsMessageHelper.toBytes(message);
        MapMessage result = JmsMessageHelper.fromBytes(bytes);


        assertAll(
                () -> assertTrue(result.itemExists("double")),
                () -> assertTrue(result.itemExists("foo")),
                () -> assertFalse(result.itemExists("bar"))
        );
    }

    @Test
    public void getJMSType() throws Exception {
        assertEquals(MessageType.Map.name(), new JmsMapMessage().getJMSType());
    }

    @Test
    public void clearBody() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setString("foo", "bar");

        assertTrue(message.itemExists("foo"));

        message.clearBody();

        assertFalse(message.itemExists("foo"));
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setInt("integer", 20);

        assertAll(
                () -> assertTrue(message.isBodyAssignableTo(Map.class)),
                () -> assertFalse(message.isBodyAssignableTo(StringType.class))
        );
    }

}
