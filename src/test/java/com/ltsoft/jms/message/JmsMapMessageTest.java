package com.ltsoft.jms.message;

import com.ltsoft.jms.type.StringType;
import com.ltsoft.jms.util.MessageType;
import org.junit.Test;

import javax.jms.MapMessage;
import javax.jms.MessageFormatRuntimeException;
import java.util.Map;

import static com.ltsoft.jms.message.JmsMessageHelper.getMessageId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        assertThat(message.getObject("int")).isInstanceOf(Integer.class);
    }

    @Test
    public void testBoolean() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setBoolean("bool", true);
        message.setString("str", "true");
        message.setInt("int", 0);

        assertThat(message.getBoolean("bool")).isTrue();
        assertThat(message.getBoolean("str")).isTrue();

        assertThatThrownBy(() -> message.getBoolean("int")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testByte() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setByte("byte", Byte.valueOf("10"));
        message.setString("str", "20");
        message.setDouble("double", 4);

        assertThat(message.getByte("byte")).isEqualTo((byte) 10);

        assertThatThrownBy(() -> message.getByte("double")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testShort() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setShort("short", Short.valueOf("10"));
        message.setString("str", "20");

        assertThat(message.getShort("short")).isEqualTo((short) 10);
        assertThat(message.getShort("str")).isEqualTo((short) 20);
    }

    @Test
    public void testChar() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setChar("char", 'c');
        message.setString("str", "foo");

        assertThat(message.getChar("char")).isEqualTo('c');
        assertThatThrownBy(() -> message.getChar("str")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testInt() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setInt("integer", 10);
        message.setString("str", "20");
        message.setDouble("double", 30.5);

        assertThat(message.getInt("integer")).isEqualTo(10);
        assertThat(message.getInt("str")).isEqualTo(20);

        assertThatThrownBy(() -> message.getInt("double")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testLong() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setLong("long", 300);
        message.setInt("integer", 400);
        message.setString("str", "500");
        message.setDouble("double", 600.65);

        assertThat(message.getLong("long")).isEqualTo(300L);
        assertThat(message.getLong("integer")).isEqualTo(400L);
        assertThat(message.getLong("str")).isEqualTo(500L);

        assertThatThrownBy(() -> message.getLong("double")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testFloat() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setFloat("float", 20.3f);
        message.setString("str", "30.5");
        message.setInt("integer", 40);
        message.setDouble("double", 50.6);

        assertThat(message.getFloat("float")).isEqualTo(20.3f);
        assertThat(message.getFloat("str")).isEqualTo(30.5f);

        assertThatThrownBy(() -> message.getFloat("integer")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
        assertThatThrownBy(() -> message.getFloat("double")).isInstanceOf(MessageFormatRuntimeException.class).hasMessageContaining("No converter");
    }

    @Test
    public void testDouble() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setDouble("double", 23.5);
        message.setFloat("float", 33.5f);
        message.setString("str", "44.5");

        assertThat(message.getDouble("double")).isEqualTo(23.5);
        assertThat(message.getDouble("float")).isEqualTo(33.5);
        assertThat(message.getDouble("str")).isEqualTo(44.5);
    }

    @Test
    public void testString() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setString("str", "foo");
        message.setInt("integer", 10);
        message.setBoolean("bool", true);

        assertThat(message.getString("str")).isEqualTo("foo");
        assertThat(message.getString("integer")).isEqualTo("10");
        assertThat(message.getString("bool")).isEqualTo("true");
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

        assertThat(result.getBytes("bytes")).isEqualTo(strBytes);
        assertThat(result.getBytes("bytes-len")).isEqualTo("It".getBytes());
    }

    @Test
    public void testObject() throws Exception {
        JmsMapMessage message = new JmsMapMessage();
        message.setObject("obj1", "type1");
        message.setObject("obj2", 20);

        byte[] bytes = message.getBody();

        JmsMapMessage result = new JmsMapMessage();
        result.setBody(bytes);

        assertThat(result.getObject("obj1")).isEqualTo("type1");
        assertThat(result.getObject("obj2")).isEqualTo(20);
    }

    @Test
    public void getBody() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setObject("integer", 20);

        assertThat(message.getBody(Map.class).get("integer")).isEqualTo(20);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getMapNames() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setDouble("double", 2);
        message.setString("str", "foo");

        assertThat(((Map<String, Object>) message.getBody(Map.class)).keySet()).containsOnly("double", "str");
    }

    @Test
    public void itemExists() throws Exception {
        JmsMapMessage message = new JmsMapMessage();
        message.setJMSMessageID(getMessageId());
        message.setDouble("double", 5);
        message.setString("foo", null);

        byte[] bytes = JmsMessageHelper.toBytes(message);
        MapMessage result = JmsMessageHelper.fromBytes(bytes);

        assertThat(result.itemExists("double")).isTrue();
        assertThat(result.itemExists("foo")).isTrue();
        assertThat(result.itemExists("bar")).isFalse();
    }

    @Test
    public void getJMSType() throws Exception {
        assertThat(new JmsMapMessage().getJMSType()).isEqualTo(MessageType.Map.name());
    }

    @Test
    public void clearBody() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setString("foo", "bar");

        assertThat(message.itemExists("foo")).isTrue();

        message.clearBody();

        assertThat(message.itemExists("foo")).isFalse();
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        MapMessage message = new JmsMapMessage();
        message.setInt("integer", 20);

        assertThat(message.isBodyAssignableTo(Map.class)).isTrue();
        assertThat(message.isBodyAssignableTo(StringType.class)).isFalse();
    }

}
