package com.ltsoft.jms.message;

import com.ltsoft.jms.type.IntegerType;
import com.ltsoft.jms.type.StringType;
import com.ltsoft.jms.util.MessageType;
import org.junit.jupiter.api.Test;

import javax.jms.ObjectMessage;

import static com.ltsoft.jms.message.JmsMessageHelper.getMessageId;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 对象消息测试
 */
public class JmsObjectMessageTest {

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setJMSMessageID(getMessageId());
        message.setObject(type);

        byte[] bytes = JmsMessageHelper.toBytes(message);

        ObjectMessage result = JmsMessageHelper.fromBytes(bytes);

        assertTrue(result.getObject() instanceof StringType);
    }

    @Test
    public void getBody() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setJMSMessageID(getMessageId());
        message.setObject(type);

        byte[] bytes = JmsMessageHelper.toBytes(message);

        ObjectMessage result = JmsMessageHelper.fromBytes(bytes);

        assertEquals("foo", result.getBody(StringType.class).getString());
    }

    @Test
    public void getJMSType() throws Exception {
        assertEquals(MessageType.Object.name(), new JmsObjectMessage().getJMSType());
    }

    @Test
    public void clearBody() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        assertNotNull(message.getBody(StringType.class));

        message.clearBody();

        assertNull(message.getBody(StringType.class));
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        assertTrue(message.isBodyAssignableTo(StringType.class));
        assertFalse(message.isBodyAssignableTo(IntegerType.class));
    }

}
