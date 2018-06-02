package com.ltsoft.jms.message;

import com.ltsoft.jms.util.MessageType;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.List;

import static com.ltsoft.jms.message.JmsMessageHelper.getMessageId;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 文本消息测试
 */
public class JmsTextMessageTest {

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        JmsTextMessage message = new JmsTextMessage();
        message.setJMSMessageID(getMessageId());
        message.setText("text 和 中文内容");

        byte[] bytes = JmsMessageHelper.toBytes(message);

        JmsTextMessage result = JmsMessageHelper.fromBytes(bytes);

        assertEquals("text 和 中文内容", result.getText());
    }

    @Test
    public void testGetAndSetText() throws Exception {
        TextMessage message = new JmsTextMessage();
        message.setText("info");

        assertEquals("info", message.getText());
    }

    @Test
    public void getBody() throws Exception {
        JmsTextMessage string = new JmsTextMessage();
        string.setText("foo");

        assertEquals("foo", string.getBody(String.class));

        JmsTextMessage obj = new JmsTextMessage();
        obj.setText("10");

        assertAll(
                () -> assertEquals(10, (int) obj.getBody(Integer.class)),
                () -> assertThrows(JMSException.class, () -> obj.getBody(List.class))
        );
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        JmsTextMessage obj = new JmsTextMessage();
        obj.setText("10.5");

        assertAll(
                () -> assertFalse(obj.isBodyAssignableTo(Integer.class)),
                () -> assertTrue(obj.isBodyAssignableTo(Float.class))
        );
    }

    @Test
    public void getJMSType() throws Exception {
        assertEquals(MessageType.Text.name(), new JmsTextMessage().getJMSType());
    }

    @Test
    public void clearBody() throws Exception {
        JmsTextMessage message = new JmsTextMessage();
        message.setText("info");
        message.clearBody();

        assertNull(message.getText());
    }
}
