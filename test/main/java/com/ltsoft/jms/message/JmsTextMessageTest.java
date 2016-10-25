package com.ltsoft.jms.message;

import com.ltsoft.jms.util.MessageType;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.List;

import static com.ltsoft.jms.message.JmsMessageHelper.getMessageId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        assertThat(result.getText()).isEqualTo("text 和 中文内容");
        assertThat(result).isInstanceOf(JmsTextMessage.class);
    }

    @Test
    public void testGetAndSetText() throws Exception {
        TextMessage message = new JmsTextMessage();
        message.setText("info");

        assertThat(message.getText()).isEqualTo("info");
    }

    @Test
    public void getBody() throws Exception {
        JmsTextMessage string = new JmsTextMessage();
        string.setText("foo");

        assertThat(string.getBody(String.class)).isEqualTo("foo");

        JmsTextMessage obj = new JmsTextMessage();
        obj.setText("10");

        assertThat(obj.getBody(Integer.class)).isEqualTo(10);

        assertThatThrownBy(() -> obj.getBody(List.class)).isInstanceOf(JMSException.class);
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        JmsTextMessage obj = new JmsTextMessage();
        obj.setText("10.5");

        assertThat(obj.isBodyAssignableTo(Integer.class)).isFalse();
        assertThat(obj.isBodyAssignableTo(Float.class)).isTrue();
    }

    @Test
    public void getJMSType() throws Exception {
        assertThat(new JmsTextMessage().getJMSType()).isEqualTo(MessageType.Text.name());
    }

    @Test
    public void clearBody() throws Exception {
        JmsTextMessage message = new JmsTextMessage();
        message.setText("info");
        message.clearBody();
        assertThat(message.getText()).isNullOrEmpty();
    }
}
