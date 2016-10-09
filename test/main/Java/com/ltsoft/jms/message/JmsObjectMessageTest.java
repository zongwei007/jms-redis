package com.ltsoft.jms.message;

import com.ltsoft.jms.type.IntegerType;
import com.ltsoft.jms.type.StringType;
import com.ltsoft.jms.util.MessageType;
import org.junit.Test;

import javax.jms.ObjectMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 对象消息测试
 */
public class JmsObjectMessageTest {

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        byte[] bytes = JmsMessageHelper.toBytes(message);

        ObjectMessage result = JmsMessageHelper.fromBytes(bytes);

        assertThat(result.getObject()).isInstanceOf(StringType.class);
    }

    @Test
    public void getBody() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        byte[] bytes = JmsMessageHelper.toBytes(message);

        ObjectMessage result = JmsMessageHelper.fromBytes(bytes);

        assertThat(result.getBody(StringType.class).getString()).isEqualTo("foo");
    }

    @Test
    public void getJMSType() throws Exception {
        assertThat(new JmsObjectMessage().getJMSType()).isEqualTo(MessageType.Object.name());
    }

    @Test
    public void clearBody() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        assertThat(message.getBody(StringType.class)).isNotNull();

        message.clearBody();

        assertThat(message.getBody(StringType.class)).isNull();
    }

    @Test
    public void isBodyAssignableTo() throws Exception {
        StringType type = new StringType();
        type.setString("foo");

        JmsObjectMessage message = new JmsObjectMessage();
        message.setObject(type);

        assertThat(message.isBodyAssignableTo(StringType.class)).isTrue();
        assertThat(message.isBodyAssignableTo(IntegerType.class)).isFalse();
    }

}
