package com.ltsoft.jms.message;

import com.ltsoft.jms.destination.JmsQueue;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(result.getJMSReplyTo()).isInstanceOf(JmsQueue.class);
        assertThat(result.getJMSDestination()).isInstanceOf(JmsQueue.class);

        assertThat(result.getBooleanProperty("bool")).isTrue();
        assertThat(result.getByteProperty("byte")).isEqualTo(Byte.valueOf("1"));
        assertThat(result.getDoubleProperty("double")).isEqualTo(10.10);
        assertThat(result.getFloatProperty("float")).isEqualTo(20.20f);
        assertThat(result.getIntProperty("int")).isEqualTo(30);
        assertThat(result.getLongProperty("long")).isEqualTo(40);
        assertThat(result.getShortProperty("short")).isEqualTo((short) 50);
        assertThat(result.getStringProperty("string")).isEqualTo("abc");
        assertThat(result.getObjectProperty("obj")).isNotNull().isInstanceOf(String.class);
    }
}
