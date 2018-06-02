package com.ltsoft.jms;

import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.type.IntegerType;
import com.ltsoft.jms.util.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ltsoft.jms.message.JmsMessageHelper.fromMap;
import static com.ltsoft.jms.message.JmsMessageHelper.toStringKey;
import static com.ltsoft.jms.util.KeyHelper.getDestinationKey;
import static com.ltsoft.jms.util.KeyHelper.getDestinationPropsKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * 消息提供者测试
 */
public class JmsProducerImplTest {


    private static RedissonClient client;
    private static JmsContextImpl context;

    private Queue queue = context.createQueue("queue");

    @BeforeClass
    public static void setupBeforeClass() {
        client = Redisson.create();
        JmsConfig jmsConfig = new JmsConfig();
        ThreadPool threadPool = new ThreadPool(jmsConfig);

        context = new JmsContextImpl("ClientID", client, jmsConfig, threadPool, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        context.close();
        client.shutdown();
    }

    @Before
    public void setup() {
        client.getKeys().flushdb();
    }

    @Test
    public void send() throws Exception {
        TextMessage message = context.createTextMessage("text");

        context.createProducer().send(queue, message);

        assertThat(client.getList(getDestinationKey(queue)).size()).isGreaterThan(0);
        assertThat(client.getKeys().countExists(getDestinationPropsKey(queue, message.getJMSMessageID()))).isGreaterThan(0);
    }

    @Test
    public void sendText() throws Exception {
        context.createProducer().send(queue, "text");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertThat(messageId).isNotEmpty();
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message).isInstanceOf(TextMessage.class);
    }

    @Test
    public void sendMap() throws Exception {
        Map<String, Object> value = new HashMap<>();
        value.put("foo", 1);
        context.createProducer().send(queue, value);

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertThat(messageId).isNotEmpty();
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message).isInstanceOf(MapMessage.class);
    }

    @Test
    public void sendBytes() throws Exception {
        context.createProducer().send(queue, "foo".getBytes());

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertThat(messageId).isNotEmpty();
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message).isInstanceOf(StreamMessage.class);
    }

    @Test
    public void sendObject() throws Exception {
        IntegerType obj = new IntegerType();
        obj.setInteger(123);

        context.createProducer().send(queue, obj);

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertThat(messageId).isNotEmpty();
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message).isInstanceOf(ObjectMessage.class);
    }

    @Test
    public void sendToTopic() throws Exception {
        Topic topic = context.createTopic("topic");

        context.createProducer().send(topic, "to Topic");

        //TODO 校验写入，丰富场景
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setDisableMessageID() throws Exception {
        context.createProducer().setDisableMessageID(true);
        fail("unsupported");
    }

    @Test
    public void getDisableMessageID() throws Exception {
        assertThat(context.createProducer().getDisableMessageID()).isFalse();
    }

    @Test
    public void disableMessageTimestamp() throws Exception {
        context.createProducer()
                .setDisableMessageTimestamp(true)
                .send(queue, "foo");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message.getJMSTimestamp()).isEqualTo(0);
    }

    @Test
    public void deliveryMode() throws Exception {
        context.createProducer().setDeliveryMode(4);
    }

    @Test
    public void priority() throws Exception {

    }

    @Test
    public void timeToLive() throws Exception {
        context.createProducer()
                .setTimeToLive(1000)
                .send(queue, "text");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertThat(client.getKeys().remainTimeToLive(getDestinationPropsKey(queue, messageId))).isGreaterThan(0);
    }

    @Test
    public void deliveryDelay() throws Exception {
        context.createProducer().setDeliveryDelay(100);
    }

    @Test
    public void async() throws Exception {
        AtomicBoolean flag = new AtomicBoolean(false);
        context.createProducer()
                .setAsync(new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                        try {
                            assertThat(message.getJMSMessageID()).isNotEmpty();
                            assertThat(message.getJMSTimestamp()).isGreaterThan(0);
                            flag.set(true);
                        } catch (JMSException e) {
                            fail(e.getMessage());
                        }
                    }

                    @Override
                    public void onException(Message message, Exception exception) {
                        fail(exception.getMessage());
                    }
                })
                .send(queue, "info");

        Thread.sleep(1000);

        assertThat(flag.get()).isTrue();
    }

    @Test
    public void setProperty() throws Exception {

        context.createProducer()
                .setProperty("bool", true)
                .setProperty("byte", Byte.parseByte("0"))
                .setProperty("double", 10D)
                .setProperty("long", 20L)
                .setProperty("float", 30F)
                .setProperty("short", Short.parseShort("40"))
                .setProperty("int", 50)
                .setProperty("obj", "some_info")
                .setProperty("text", "foo")
                .send(queue, "info");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message.getBooleanProperty("bool")).isTrue();
        assertThat(message.getByteProperty("byte")).isEqualTo(Byte.valueOf("0"));
        assertThat(message.getDoubleProperty("double")).isEqualTo(10D);
        assertThat(message.getLongProperty("long")).isEqualTo(20L);
        assertThat(message.getFloatProperty("float")).isEqualTo(30F);
        assertThat(message.getShortProperty("short")).isEqualTo(Short.valueOf("40"));
        assertThat(message.getIntProperty("int")).isEqualTo(50);
        assertThat(message.getObjectProperty("obj")).isInstanceOf(String.class);
        assertThat(message.getStringProperty("text")).isEqualTo("foo");
    }

    @Test
    public void clearProperties() throws Exception {
        assertThat(context.createProducer()
                .setProperty("foo", "bar")
                .clearProperties().getPropertyNames()).isEmpty();
    }

    @Test
    public void propertyExists() throws Exception {
        assertThat(context.createProducer()
                .setProperty("foo", "bar").propertyExists("foo")).isTrue();
    }

    @Test
    public void JMSCorrelationIDAsBytes() throws Exception {
        byte[] idAsBytes = "INSTANCE".getBytes();
        context.createProducer()
                .setJMSCorrelationIDAsBytes(idAsBytes)
                .send(queue, "info");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message.getJMSCorrelationIDAsBytes()).isEqualTo(idAsBytes);
    }

    @Test
    public void JMSCorrelationID() throws Exception {
        String id = "INSTANCE";
        context.createProducer()
                .setJMSCorrelationID(id)
                .send(queue, "info");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message.getJMSCorrelationID()).isEqualTo(id);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setJMSType() throws Exception {
        context.createProducer().setJMSType("type");
        fail("unsupported");
    }

    @Test
    public void getJMSType() throws Exception {
        assertThat(context.createProducer().getJMSType()).isNull();
    }

    @Test
    public void setJMSReplyTo() throws Exception {
        Queue reply = context.createQueue("reply");

        context.createProducer()
                .setJMSReplyTo(reply)
                .send(queue, "info");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertThat(message.getJMSReplyTo()).isEqualTo(reply);
    }

}
