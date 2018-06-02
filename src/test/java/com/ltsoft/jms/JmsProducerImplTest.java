package com.ltsoft.jms;

import com.ltsoft.jms.message.JmsMessage;
import com.ltsoft.jms.type.IntegerType;
import com.ltsoft.jms.util.ThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息提供者测试
 */
public class JmsProducerImplTest {


    private static RedissonClient client;
    private static JmsContextImpl context;

    private Queue queue = context.createQueue("queue");

    @BeforeAll
    public static void setupBeforeClass() {
        client = Redisson.create();
        JmsConfig jmsConfig = new JmsConfig();
        ThreadPool threadPool = new ThreadPool(jmsConfig);

        context = new JmsContextImpl("ClientID", client, jmsConfig, threadPool, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @AfterAll
    public static void tearDownAfterClass() {
        context.close();
        client.shutdown();
    }

    @BeforeEach
    public void setup() {
        client.getKeys().flushdb();
    }

    @Test
    public void send() throws Exception {
        TextMessage message = context.createTextMessage("text");

        context.createProducer().send(queue, message);

        assertAll(
                () -> assertTrue(client.getList(getDestinationKey(queue)).size() > 0),
                () -> assertTrue(client.getKeys().countExists(getDestinationPropsKey(queue, message.getJMSMessageID())) > 0)
        );
    }

    @Test
    public void sendText() throws Exception {
        context.createProducer().send(queue, "text");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertFalse(messageId.isEmpty());

        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertTrue(message instanceof TextMessage);
    }

    @Test
    public void sendMap() throws Exception {
        Map<String, Object> value = new HashMap<>();
        value.put("foo", 1);
        context.createProducer().send(queue, value);

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertFalse(messageId.isEmpty());

        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertTrue(message instanceof MapMessage);
    }

    @Test
    public void sendBytes() throws Exception {
        context.createProducer().send(queue, "foo".getBytes());

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertFalse(messageId.isEmpty());

        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertTrue(message instanceof StreamMessage);
    }

    @Test
    public void sendObject() throws Exception {
        IntegerType obj = new IntegerType();
        obj.setInteger(123);

        context.createProducer().send(queue, obj);

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        assertFalse(messageId.isEmpty());

        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertTrue(message instanceof ObjectMessage);
    }

    @Test
    public void sendToTopic() throws Exception {
        Topic topic = context.createTopic("topic");

        context.createProducer().send(topic, "to Topic");

        //TODO 校验写入，丰富场景
    }

    @Test
    public void setDisableMessageID() throws Exception {
        assertThrows(UnsupportedOperationException.class, () -> context.createProducer().setDisableMessageID(true));
    }

    @Test
    public void getDisableMessageID() throws Exception {
        assertFalse(context.createProducer().getDisableMessageID());
    }

    @Test
    public void disableMessageTimestamp() throws Exception {
        context.createProducer()
                .setDisableMessageTimestamp(true)
                .send(queue, "foo");

        String messageId = client.<String>getList(getDestinationKey(queue), StringCodec.INSTANCE).get(0);
        RMap<byte[], byte[]> map = client.getMap(getDestinationPropsKey(queue, messageId), ByteArrayCodec.INSTANCE);
        JmsMessage message = fromMap(toStringKey(map.readAllMap()));
        assertEquals(0, message.getJMSTimestamp());
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
        assertTrue(client.getKeys().remainTimeToLive(getDestinationPropsKey(queue, messageId)) > 0);
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
                        assertAll(
                                () -> assertFalse(message.getJMSMessageID().isEmpty()),
                                () -> assertTrue(message.getJMSTimestamp() > 0)
                        );

                        flag.set(true);
                    }

                    @Override
                    public void onException(Message message, Exception exception) {
                        fail(exception.getMessage());
                    }
                })
                .send(queue, "info");

        Thread.sleep(1000);

        assertTrue(flag.get());
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

        assertAll(
                () -> assertTrue(message.getBooleanProperty("bool")),
                () -> assertEquals((byte) Byte.valueOf("0"), message.getByteProperty("byte")),
                () -> assertEquals(10D, message.getDoubleProperty("double")),
                () -> assertEquals(20L, message.getLongProperty("long")),
                () -> assertEquals(30F, message.getFloatProperty("float")),
                () -> assertEquals((short) Short.valueOf("40"), message.getShortProperty("short")),
                () -> assertEquals(50, message.getIntProperty("int")),
                () -> assertTrue(message.getObjectProperty("obj") instanceof String),
                () -> assertEquals("foo", message.getStringProperty("text"))
        );
    }

    @Test
    public void clearProperties() throws Exception {
        assertTrue(context.createProducer()
                .setProperty("foo", "bar")
                .clearProperties().getPropertyNames().isEmpty());
    }

    @Test
    public void propertyExists() throws Exception {
        assertTrue(context.createProducer()
                .setProperty("foo", "bar").propertyExists("foo"));
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
        assertArrayEquals(idAsBytes, message.getJMSCorrelationIDAsBytes());
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
        assertEquals(id, message.getJMSCorrelationID());
    }

    @Test
    public void setJMSType() throws Exception {
        assertThrows(UnsupportedOperationException.class, () -> context.createProducer().setJMSType("type"));
    }

    @Test
    public void getJMSType() throws Exception {
        assertNull(context.createProducer().getJMSType());
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
        assertEquals(reply, message.getJMSReplyTo());
    }

}
