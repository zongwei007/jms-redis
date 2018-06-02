package com.ltsoft.jms;

import com.ltsoft.jms.util.ThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import javax.jms.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ltsoft.jms.util.KeyHelper.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 消费者测试
 */
public class JmsConsumerImplTest {

    private static RedissonClient client;
    private static JmsContextImpl context;

    private final long THREAD_WAIT = Duration.ofSeconds(5).toMillis();

    @BeforeAll
    public static void setupBeforeClass() throws Exception {
        client = Redisson.create();
        JmsConfig jmsConfig = new JmsConfig();
        ThreadPool threadPool = new ThreadPool(jmsConfig);

        context = new JmsContextImpl("ClientID", client, jmsConfig, threadPool, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @AfterAll
    public static void tearDownAfterClass() throws Exception {
        context.close();
        client.shutdown();
    }

    @BeforeEach
    public void setup() throws Exception {
        client.getKeys().flushdb();
    }

    @Test
    public void testRegisterAndUnRegister() throws Exception {
        Topic topic = context.createTopic("register");

        String consumerKey = getTopicConsumersKey(topic);
        RScoredSortedSet<String> scoredSortedSet = client.getScoredSortedSet(consumerKey, StringCodec.INSTANCE);

        try (JMSConsumer consumer = context.createConsumer(topic)) {
            Thread.sleep(1000);

            assertTrue(scoredSortedSet.size() > 0);
            assertTrue(scoredSortedSet.readAll().contains("ClientID"));
        }

        assertEquals(0, scoredSortedSet.size());
    }

    @Test
    public void testReceiveBody() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("receive-body");

        try (JMSConsumer consumer = context.createConsumer(queue)) {
            Future<String> future = context.cachedPool().submit(() -> consumer.receiveBody(String.class));

            context.createProducer().send(queue, text);

            assertEquals(text, future.get());
        }
    }

    @Test
    public void testReceiveNoWait() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("receive-no-wait");

        try (JMSConsumer consumer = context.createConsumer(queue)) {
            assertNull(consumer.receiveBodyNoWait(String.class));

            context.createProducer().send(queue, text);

            assertEquals(text, consumer.receiveBodyNoWait(String.class));
        }
    }

    @Test
    public void testReceiveToTimeout() {

        Queue queue = context.createQueue("receive-to-timeout");

        try (JMSConsumer consumer = context.createConsumer(queue)) {
            assertNull(consumer.receive(Duration.ofSeconds(1).toMillis()));
        }
    }

    @Test
    public void testMessageAck() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("msg-ack");

        context.createProducer().send(queue, text);

        try (JMSConsumer consumer = context.createConsumer(queue)) {

            Message message = consumer.receiveNoWait();
            assertNotNull(message);
            assertEquals(text, message.getBody(String.class));


            String propsKey = getDestinationPropsKey(queue, message.getJMSMessageID());
            String bodyKey = getDestinationBodyKey(queue, message.getJMSMessageID());


            assertTrue(client.getKeys().countExists(propsKey) > 0);
            assertTrue(client.getKeys().countExists(bodyKey) > 0);

            message.acknowledge();

            assertEquals(0, client.getKeys().countExists(propsKey));
            assertEquals(0, client.getKeys().countExists(bodyKey));
        }
    }

    @Test
    public void testQueueListener() throws Exception {

        CountDownLatch countDown = new CountDownLatch(5);

        Queue queue = context.createQueue("queue-listener");

        JMSConsumer consumer = context.createConsumer(queue);

        for (int i = 0; i < 5; i++) {
            int index = i;
            Thread thread = new Thread(() -> context.createProducer()
                    .setProperty("count", index)
                    .send(queue, "do count"));
            thread.start();
            thread.join(THREAD_WAIT);
        }

        List<Integer> props = Collections.synchronizedList(new ArrayList<>());
        consumer.setMessageListener(message -> {
            try {
                props.add(message.getIntProperty("count"));
                countDown.countDown();
                message.acknowledge();
            } catch (JMSException e) {
                fail(e.getMessage());
            }
        });

        countDown.await(20, TimeUnit.SECONDS);

        assertIterableEquals(props, Arrays.asList(0, 1, 2, 3, 4));

        consumer.close();
    }

    @Test
    public void testTopicListener() throws Exception {

        CountDownLatch countDown = new CountDownLatch(10);

        Topic topic = context.createTopic("topic-listener");

        JMSConsumer consumer = context.createConsumer(topic);

        int i = 0;
        while (i < 5) {
            int index = i;
            Thread thread = new Thread(() -> context.createProducer()
                    .setProperty("count", index)
                    .send(topic, "do count"));
            thread.start();
            thread.join(THREAD_WAIT);
            i++;
        }

        List<Integer> props = Collections.synchronizedList(new ArrayList<>());
        consumer.setMessageListener(message -> {
            try {
                props.add(message.getIntProperty("count"));
                countDown.countDown();
                message.acknowledge();
            } catch (JMSException e) {
                fail(e.getMessage());
            }
        });

        while (i < 10) {
            int index = i;
            Thread thread = new Thread(() -> context.createProducer()
                    .setProperty("count", index)
                    .send(topic, "do count"));
            thread.start();
            thread.join(THREAD_WAIT);
            i++;
        }

        countDown.await(20, TimeUnit.SECONDS);

        assertIterableEquals(props, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        consumer.close();
    }

    @Test
    public void testTopicNoPersistent() throws Exception {

        Topic topic = context.createTopic("topic-no-persistent");

        JMSConsumer consumer = context.createSharedConsumer(topic, "topic");

        int i = 0;
        while (i < 5) {
            int index = i;
            Thread thread = new Thread(() -> context.createProducer()
                    .setProperty("count", index)
                    .setDeliveryMode(DeliveryMode.NON_PERSISTENT)
                    .send(topic, "do count"));
            thread.start();
            thread.join(THREAD_WAIT);
            i++;
        }

        List<Integer> props = Collections.synchronizedList(new ArrayList<>());
        consumer.setMessageListener(message -> {
            try {
                props.add(message.getIntProperty("count"));
                message.acknowledge();
            } catch (JMSException e) {
                fail(e.getMessage());
            }
        });

        while (i < 10) {
            int index = i;
            Thread thread = new Thread(() -> context.createProducer()
                    .setProperty("count", index)
                    .setDeliveryMode(DeliveryMode.NON_PERSISTENT)
                    .send(topic, "do count"));
            thread.start();
            thread.join(THREAD_WAIT);
            i++;
        }

        Thread.sleep(1000);

        assertIterableEquals(props, Arrays.asList(5, 6, 7, 8, 9));

        consumer.close();
    }
}
