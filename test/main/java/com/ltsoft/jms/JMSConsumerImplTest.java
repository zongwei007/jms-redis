package com.ltsoft.jms;

import org.junit.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.jms.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ltsoft.jms.util.KeyHelper.*;
import static com.ltsoft.jms.util.ThreadPool.cachedPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * 消费者测试
 */
public class JMSConsumerImplTest {

    private static JedisPool pool;
    private static JMSContextImpl context;

    private final long THREAD_WAIT = Duration.ofSeconds(5).toMillis();

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        pool = new JedisPool();

        context = new JMSContextImpl("ClientID", pool, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        context.close();
        pool.close();
    }

    @Before
    @After
    public void setup() throws Exception {
        try (Jedis client = pool.getResource()) {
            client.flushDB();
        }
    }

    @Test
    public void testRegisterAndUnRegister() throws Exception {
        Topic topic = context.createTopic("register");

        String consumerKey = getTopicConsumersKey(topic);

        try (Jedis client = pool.getResource()) {
            try (JMSConsumer consumer = context.createConsumer(topic)) {

                Thread.sleep(1000);

                assertThat(client.zcard(consumerKey)).isGreaterThan(0);
                assertThat(client.zrange(consumerKey, 0, -1).contains("ClientID"));
            }
            assertThat(client.zcard(consumerKey)).isEqualTo(0);
        }
    }

    @Test
    public void testReceiveBody() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("receive-body");

        try (JMSConsumer consumer = context.createConsumer(queue)) {
            Future<String> future = cachedPool().submit(() -> consumer.receiveBody(String.class));

            context.createProducer().send(queue, text);

            assertThat(future.get()).isEqualTo(text);
        }
    }

    @Test
    public void testReceiveNoWait() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("receive-no-wait");

        try (JMSConsumer consumer = context.createConsumer(queue)) {
            assertThat(consumer.receiveBodyNoWait(String.class)).isNull();

            context.createProducer().send(queue, text);

            assertThat(consumer.receiveBodyNoWait(String.class)).isEqualTo(text);
        }
    }

    @Test
    public void testMessageAck() throws Exception {

        String text = "a text info";

        Queue queue = context.createQueue("msg-ack");

        context.createProducer().send(queue, text);

        try (JMSConsumer consumer = context.createConsumer(queue)) {

            Message message = consumer.receiveNoWait();
            assertThat(message).isNotNull();
            assertThat(message.getBody(String.class)).isEqualTo(text);

            try (Jedis client = pool.getResource()) {
                assertThat(client.exists(getDestinationPropsKey(queue, message.getJMSMessageID()))).isTrue();
                assertThat(client.exists(getDestinationBodyKey(queue, message.getJMSMessageID()))).isTrue();
            }

            message.acknowledge();

            try (Jedis client = pool.getResource()) {
                assertThat(client.exists(getDestinationPropsKey(queue, message.getJMSMessageID()))).isFalse();
                assertThat(client.exists(getDestinationBodyKey(queue, message.getJMSMessageID()))).isFalse();
            }
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

        assertThat(props).containsOnly(0, 1, 2, 3, 4);

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

        assertThat(props).containsOnly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

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

        assertThat(props).containsOnly(5, 6, 7, 8, 9);

        consumer.close();
    }
}
