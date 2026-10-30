/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class TopicSubscriptionBatchAckTest {

    private static final int MESSAGE_COUNT = 5000;

    private BrokerService broker;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName("batch-ack");
        broker.start();
        broker.waitUntilStarted();
        connection = new ActiveMQConnectionFactory("vm://batch-ack?create=false").createConnection();
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testSingleAckOfWholePrefetchWindowClearsInflight() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("BATCH.ACK.WHOLE");
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(
                new ActiveMQTopic(topic.getPhysicalName() + "?consumer.prefetchSize=" + (MESSAGE_COUNT * 2)));
        send(topic, MESSAGE_COUNT);

        Message last = receive(consumer, MESSAGE_COUNT);
        TopicSubscription sub = subscription(topic);
        assertEquals(MESSAGE_COUNT, sub.getDispatchedQueueSize());
        assertTrue(sub.getSubscriptionStatistics().getInflightMessageSize().getTotalSize() > 0);

        last.acknowledge();

        assertInflightCleared(topic, sub, MESSAGE_COUNT);
    }

    @Test(timeout = 60000)
    public void testSuccessiveRangeAcksClearInflight() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("BATCH.ACK.RANGES");
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(
                new ActiveMQTopic(topic.getPhysicalName() + "?consumer.prefetchSize=" + (MESSAGE_COUNT * 2)));
        send(topic, MESSAGE_COUNT);

        TopicSubscription sub = subscription(topic);
        int batch = MESSAGE_COUNT / 5;
        for (int acked = batch; acked <= MESSAGE_COUNT; acked += batch) {
            receive(consumer, batch).acknowledge();
            final int remaining = MESSAGE_COUNT - acked;
            assertTrue("dispatched should drop to " + remaining,
                    Wait.waitFor(() -> sub.getDispatchedQueueSize() == remaining, 10000, 10));
        }

        assertInflightCleared(topic, sub, MESSAGE_COUNT);
    }

    private void send(ActiveMQTopic topic, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("m" + i));
        }
        session.close();
    }

    private Message receive(MessageConsumer consumer, int count) throws Exception {
        Message last = null;
        for (int i = 0; i < count; i++) {
            last = consumer.receive(5000);
            assertNotNull("missing message " + i, last);
        }
        return last;
    }

    private TopicSubscription subscription(ActiveMQTopic topic) throws Exception {
        for (Subscription s : broker.getDestination(topic).getConsumers()) {
            if (s instanceof TopicSubscription) {
                return (TopicSubscription) s;
            }
        }
        throw new AssertionError("no TopicSubscription on " + topic);
    }

    private void assertInflightCleared(ActiveMQTopic topic, TopicSubscription sub, int expectedDequeues) throws Exception {
        assertTrue("dispatched list not drained",
                Wait.waitFor(() -> sub.getDispatchedQueueSize() == 0, 10000, 10));
        assertEquals(0, sub.getSubscriptionStatistics().getInflightMessageSize().getTotalSize());
        assertEquals(expectedDequeues, sub.getSubscriptionStatistics().getDequeues().getCount());
        assertEquals(0, broker.getDestination(topic).getDestinationStatistics().getInflight().getCount());
    }
}
