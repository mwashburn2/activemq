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

package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TopicFanoutMemoryUsageTest {

    private static final int TOPICS = 3;
    private static final int PRODUCERS_PER_TOPIC = 2;
    private static final int MESSAGES_PER_PRODUCER = 500;
    private static final int FAST_SUBSCRIBERS_PER_TOPIC = 2;
    private static final int SLOW_SUBSCRIBERS_PER_TOPIC = 1;
    private static final int MESSAGE_SIZE = 1024;

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private final List<Connection> connections = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("topicFanoutMemoryUsage");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.getSystemUsage().getMemoryUsage().setLimit(512 * 1024);
        PolicyEntry policy = new PolicyEntry();
        policy.setProducerFlowControl(true);
        policy.setMemoryLimit(128 * 1024);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        factory = new ActiveMQConnectionFactory("vm://topicFanoutMemoryUsage?create=false");
        factory.setUseAsyncSend(true);
        factory.setProducerWindowSize(32 * 1024);
        factory.setWatchTopicAdvisories(false);
    }

    @After
    public void tearDown() throws Exception {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private Connection newConnection() throws Exception {
        Connection connection = factory.createConnection();
        connections.add(connection);
        connection.start();
        return connection;
    }

    @Test(timeout = 90000)
    public void testProducersCompleteWithSlowSubscribers() throws Exception {
        final int expectedPerTopic = PRODUCERS_PER_TOPIC * MESSAGES_PER_PRODUCER;
        final int subscribersPerTopic = FAST_SUBSCRIBERS_PER_TOPIC + SLOW_SUBSCRIBERS_PER_TOPIC;
        final CountDownLatch allReceived = new CountDownLatch(TOPICS * subscribersPerTopic);
        final List<AtomicInteger> counts = new ArrayList<>();

        for (int t = 0; t < TOPICS; t++) {
            for (int s = 0; s < subscribersPerTopic; s++) {
                final boolean slow = s >= FAST_SUBSCRIBERS_PER_TOPIC;
                final AtomicInteger count = new AtomicInteger();
                counts.add(count);
                Session session = newConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createTopic("fanout." + t));
                consumer.setMessageListener(message -> {
                    if (slow) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(2);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (count.incrementAndGet() == expectedPerTopic) {
                        allReceived.countDown();
                    }
                });
            }
        }

        final byte[] payload = new byte[MESSAGE_SIZE];
        ExecutorService pool = Executors.newFixedThreadPool(TOPICS * PRODUCERS_PER_TOPIC);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < TOPICS; t++) {
                final String topicName = "fanout." + t;
                for (int p = 0; p < PRODUCERS_PER_TOPIC; p++) {
                    futures.add(pool.submit(() -> {
                        Session session = newConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Topic topic = session.createTopic(topicName);
                        MessageProducer producer = session.createProducer(topic);
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
                            BytesMessage message = session.createBytesMessage();
                            message.writeBytes(payload);
                            producer.send(message);
                        }
                        producer.close();
                        return null;
                    }));
                }
            }
            for (Future<?> future : futures) {
                future.get(45, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertTrue("all subscribers received all messages", allReceived.await(30, TimeUnit.SECONDS));
        for (AtomicInteger count : counts) {
            assertEquals(expectedPerTopic, count.get());
        }
    }
}