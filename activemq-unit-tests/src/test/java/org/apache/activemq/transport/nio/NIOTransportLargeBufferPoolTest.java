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
package org.apache.activemq.transport.nio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import java.util.Random;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class NIOTransportLargeBufferPoolTest {

    private static final int POOL_BUFFER_SIZE = 256 * 1024;

    private BrokerService broker;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("nio://localhost:0");
        broker.start();
        broker.waitUntilStarted();
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
    public void testFramesUsePoolOrHeap() throws Exception {
        String uri = "nio://localhost:" + broker.getTransportConnectors().get(0).getConnectUri().getPort();
        connection = new ActiveMQConnectionFactory(uri).createConnection();
        NIOTransport transport = ((ActiveMQConnection) connection).getTransport().narrow(NIOTransport.class);
        assertNotNull(transport);
        NIOLargeBufferPool pool = new NIOLargeBufferPool(2L * POOL_BUFFER_SIZE, 2);
        transport.largeBufferPool = pool;
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("pool.test");
        MessageConsumer consumer = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        int[] sizes = {1024, 100 * 1024, 2 * 1024 * 1024, 200 * 1024, 4096, 1024 * 1024, 150 * 1024};
        Random random = new Random(42);
        for (int size : sizes) {
            byte[] payload = new byte[size];
            random.nextBytes(payload);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message);

            BytesMessage received = (BytesMessage) consumer.receive(10000);
            assertNotNull("no message of size " + size, received);
            byte[] body = new byte[(int) received.getBodyLength()];
            received.readBytes(body);
            assertArrayEquals("payload of size " + size, payload, body);
        }

        assertTrue("pooled reads: " + pool.getHits(), pool.getHits() >= 3);
        assertTrue("oversize reads: " + pool.getOversize(), pool.getOversize() >= 2);
        assertEquals(0, pool.getMisses());
        assertEquals(pool.getAllocated(), pool.getAvailable());
    }
}
