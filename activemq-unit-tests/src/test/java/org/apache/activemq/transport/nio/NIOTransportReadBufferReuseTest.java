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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Frames larger than the initial read buffer must be read into a grown, reused direct buffer
 * (up to MAX_READ_BUFFER_SIZE) instead of a fresh direct buffer per frame, without corrupting
 * any payload when the buffer is reused for frames of different sizes.
 */
@Category(ParallelTest.class)
public class NIOTransportReadBufferReuseTest {

    private BrokerService broker;
    private TransportConnector connector;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        connector = broker.addConnector("nio://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        connection = new ActiveMQConnectionFactory(connector.getPublishableConnectString()).createConnection();
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testReadBufferCapacity() {
        int max = NIOTransport.MAX_READ_BUFFER_SIZE;
        assertEquals(16 * 1024, NIOTransport.readBufferCapacityFor(8 * 1024 + 1));
        assertEquals(16 * 1024, NIOTransport.readBufferCapacityFor(16 * 1024));
        assertEquals(max, NIOTransport.readBufferCapacityFor(max));
        assertEquals(max, NIOTransport.readBufferCapacityFor(max - 1));
    }

    @Test(timeout = 60000)
    public void testLargeFramesReuseReadBuffer() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("NIOTransportReadBufferReuseTest");
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);
        Random random = new Random(42);

        int max = NIOTransport.MAX_READ_BUFFER_SIZE;
        sendAndVerify(session, producer, consumer, random, 700 * 1024);
        ByteBuffer grown = serverTransport().inputBuffer;
        assertTrue("read buffer should be direct", grown.isDirect());
        assertEquals(max, grown.capacity());

        // Mixed sizes, including frames above the cap and below the initial buffer size,
        // must all round-trip intact and leave the grown buffer in place.
        int[] sizes = {1024, 20 * 1024, max + 64 * 1024, 300 * 1024, 100, 3 * max, 900 * 1024, 9 * 1024};
        for (int size : sizes) {
            sendAndVerify(session, producer, consumer, random, size);
            assertSame("read buffer should be reused, not reallocated", grown, serverTransport().inputBuffer);
        }
    }

    private void sendAndVerify(Session session, MessageProducer producer, MessageConsumer consumer,
                               Random random, int size) throws Exception {
        byte[] payload = new byte[size];
        random.nextBytes(payload);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(payload);
        producer.send(message);

        BytesMessage received = (BytesMessage) consumer.receive(10000);
        assertNotNull("message of " + size + " bytes not received", received);
        byte[] actual = new byte[(int) received.getBodyLength()];
        received.readBytes(actual);
        assertArrayEquals("payload of " + size + " bytes corrupted", payload, actual);
    }

    private NIOTransport serverTransport() {
        assertEquals(1, connector.getConnections().size());
        TransportConnection serverConnection = connector.getConnections().get(0);
        NIOTransport transport = serverConnection.getTransport().narrow(NIOTransport.class);
        assertNotNull(transport);
        return transport;
    }
}