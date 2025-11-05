/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aok.core;

import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {

    private Consumer consumer;
    private FieldTable arguments;

    @BeforeEach
    void setUp() {
        arguments = FieldTable.convertToFieldTable(new java.util.HashMap<>());
        consumer = new Consumer(
            "ctag-1",
            "test-queue",
            null,  // channel - not used in these tests
            false,  // noLocal
            false,  // noAck
            false,  // exclusive
            arguments
        );
    }

    @Test
    void testConsumerCreation() {
        assertEquals("ctag-1", consumer.getConsumerTag());
        assertEquals("test-queue", consumer.getQueueName());
        assertNull(consumer.getChannel());
        assertFalse(consumer.isNoLocal());
        assertFalse(consumer.isNoAck());
        assertFalse(consumer.isExclusive());
        assertEquals(arguments, consumer.getArguments());
    }

    @Test
    void testConsumerIsActiveByDefault() {
        assertTrue(consumer.isActive());
    }

    @Test
    void testCancelConsumer() {
        assertTrue(consumer.isActive());
        
        consumer.cancel();
        
        assertFalse(consumer.isActive());
    }

    @Test
    void testMessageCountIncrement() {
        assertEquals(0, consumer.getMessageCount());
        
        consumer.incrementMessageCount();
        assertEquals(1, consumer.getMessageCount());
        
        consumer.incrementMessageCount();
        consumer.incrementMessageCount();
        assertEquals(3, consumer.getMessageCount());
    }

    @Test
    void testCreatedAtTimestamp() {
        long beforeCreation = System.currentTimeMillis();
        Consumer newConsumer = new Consumer(
            "ctag-2",
            "test-queue-2",
            null,  // channel
            true, true, true,
            arguments
        );
        long afterCreation = System.currentTimeMillis();
        
        assertTrue(newConsumer.getCreatedAt() >= beforeCreation);
        assertTrue(newConsumer.getCreatedAt() <= afterCreation);
    }

    @Test
    void testConsumerWithNoAck() {
        Consumer noAckConsumer = new Consumer(
            "ctag-noack",
            "test-queue",
            null,  // channel
            false,
            true,  // noAck = true
            false,
            arguments
        );
        
        assertTrue(noAckConsumer.isNoAck());
    }

    @Test
    void testConsumerWithExclusive() {
        Consumer exclusiveConsumer = new Consumer(
            "ctag-exclusive",
            "test-queue",
            null,  // channel
            false,
            false,
            true,  // exclusive = true
            arguments
        );
        
        assertTrue(exclusiveConsumer.isExclusive());
    }

    @Test
    void testConsumerWithNoLocal() {
        Consumer noLocalConsumer = new Consumer(
            "ctag-nolocal",
            "test-queue",
            null,  // channel
            true,  // noLocal = true
            false,
            false,
            arguments
        );
        
        assertTrue(noLocalConsumer.isNoLocal());
    }

    @Test
    void testMultipleConsumersIndependence() {
        Consumer consumer1 = new Consumer("ctag-1", "queue-1", null, false, false, false, arguments);
        Consumer consumer2 = new Consumer("ctag-2", "queue-2", null, false, false, false, arguments);
        
        consumer1.incrementMessageCount();
        consumer1.incrementMessageCount();
        consumer2.incrementMessageCount();
        
        assertEquals(2, consumer1.getMessageCount());
        assertEquals(1, consumer2.getMessageCount());
        
        consumer1.cancel();
        
        assertFalse(consumer1.isActive());
        assertTrue(consumer2.isActive());
    }
}
