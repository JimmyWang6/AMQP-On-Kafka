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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnacknowledgedMessageMapTest {

    private UnacknowledgedMessageMap messageMap;

    @BeforeEach
    void setUp() {
        messageMap = new UnacknowledgedMessageMap(null);
    }

    @Test
    void testGenerateDeliveryTag() {
        long tag1 = messageMap.generateDeliveryTag();
        long tag2 = messageMap.generateDeliveryTag();
        
        assertEquals(1, tag1);
        assertEquals(2, tag2);
        assertTrue(tag2 > tag1);
    }

    @Test
    void testAddMessage() {
        long deliveryTag = messageMap.generateDeliveryTag();
        UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("testQueue");
        
        messageMap.addMessage(deliveryTag, metadata);
        
        assertEquals(1, messageMap.size());
    }

    @Test
    void testAddMultipleMessages() {
        for (int i = 0; i < 5; i++) {
            long deliveryTag = messageMap.generateDeliveryTag();
            UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("queue" + i);
            messageMap.addMessage(deliveryTag, metadata);
        }
        
        assertEquals(5, messageMap.size());
    }

    @Test
    void testAcknowledgeSingleMessage() {
        long deliveryTag = messageMap.generateDeliveryTag();
        UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("testQueue");
        messageMap.addMessage(deliveryTag, metadata);
        
        assertEquals(1, messageMap.size());
        
        int acked = messageMap.acknowledge(deliveryTag, false);
        
        assertEquals(1, acked);
        assertEquals(0, messageMap.size());
    }

    @Test
    void testAcknowledgeMultipleMessages() {
        // Add 3 messages
        long tag1 = messageMap.generateDeliveryTag();
        long tag2 = messageMap.generateDeliveryTag();
        long tag3 = messageMap.generateDeliveryTag();
        
        messageMap.addMessage(tag1, new UnacknowledgedMessageMap.MessageMetadata("queue1"));
        messageMap.addMessage(tag2, new UnacknowledgedMessageMap.MessageMetadata("queue2"));
        messageMap.addMessage(tag3, new UnacknowledgedMessageMap.MessageMetadata("queue3"));
        
        assertEquals(3, messageMap.size());
        
        // Acknowledge all messages up to and including tag2
        int acked = messageMap.acknowledge(tag2, true);
        
        assertEquals(2, acked);
        assertEquals(1, messageMap.size());
    }

    @Test
    void testAcknowledgeUnknownDeliveryTag() {
        int acked = messageMap.acknowledge(999, false);
        
        assertEquals(0, acked);
    }

    @Test
    void testNackSingleMessage() {
        long deliveryTag = messageMap.generateDeliveryTag();
        UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("testQueue");
        messageMap.addMessage(deliveryTag, metadata);
        
        assertEquals(1, messageMap.size());
        
        int nacked = messageMap.nack(deliveryTag, false, true);
        
        assertEquals(1, nacked);
        assertEquals(0, messageMap.size());
    }

    @Test
    void testNackMultipleMessages() {
        // Add 3 messages
        long tag1 = messageMap.generateDeliveryTag();
        long tag2 = messageMap.generateDeliveryTag();
        long tag3 = messageMap.generateDeliveryTag();
        
        messageMap.addMessage(tag1, new UnacknowledgedMessageMap.MessageMetadata("queue1"));
        messageMap.addMessage(tag2, new UnacknowledgedMessageMap.MessageMetadata("queue2"));
        messageMap.addMessage(tag3, new UnacknowledgedMessageMap.MessageMetadata("queue3"));
        
        assertEquals(3, messageMap.size());
        
        // Nack all messages up to and including tag2
        int nacked = messageMap.nack(tag2, true, false);
        
        assertEquals(2, nacked);
        assertEquals(1, messageMap.size());
    }

    @Test
    void testRejectMessage() {
        long deliveryTag = messageMap.generateDeliveryTag();
        UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("testQueue");
        messageMap.addMessage(deliveryTag, metadata);
        
        assertEquals(1, messageMap.size());
        
        boolean rejected = messageMap.reject(deliveryTag, false);
        
        assertTrue(rejected);
        assertEquals(0, messageMap.size());
    }

    @Test
    void testRejectUnknownMessage() {
        boolean rejected = messageMap.reject(999, false);
        
        assertFalse(rejected);
    }

    @Test
    void testClearMessages() {
        // Add multiple messages
        for (int i = 0; i < 5; i++) {
            long deliveryTag = messageMap.generateDeliveryTag();
            messageMap.addMessage(deliveryTag, new UnacknowledgedMessageMap.MessageMetadata("queue" + i));
        }
        
        assertEquals(5, messageMap.size());
        
        messageMap.clear();
        
        assertEquals(0, messageMap.size());
    }

    @Test
    void testMessageMetadata() {
        UnacknowledgedMessageMap.MessageMetadata metadata = new UnacknowledgedMessageMap.MessageMetadata("testQueue");
        
        assertEquals("testQueue", metadata.getQueue());
        assertTrue(metadata.getTimestamp() > 0);
        assertTrue(metadata.getTimestamp() <= System.currentTimeMillis());
    }

    @Test
    void testSize() {
        assertEquals(0, messageMap.size());
        
        messageMap.addMessage(messageMap.generateDeliveryTag(), 
            new UnacknowledgedMessageMap.MessageMetadata("queue1"));
        assertEquals(1, messageMap.size());
        
        messageMap.addMessage(messageMap.generateDeliveryTag(), 
            new UnacknowledgedMessageMap.MessageMetadata("queue2"));
        assertEquals(2, messageMap.size());
        
        messageMap.clear();
        assertEquals(0, messageMap.size());
    }

    @Test
    void testDeliveryTagSequence() {
        // Test that delivery tags are generated in ascending order
        long previousTag = 0;
        for (int i = 0; i < 10; i++) {
            long tag = messageMap.generateDeliveryTag();
            assertTrue(tag > previousTag);
            previousTag = tag;
        }
    }
}
