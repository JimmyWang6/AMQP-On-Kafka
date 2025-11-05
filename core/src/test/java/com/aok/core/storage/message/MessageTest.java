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
package com.aok.core.storage.message;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    @Test
    void testMessageCreation() {
        Message message = new Message();
        assertNotNull(message);
    }

    @Test
    void testMessageSettersAndGetters() {
        Message message = new Message();
        
        message.setVhost("testVhost");
        message.setQueue("testQueue");
        message.setExchange("testExchange");
        message.setRoutingKey("test.routing.key");
        message.setBody("test body".getBytes());
        message.setContentType("text/plain");
        message.setContentEncoding("UTF-8");
        message.setDeliveryMode(2);
        message.setPriority(5);
        message.setCorrelationId("corr-123");
        message.setReplyTo("replyQueue");
        message.setExpiration("60000");
        message.setMessageId("msg-123");
        message.setTimestamp(System.currentTimeMillis());
        message.setType("testType");
        message.setUserId("testUser");
        message.setAppId("testApp");
        message.setMandatory(true);
        message.setImmediate(false);
        message.setBodySize(1024L);
        
        Map<String, Object> headers = new HashMap<>();
        headers.put("header1", "value1");
        message.setHeaders(headers);

        assertEquals("testVhost", message.getVhost());
        assertEquals("testQueue", message.getQueue());
        assertEquals("testExchange", message.getExchange());
        assertEquals("test.routing.key", message.getRoutingKey());
        assertArrayEquals("test body".getBytes(), message.getBody());
        assertEquals("text/plain", message.getContentType());
        assertEquals("UTF-8", message.getContentEncoding());
        assertEquals(2, message.getDeliveryMode());
        assertEquals(5, message.getPriority());
        assertEquals("corr-123", message.getCorrelationId());
        assertEquals("replyQueue", message.getReplyTo());
        assertEquals("60000", message.getExpiration());
        assertEquals("msg-123", message.getMessageId());
        assertNotNull(message.getTimestamp());
        assertEquals("testType", message.getType());
        assertEquals("testUser", message.getUserId());
        assertEquals("testApp", message.getAppId());
        assertTrue(message.isMandatory());
        assertFalse(message.isImmediate());
        assertEquals(1024L, message.getBodySize());
        assertEquals(headers, message.getHeaders());
    }

    @Test
    void testMessageWithHeaders() {
        Message message = new Message();
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "value");
        headers.put("x-priority", 10);
        
        message.setHeaders(headers);
        
        assertEquals(2, message.getHeaders().size());
        assertEquals("value", message.getHeaders().get("x-custom"));
        assertEquals(10, message.getHeaders().get("x-priority"));
    }

    @Test
    void testMessageWithByteBody() {
        Message message = new Message();
        byte[] body = {1, 2, 3, 4, 5};
        
        message.setBody(body);
        message.setBodySize(body.length);
        
        assertArrayEquals(body, message.getBody());
        assertEquals(5, message.getBodySize());
    }

    @Test
    void testMessageDeliveryModes() {
        Message transientMessage = new Message();
        transientMessage.setDeliveryMode(1); // Non-persistent
        assertEquals(1, transientMessage.getDeliveryMode());
        
        Message persistentMessage = new Message();
        persistentMessage.setDeliveryMode(2); // Persistent
        assertEquals(2, persistentMessage.getDeliveryMode());
    }

    @Test
    void testMessageMandatoryAndImmediateFlags() {
        Message message = new Message();
        
        message.setMandatory(true);
        message.setImmediate(true);
        
        assertTrue(message.isMandatory());
        assertTrue(message.isImmediate());
        
        message.setMandatory(false);
        message.setImmediate(false);
        
        assertFalse(message.isMandatory());
        assertFalse(message.isImmediate());
    }

    @Test
    void testMessageWithNullValues() {
        Message message = new Message();
        
        assertNull(message.getVhost());
        assertNull(message.getQueue());
        assertNull(message.getExchange());
        assertNull(message.getRoutingKey());
        assertNull(message.getBody());
        assertNull(message.getHeaders());
    }
}
