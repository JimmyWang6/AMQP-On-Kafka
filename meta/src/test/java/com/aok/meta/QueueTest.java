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
package com.aok.meta;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueueTest {

    @Test
    void testQueueConstructor() {
        String vhost = "testVhost";
        String name = "testQueue";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-max-length", 1000);

        Queue queue = new Queue(vhost, name, false, true, true, arguments);

        assertEquals(vhost, queue.getVhost());
        assertEquals(name, queue.getName());
        assertFalse(queue.getExclusive());
        assertTrue(queue.getAutoDelete());
        assertTrue(queue.getDurable());
        assertEquals(arguments, queue.getArguments());
        assertEquals("queue", queue.getMetaType());
    }

    @Test
    void testQueueWithNullArguments() {
        String vhost = "testVhost";
        String name = "testQueue";

        Queue queue = new Queue(vhost, name, true, false, false, null);

        assertEquals(vhost, queue.getVhost());
        assertEquals(name, queue.getName());
        assertTrue(queue.getExclusive());
        assertFalse(queue.getAutoDelete());
        assertFalse(queue.getDurable());
        assertNull(queue.getArguments());
    }

    @Test
    void testQueueEquality() {
        Queue queue1 = new Queue("vhost1", "queue1", false, false, true, null);
        Queue queue2 = new Queue("vhost1", "queue1", false, false, true, null);
        Queue queue3 = new Queue("vhost1", "queue2", false, false, true, null);

        assertEquals(queue1, queue2);
        // queue1 and queue3 have different names, so they should not be equal
        assertFalse(queue1.getName().equals(queue3.getName()));
    }

    @Test
    void testQueueSetters() {
        Queue queue = new Queue();
        
        queue.setVhost("vhost1");
        queue.setName("queue1");
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(true);
        
        Map<String, Object> args = new HashMap<>();
        args.put("test", "value");
        queue.setArguments(args);

        assertEquals("vhost1", queue.getVhost());
        assertEquals("queue1", queue.getName());
        assertTrue(queue.getDurable());
        assertFalse(queue.getExclusive());
        assertTrue(queue.getAutoDelete());
        assertEquals(args, queue.getArguments());
    }
}
