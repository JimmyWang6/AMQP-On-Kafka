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
package com.aok.meta.service;

import com.aok.meta.Queue;
import com.aok.meta.container.InMemoryMetaContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueueServiceTest {

    private QueueService queueService;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        InMemoryMetaContainer metaContainer = new InMemoryMetaContainer();
        queueService = new QueueService((com.aok.meta.container.MetaContainer) metaContainer);
    }

    @Test
    void testAddQueue() {
        String vhost = "testVhost";
        String name = "testQueue";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-max-length", 1000);

        Queue previousQueue = queueService.addQueue(vhost, name, false, true, true, arguments);

        // First add should return null (no previous value)
        assertNull(previousQueue);
        
        // Verify the queue was added by retrieving it
        Queue queue = queueService.getQueue(vhost, name);
        assertNotNull(queue);
        assertEquals(vhost, queue.getVhost());
        assertEquals(name, queue.getName());
        assertFalse(queue.getExclusive());
        assertTrue(queue.getAutoDelete());
        assertTrue(queue.getDurable());
        assertEquals(arguments, queue.getArguments());
    }

    @Test
    void testGetQueue() {
        String vhost = "testVhost";
        String name = "testQueue";
        
        queueService.addQueue(vhost, name, false, false, true, null);
        Queue retrieved = queueService.getQueue(vhost, name);

        assertNotNull(retrieved);
        assertEquals(vhost, retrieved.getVhost());
        assertEquals(name, retrieved.getName());
    }

    @Test
    void testGetNonExistentQueue() {
        Queue retrieved = queueService.getQueue("nonexistent", "queue");
        assertNull(retrieved);
    }

    @Test
    void testDeleteQueue() {
        String vhost = "testVhost";
        String name = "testQueue";
        
        queueService.addQueue(vhost, name, false, false, true, null);
        Queue addedQueue = queueService.getQueue(vhost, name);
        assertNotNull(addedQueue);

        Queue deleted = queueService.deleteQueue(addedQueue);
        assertNotNull(deleted);
        assertEquals(name, deleted.getName());
        assertNull(queueService.getQueue(vhost, name));
    }

    @Test
    void testListQueue() {
        queueService.addQueue("vhost1", "queue1", false, false, true, null);
        queueService.addQueue("vhost1", "queue2", false, false, true, null);
        queueService.addQueue("vhost2", "queue3", false, false, true, null);

        List<Queue> queues = queueService.listQueue();
        assertEquals(3, queues.size());
    }

    @Test
    void testListQueueEmpty() {
        List<Queue> queues = queueService.listQueue();
        assertNotNull(queues);
        assertEquals(0, queues.size());
    }

    @Test
    void testSize() {
        assertEquals(0, queueService.size());
        
        queueService.addQueue("vhost1", "queue1", false, false, true, null);
        assertEquals(1, queueService.size());
        
        queueService.addQueue("vhost1", "queue2", false, false, true, null);
        assertEquals(2, queueService.size());
    }

    @Test
    void testAddMultipleQueuesWithSameName() {
        String vhost = "testVhost";
        String name = "testQueue";
        
        queueService.addQueue(vhost, name, false, false, true, null);
        queueService.addQueue(vhost, name, true, true, false, null);

        // Adding with same name should replace
        Queue retrieved = queueService.getQueue(vhost, name);
        assertNotNull(retrieved);
        assertEquals(1, queueService.size());
    }
}
