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

import static org.junit.jupiter.api.Assertions.*;

class BindingTest {

    @Test
    void testBindingConstructor() {
        String source = "sourceExchange";
        String destination = "destinationQueue";
        String routingKey = "test.routing.key";

        Binding binding = new Binding(source, destination, routingKey);

        assertEquals(source, binding.getSource());
        assertEquals(destination, binding.getDestination());
        assertEquals(routingKey, binding.getRoutingKey());
        assertEquals("binding", binding.getMetaType());
    }

    @Test
    void testBindingNoArgsConstructor() {
        Binding binding = new Binding();
        
        assertNull(binding.getSource());
        assertNull(binding.getDestination());
        assertNull(binding.getRoutingKey());
    }

    @Test
    void testBindingEquality() {
        Binding binding1 = new Binding("exchange1", "queue1", "key1");
        Binding binding2 = new Binding("exchange1", "queue1", "key1");
        Binding binding3 = new Binding("exchange1", "queue2", "key1");

        assertEquals(binding1, binding2);
        assertNotEquals(binding1, binding3);
    }

    @Test
    void testBindingSetters() {
        Binding binding = new Binding();
        
        binding.setSource("exchange1");
        binding.setDestination("queue1");
        binding.setRoutingKey("routing.key");
        binding.setVhost("vhost1");
        binding.setName("binding1");

        assertEquals("exchange1", binding.getSource());
        assertEquals("queue1", binding.getDestination());
        assertEquals("routing.key", binding.getRoutingKey());
        assertEquals("vhost1", binding.getVhost());
        assertEquals("binding1", binding.getName());
    }

    @Test
    void testBindingWithEmptyRoutingKey() {
        Binding binding = new Binding("exchange1", "queue1", "");

        assertEquals("exchange1", binding.getSource());
        assertEquals("queue1", binding.getDestination());
        assertEquals("", binding.getRoutingKey());
    }
}
