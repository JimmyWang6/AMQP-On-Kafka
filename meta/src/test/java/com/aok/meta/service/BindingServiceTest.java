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

import com.aok.meta.Binding;
import com.aok.meta.container.InMemoryMetaContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BindingServiceTest {

    @Test
    void testCreateBinding() {
        Binding binding = new Binding("exchange1", "queue1", "test.routing.key");
        
        assertEquals("exchange1", binding.getSource());
        assertEquals("queue1", binding.getDestination());
        assertEquals("test.routing.key", binding.getRoutingKey());
    }

    @Test
    void testBindingWithVhostAndName() {
        Binding binding = new Binding("exchange1", "queue1", "key1");
        binding.setVhost("vhost1");
        binding.setName("binding1");
        
        assertEquals("vhost1", binding.getVhost());
        assertEquals("binding1", binding.getName());
        assertEquals("exchange1", binding.getSource());
        assertEquals("queue1", binding.getDestination());
    }

    @Test
    void testBindingWithEmptyRoutingKey() {
        Binding binding = new Binding("exchange1", "queue1", "");
        
        assertEquals("exchange1", binding.getSource());
        assertEquals("queue1", binding.getDestination());
        assertEquals("", binding.getRoutingKey());
    }

    @Test
    void testBindingEquality() {
        Binding binding1 = new Binding("exchange1", "queue1", "key1");
        binding1.setVhost("vhost1");
        binding1.setName("binding1");
        
        Binding binding2 = new Binding("exchange1", "queue1", "key1");
        binding2.setVhost("vhost1");
        binding2.setName("binding1");
        
        assertEquals(binding1, binding2);
    }

    @Test
    void testBindingServiceListEmpty() {
        InMemoryMetaContainer metaContainer = new InMemoryMetaContainer();
        @SuppressWarnings("unchecked")
        BindingService bindingService = new BindingService((com.aok.meta.container.MetaContainer) metaContainer);
        
        List<Binding> bindings = bindingService.listBindings();
        assertNotNull(bindings);
        assertEquals(0, bindings.size());
    }

    @Test
    void testBindingServiceListFiltering() {
        InMemoryMetaContainer metaContainer = new InMemoryMetaContainer();
        @SuppressWarnings("unchecked")
        BindingService bindingService = new BindingService((com.aok.meta.container.MetaContainer) metaContainer);
        
        // Empty list filtering should work without error
        List<Binding> filtered = bindingService.listBindings("vhost1", "exchange1");
        assertNotNull(filtered);
        assertEquals(0, filtered.size());
    }
}
