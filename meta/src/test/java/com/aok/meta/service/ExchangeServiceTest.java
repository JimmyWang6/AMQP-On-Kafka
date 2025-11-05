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

import com.aok.meta.Exchange;
import com.aok.meta.ExchangeType;
import com.aok.meta.container.InMemoryMetaContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExchangeServiceTest {

    private ExchangeService exchangeService;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        InMemoryMetaContainer metaContainer = new InMemoryMetaContainer();
        exchangeService = new ExchangeService((com.aok.meta.container.MetaContainer) metaContainer);
    }

    @Test
    void testAddExchange() {
        String vhost = "testVhost";
        String name = "testExchange";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("alternate-exchange", "backup");

        Exchange previousExchange = exchangeService.addExchange(vhost, name, ExchangeType.Direct, true, true, false, arguments);

        // First add should return null (no previous value)
        assertNull(previousExchange);
        
        // Verify the exchange was added by retrieving it
        Exchange exchange = exchangeService.getExchange(vhost, name);
        assertNotNull(exchange);
        assertEquals(vhost, exchange.getVhost());
        assertEquals(name, exchange.getName());
        assertEquals(ExchangeType.Direct, exchange.getType());
        assertTrue(exchange.getAutoDelete());
        assertTrue(exchange.getDurable());
        assertFalse(exchange.isInternal());
        assertEquals(arguments, exchange.getArguments());
    }

    @Test
    void testAddDifferentExchangeTypes() {
        exchangeService.addExchange("vhost1", "direct", ExchangeType.Direct, false, true, false, null);
        exchangeService.addExchange("vhost1", "fanout", ExchangeType.Fanout, false, true, false, null);
        exchangeService.addExchange("vhost1", "topic", ExchangeType.Topic, false, true, false, null);
        exchangeService.addExchange("vhost1", "headers", ExchangeType.Headers, false, true, false, null);

        assertEquals(4, exchangeService.size());
    }

    @Test
    void testGetExchange() {
        String vhost = "testVhost";
        String name = "testExchange";
        
        exchangeService.addExchange(vhost, name, ExchangeType.Direct, false, true, false, null);
        Exchange retrieved = exchangeService.getExchange(vhost, name);

        assertNotNull(retrieved);
        assertEquals(vhost, retrieved.getVhost());
        assertEquals(name, retrieved.getName());
        assertEquals(ExchangeType.Direct, retrieved.getType());
    }

    @Test
    void testGetNonExistentExchange() {
        Exchange retrieved = exchangeService.getExchange("nonexistent", "exchange");
        assertNull(retrieved);
    }

    @Test
    void testDeleteExchange() {
        String vhost = "testVhost";
        String name = "testExchange";
        
        exchangeService.addExchange(vhost, name, ExchangeType.Direct, false, true, false, null);
        Exchange addedExchange = exchangeService.getExchange(vhost, name);
        assertNotNull(addedExchange);

        Exchange deleted = exchangeService.deleteExchange(addedExchange);
        assertNotNull(deleted);
        assertEquals(name, deleted.getName());
        assertNull(exchangeService.getExchange(vhost, name));
    }

    @Test
    void testSize() {
        assertEquals(0, exchangeService.size());
        
        exchangeService.addExchange("vhost1", "exchange1", ExchangeType.Direct, false, true, false, null);
        assertEquals(1, exchangeService.size());
        
        exchangeService.addExchange("vhost1", "exchange2", ExchangeType.Fanout, false, true, false, null);
        assertEquals(2, exchangeService.size());
    }

    @Test
    void testAddExchangeWithInternalFlag() {
        String vhost = "testVhost";
        String name = "internalExchange";

        exchangeService.addExchange(vhost, name, ExchangeType.Direct, false, true, true, null);

        Exchange exchange = exchangeService.getExchange(vhost, name);
        assertNotNull(exchange);
        assertTrue(exchange.isInternal());
    }

    @Test
    void testAddMultipleExchangesWithSameName() {
        String vhost = "testVhost";
        String name = "testExchange";
        
        exchangeService.addExchange(vhost, name, ExchangeType.Direct, false, true, false, null);
        exchangeService.addExchange(vhost, name, ExchangeType.Fanout, true, false, true, null);

        // Adding with same name should replace
        Exchange retrieved = exchangeService.getExchange(vhost, name);
        assertNotNull(retrieved);
        assertEquals(ExchangeType.Fanout, retrieved.getType());
        assertEquals(1, exchangeService.size());
    }
}
