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

class ExchangeTest {

    @Test
    void testExchangeConstructor() {
        String vhost = "testVhost";
        String name = "testExchange";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("alternate-exchange", "backup");

        Exchange exchange = new Exchange(vhost, name, ExchangeType.Direct, true, true, false, arguments);

        assertEquals(vhost, exchange.getVhost());
        assertEquals(name, exchange.getName());
        assertEquals(ExchangeType.Direct, exchange.getType());
        assertTrue(exchange.getAutoDelete());
        assertTrue(exchange.getDurable());
        assertFalse(exchange.isInternal());
        assertEquals(arguments, exchange.getArguments());
        assertEquals("exchange", exchange.getMetaType());
    }

    @Test
    void testExchangeWithNullArguments() {
        String vhost = "testVhost";
        String name = "testExchange";

        Exchange exchange = new Exchange(vhost, name, ExchangeType.Topic, false, false, true, null);

        assertEquals(vhost, exchange.getVhost());
        assertEquals(name, exchange.getName());
        assertEquals(ExchangeType.Topic, exchange.getType());
        assertFalse(exchange.getAutoDelete());
        assertFalse(exchange.getDurable());
        assertTrue(exchange.isInternal());
        assertNull(exchange.getArguments());
    }

    @Test
    void testExchangeEquality() {
        Exchange exchange1 = new Exchange("vhost1", "exchange1", ExchangeType.Fanout, false, true, false, null);
        Exchange exchange2 = new Exchange("vhost1", "exchange1", ExchangeType.Fanout, false, true, false, null);
        Exchange exchange3 = new Exchange("vhost1", "exchange2", ExchangeType.Fanout, false, true, false, null);

        assertEquals(exchange1, exchange2);
        // exchange1 and exchange3 have different names, so they should not be equal
        assertFalse(exchange1.getName().equals(exchange3.getName()));
    }

    @Test
    void testExchangeSetters() {
        Exchange exchange = new Exchange();
        
        exchange.setVhost("vhost1");
        exchange.setName("exchange1");
        exchange.setType(ExchangeType.Headers);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        exchange.setInternal(true);
        
        Map<String, Object> args = new HashMap<>();
        args.put("test", "value");
        exchange.setArguments(args);

        assertEquals("vhost1", exchange.getVhost());
        assertEquals("exchange1", exchange.getName());
        assertEquals(ExchangeType.Headers, exchange.getType());
        assertTrue(exchange.getDurable());
        assertFalse(exchange.getAutoDelete());
        assertTrue(exchange.isInternal());
        assertEquals(args, exchange.getArguments());
    }

    @Test
    void testExchangeToString() {
        Exchange exchange = new Exchange("vhost1", "exchange1", ExchangeType.Direct, false, true, false, null);
        String result = exchange.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("Direct"));
        assertTrue(result.contains("vhost1"));
        assertTrue(result.contains("exchange1"));
    }
}
