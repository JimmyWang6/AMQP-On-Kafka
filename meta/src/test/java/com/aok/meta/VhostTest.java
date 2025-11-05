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

class VhostTest {

    @Test
    void testVhostConstructor() {
        String name = "testVhost";
        
        Vhost vhost = new Vhost(name);

        assertEquals(name, vhost.getVhost());
        assertEquals(name, vhost.getName());
        assertEquals("vhost", vhost.getMetaType());
    }

    @Test
    void testVhostNoArgsConstructor() {
        Vhost vhost = new Vhost();
        
        assertNull(vhost.getVhost());
        assertNull(vhost.getName());
    }

    @Test
    void testVhostEquality() {
        Vhost vhost1 = new Vhost("vhost1");
        Vhost vhost2 = new Vhost("vhost1");
        Vhost vhost3 = new Vhost("vhost2");

        assertEquals(vhost1, vhost2);
        // vhost1 and vhost3 have different names, so they should not be equal
        assertFalse(vhost1.getName().equals(vhost3.getName()));
    }

    @Test
    void testVhostSetters() {
        Vhost vhost = new Vhost();
        
        vhost.setVhost("vhost1");
        vhost.setName("vhost1");

        assertEquals("vhost1", vhost.getVhost());
        assertEquals("vhost1", vhost.getName());
    }

    @Test
    void testVhostWithSlashInName() {
        String name = "/vhost";
        Vhost vhost = new Vhost(name);

        assertEquals(name, vhost.getVhost());
        assertEquals(name, vhost.getName());
    }
}
