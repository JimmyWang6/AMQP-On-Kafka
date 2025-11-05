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

import com.aok.meta.Vhost;
import com.aok.meta.container.InMemoryMetaContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VhostServiceTest {

    private VhostService vhostService;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        InMemoryMetaContainer metaContainer = new InMemoryMetaContainer();
        vhostService = new VhostService((com.aok.meta.container.MetaContainer) metaContainer);
    }

    @Test
    void testAddVhost() {
        String name = "testVhost";

        Vhost previousVhost = vhostService.addVhost(name);

        // First add should return null (no previous value)
        assertNull(previousVhost);
        
        // Verify by listing
        assertEquals(1, vhostService.size());
    }

    @Test
    void testAddMultipleVhosts() {
        vhostService.addVhost("vhost1");
        vhostService.addVhost("vhost2");
        vhostService.addVhost("vhost3");

        assertEquals(3, vhostService.size());
    }

    @Test
    void testDeleteVhost() {
        String name = "testVhost";
        
        vhostService.addVhost(name);
        assertEquals(1, vhostService.size());
        
        // Get the vhost to delete it
        Vhost vhost = vhostService.listVhost().get(0);

        Vhost deleted = vhostService.deleteVhost(vhost);
        assertNotNull(deleted);
        assertEquals(name, deleted.getName());
        assertEquals(0, vhostService.size());
    }

    @Test
    void testListVhost() {
        vhostService.addVhost("vhost1");
        vhostService.addVhost("vhost2");
        vhostService.addVhost("vhost3");

        List<Vhost> vhosts = vhostService.listVhost();
        assertEquals(3, vhosts.size());
    }

    @Test
    void testListVhostEmpty() {
        List<Vhost> vhosts = vhostService.listVhost();
        assertNotNull(vhosts);
        assertEquals(0, vhosts.size());
    }

    @Test
    void testSize() {
        assertEquals(0, vhostService.size());
        
        vhostService.addVhost("vhost1");
        assertEquals(1, vhostService.size());
        
        vhostService.addVhost("vhost2");
        assertEquals(2, vhostService.size());
    }

    @Test
    void testAddVhostWithSlashInName() {
        String name = "/vhost";
        
        vhostService.addVhost(name);
        
        assertEquals(1, vhostService.size());
        Vhost vhost = vhostService.listVhost().get(0);
        assertEquals(name, vhost.getVhost());
    }

    @Test
    void testAddVhostWithSameNameReplaces() {
        String name = "testVhost";
        
        vhostService.addVhost(name);
        vhostService.addVhost(name);

        // Adding with same name should replace
        assertEquals(1, vhostService.size());
    }
}
