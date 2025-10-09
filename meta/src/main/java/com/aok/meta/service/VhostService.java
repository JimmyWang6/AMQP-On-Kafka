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
import com.aok.meta.container.MetaContainer;

import java.util.List;

public class VhostService {

    private final MetaContainer<Vhost> metaContainer;

    public VhostService(MetaContainer<Vhost> metaContainer) {
        this.metaContainer = metaContainer;
    }

    public Vhost addVhost(String name) {
        Vhost vhost = new Vhost(name);
        return metaContainer.add(vhost);
    }

    public Vhost deleteVhost(Vhost vhost) {
        return metaContainer.delete(vhost);
    }

    public List<Vhost> listVhost() {
        return metaContainer.list(Vhost.class);
    }

    public int size() {
        return metaContainer.size(Vhost.class);
    }
}

