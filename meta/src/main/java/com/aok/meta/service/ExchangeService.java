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
import com.aok.meta.container.MetaContainer;

import java.util.Map;

public class ExchangeService {

    private final MetaContainer<Exchange> metaContainer;

    public ExchangeService(MetaContainer<Exchange> metaContainer) {
        this.metaContainer = metaContainer;
    }

    public Exchange addExchange(String vhost, String name, ExchangeType exchangeType, Boolean autoDelete, Boolean durable, Boolean internal, Map<String, Object> arguments) {
        Exchange exchange = new Exchange(vhost, name, exchangeType, autoDelete, durable, internal, arguments);
        return metaContainer.add(exchange);
    }

    public Exchange getExchange(String vhost, String name) {
        return metaContainer.get(Exchange.class, vhost, name);
    }

    public Exchange deleteExchange(Exchange exchange) {
        return metaContainer.delete(exchange);
    }

    public int size() {
        return metaContainer.size(Exchange.class);
    }
}
