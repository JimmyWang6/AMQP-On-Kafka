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
package com.aok.core;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for managing all active AMQP consumers.
 * Provides statistics and management capabilities for consumers.
 */
@Slf4j
public class ConsumerContainer {
    
    // Map: consumerTag -> Consumer
    private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();
    
    // Map: channelId -> Map of consumerTag -> Consumer
    private final Map<Integer, Map<String, Consumer>> consumersByChannel = new ConcurrentHashMap<>();
    
    /**
     * Adds a consumer to the container
     * 
     * @param consumer the consumer to add
     */
    public void addConsumer(Consumer consumer) {
        String consumerTag = consumer.getConsumerTag();
        int channelId = consumer.getChannel().getChannelId();
        
        consumers.put(consumerTag, consumer);
        
        consumersByChannel.computeIfAbsent(channelId, k -> new ConcurrentHashMap<>())
            .put(consumerTag, consumer);
        
        log.debug("Added consumer {} to container, total consumers: {}", consumerTag, consumers.size());
    }
    
    /**
     * Removes a consumer from the container
     * 
     * @param consumerTag the consumer tag to remove
     * @return the removed consumer, or null if not found
     */
    public Consumer removeConsumer(String consumerTag) {
        Consumer consumer = consumers.remove(consumerTag);
        if (consumer != null) {
            int channelId = consumer.getChannel().getChannelId();
            Map<String, Consumer> channelConsumers = consumersByChannel.get(channelId);
            if (channelConsumers != null) {
                channelConsumers.remove(consumerTag);
                if (channelConsumers.isEmpty()) {
                    consumersByChannel.remove(channelId);
                }
            }
            log.debug("Removed consumer {} from container, total consumers: {}", consumerTag, consumers.size());
        }
        return consumer;
    }
    
    /**
     * Gets a consumer by tag
     * 
     * @param consumerTag the consumer tag
     * @return the consumer, or null if not found
     */
    public Consumer getConsumer(String consumerTag) {
        return consumers.get(consumerTag);
    }
    
    /**
     * Gets all consumers for a specific channel
     * 
     * @param channelId the channel ID
     * @return collection of consumers for the channel
     */
    public Collection<Consumer> getConsumersByChannel(int channelId) {
        Map<String, Consumer> channelConsumers = consumersByChannel.get(channelId);
        return channelConsumers != null ? channelConsumers.values() : java.util.Collections.emptyList();
    }
    
    /**
     * Removes all consumers for a specific channel
     * 
     * @param channelId the channel ID
     */
    public void removeConsumersByChannel(int channelId) {
        Map<String, Consumer> channelConsumers = consumersByChannel.remove(channelId);
        if (channelConsumers != null) {
            channelConsumers.keySet().forEach(consumers::remove);
            log.info("Removed {} consumers for channel {}", channelConsumers.size(), channelId);
        }
    }
    
    /**
     * Gets the total number of consumers
     * 
     * @return total consumer count
     */
    public int getTotalConsumerCount() {
        return consumers.size();
    }
    
    /**
     * Gets the number of consumers for a specific channel
     * 
     * @param channelId the channel ID
     * @return consumer count for the channel
     */
    public int getConsumerCountByChannel(int channelId) {
        Map<String, Consumer> channelConsumers = consumersByChannel.get(channelId);
        return channelConsumers != null ? channelConsumers.size() : 0;
    }
    
    /**
     * Checks if a consumer tag exists
     * 
     * @param consumerTag the consumer tag
     * @return true if exists
     */
    public boolean containsConsumer(String consumerTag) {
        return consumers.containsKey(consumerTag);
    }
    
    /**
     * Gets all consumers
     * 
     * @return collection of all consumers
     */
    public Collection<Consumer> getAllConsumers() {
        return consumers.values();
    }
}
