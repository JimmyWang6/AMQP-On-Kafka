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

import com.aok.core.ack.AckService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages unacknowledged messages for a channel using delivery tags.
 * Delivery tags are unique identifiers assigned to each message delivered on a channel,
 * used for acknowledgment, rejection, and recovery operations.
 */
@Slf4j
public class UnacknowledgedMessageMap {
    
    private final NavigableMap<Long, MessageMetadata> unacknowledgedMessages = new TreeMap<>();
    private final AtomicLong deliveryTagSequence = new AtomicLong(0);
    private final AckService ackService;
    
    public UnacknowledgedMessageMap(AckService ackService) {
        this.ackService = ackService;
    }
    
    /**
     * Generates a new delivery tag in ascending order
     * 
     * @return the next delivery tag
     */
    public long generateDeliveryTag() {
        return deliveryTagSequence.incrementAndGet();
    }
    
    /**
     * Adds a message to the unacknowledged map
     * 
     * @param deliveryTag the delivery tag for this message
     * @param metadata metadata about the message
     */
    public void addMessage(long deliveryTag, MessageMetadata metadata) {
        unacknowledgedMessages.put(deliveryTag, metadata);
        log.debug("Added unacknowledged message with deliveryTag: {}", deliveryTag);
    }
    
    /**
     * Acknowledges a message or multiple messages
     * 
     * @param deliveryTag the delivery tag to acknowledge
     * @param multiple if true, acknowledges all messages up to and including this tag
     * @return the number of messages acknowledged
     */
    public int acknowledge(long deliveryTag, boolean multiple) {
        // First, acknowledge to the backend service (Kafka/Memory/File)
        if (ackService != null) {
            ackService.acknowledge(deliveryTag, multiple);
        }
        
        // Then remove from local store
        int count = 0;
        if (multiple) {
            // Acknowledge all messages with tags <= deliveryTag
            NavigableMap<Long, MessageMetadata> toAck = unacknowledgedMessages.headMap(deliveryTag, true);
            count = toAck.size();
            toAck.clear();
            log.debug("Acknowledged {} messages up to deliveryTag: {}", count, deliveryTag);
        } else {
            // Acknowledge single message
            MessageMetadata removed = unacknowledgedMessages.remove(deliveryTag);
            if (removed != null) {
                count = 1;
                log.debug("Acknowledged single message with deliveryTag: {}", deliveryTag);
            } else {
                log.warn("Attempted to acknowledge unknown deliveryTag: {}", deliveryTag);
            }
        }
        return count;
    }
    
    /**
     * Negatively acknowledges a message or multiple messages
     * 
     * @param deliveryTag the delivery tag to nack
     * @param multiple if true, nacks all messages up to and including this tag
     * @param requeue if true, messages should be requeued
     * @return the number of messages nacked
     */
    public int nack(long deliveryTag, boolean multiple, boolean requeue) {
        // First, nack to the backend service (Kafka/Memory/File)
        if (ackService != null) {
            ackService.nack(deliveryTag, multiple, requeue);
        }
        
        // Then remove from local store
        int count = 0;
        if (multiple) {
            // Nack all messages with tags <= deliveryTag
            NavigableMap<Long, MessageMetadata> toNack = unacknowledgedMessages.headMap(deliveryTag, true);
            count = toNack.size();
            toNack.clear();
            log.debug("Nacked {} messages up to deliveryTag: {}, requeue: {}", count, deliveryTag, requeue);
        } else {
            // Nack single message
            MessageMetadata removed = unacknowledgedMessages.remove(deliveryTag);
            if (removed != null) {
                count = 1;
                log.debug("Nacked single message with deliveryTag: {}, requeue: {}", deliveryTag, requeue);
            } else {
                log.warn("Attempted to nack unknown deliveryTag: {}", deliveryTag);
            }
        }
        return count;
    }
    
    /**
     * Rejects a message
     * 
     * @param deliveryTag the delivery tag to reject
     * @param requeue if true, the message should be requeued
     * @return true if the message was found and rejected
     */
    public boolean reject(long deliveryTag, boolean requeue) {
        // First, reject to the backend service (Kafka/Memory/File)
        if (ackService != null) {
            ackService.reject(deliveryTag, requeue);
        }
        
        // Then remove from local store
        MessageMetadata removed = unacknowledgedMessages.remove(deliveryTag);
        if (removed != null) {
            log.debug("Rejected message with deliveryTag: {}, requeue: {}", deliveryTag, requeue);
            return true;
        } else {
            log.warn("Attempted to reject unknown deliveryTag: {}", deliveryTag);
            return false;
        }
    }
    
    /**
     * Gets the number of unacknowledged messages
     */
    public int size() {
        return unacknowledgedMessages.size();
    }
    
    /**
     * Clears all unacknowledged messages (typically when channel closes)
     */
    public void clear() {
        int count = unacknowledgedMessages.size();
        unacknowledgedMessages.clear();
        log.debug("Cleared {} unacknowledged messages", count);
    }
    
    /**
     * Metadata about a delivered message
     */
    @Data
    public static class MessageMetadata {
        private final String queue;
        private final long timestamp;
        
        public MessageMetadata(String queue) {
            this.queue = queue;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
