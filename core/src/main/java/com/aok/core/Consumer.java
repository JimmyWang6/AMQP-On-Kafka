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

import lombok.Data;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * Represents an active AMQP consumer in a channel.
 * Tracks consumer state and configuration.
 */
@Data
public class Consumer {
    
    /**
     * Unique identifier for this consumer within the channel
     */
    private final String consumerTag;
    
    /**
     * Name of the queue this consumer is consuming from
     */
    private final String queueName;
    
    /**
     * The channel this consumer belongs to
     */
    private final AmqpChannel channel;
    
    /**
     * Whether the server should not send messages to the connection that published them
     */
    private final boolean noLocal;
    
    /**
     * Whether messages should be automatically acknowledged
     */
    private final boolean noAck;
    
    /**
     * Whether this consumer has exclusive access to the queue
     */
    private final boolean exclusive;
    
    /**
     * Additional consumer arguments
     */
    private final FieldTable arguments;
    
    /**
     * Timestamp when the consumer was created
     */
    private final long createdAt;
    
    /**
     * Whether this consumer is active (not cancelled)
     */
    private volatile boolean active = true;
    
    /**
     * Number of messages delivered to this consumer
     */
    private volatile long messageCount = 0;
    
    public Consumer(String consumerTag, String queueName, AmqpChannel channel,
                   boolean noLocal, boolean noAck, boolean exclusive, FieldTable arguments) {
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.channel = channel;
        this.noLocal = noLocal;
        this.noAck = noAck;
        this.exclusive = exclusive;
        this.arguments = arguments;
        this.createdAt = System.currentTimeMillis();
    }
    
    /**
     * Marks this consumer as cancelled (inactive)
     */
    public void cancel() {
        this.active = false;
    }
    
    /**
     * Increments the message count for this consumer
     */
    public void incrementMessageCount() {
        this.messageCount++;
    }
}
