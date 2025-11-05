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
package com.aok.core.storage;

import com.aok.core.Consumer;
import com.aok.core.storage.message.Message;

/**
 * Interface for consuming messages from storage backend (Kafka, etc.)
 */
public interface ConsumeService {
    
    /**
     * Starts consuming messages for a consumer
     * 
     * @param consumer the consumer to start
     * @param messageHandler callback to handle received messages
     */
    void startConsuming(Consumer consumer, MessageHandler messageHandler);
    
    /**
     * Stops consuming messages for a consumer
     * 
     * @param consumerTag the consumer tag to stop
     */
    void stopConsuming(String consumerTag);
    
    /**
     * Callback interface for handling consumed messages
     */
    interface MessageHandler {
        /**
         * Called when a message is received from the backend
         * 
         * @param message the received message
         * @param topic the Kafka topic
         * @param partition the Kafka partition
         * @param offset the Kafka offset
         */
        void handleMessage(Message message, String topic, int partition, long offset);
    }
}
