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
package com.aok.core.ack;

/**
 * Interface for acknowledgment service supporting multiple implementations
 * (Kafka, Memory, File, etc.)
 */
public interface AckService {
    
    /**
     * Acknowledges a message or multiple messages
     * 
     * @param deliveryTag the delivery tag to acknowledge
     * @param multiple if true, acknowledges all messages up to and including this tag
     */
    void acknowledge(long deliveryTag, boolean multiple);
    
    /**
     * Negatively acknowledges a message or multiple messages
     * 
     * @param deliveryTag the delivery tag to nack
     * @param multiple if true, nacks all messages up to and including this tag
     * @param requeue if true, messages should be requeued
     */
    void nack(long deliveryTag, boolean multiple, boolean requeue);
    
    /**
     * Rejects a message
     * 
     * @param deliveryTag the delivery tag to reject
     * @param requeue if true, the message should be requeued
     */
    void reject(long deliveryTag, boolean requeue);
}
