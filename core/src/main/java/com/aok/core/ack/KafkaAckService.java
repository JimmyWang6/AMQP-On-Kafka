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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka-based acknowledgment service using Kafka Share Consumer (KIP-932).
 * This implementation uses the same KafkaShareConsumer to acknowledge messages.
 * 
 * Share Groups allow multiple consumers to share the same partition while Kafka
 * tracks which messages have been acknowledged by each consumer group member.
 */
@Slf4j
public class KafkaAckService implements AckService {
    
    private final Consumer<String, ?> shareConsumer;
    private final String queueName;
    private final Map<Long, ConsumerRecord> deliveryTagToRecord = new ConcurrentHashMap<>();
    
    /**
     * Creates a KafkaAckService with the specified Kafka Share Consumer
     * 
     * @param shareConsumer the Kafka Share Consumer instance
     * @param queueName the queue name (used for topic identification)
     */
    public KafkaAckService(Consumer<String, ?> shareConsumer, String queueName) {
        this.shareConsumer = shareConsumer;
        this.queueName = queueName;
    }
    
    /**
     * Registers a delivery tag with its corresponding Kafka record
     * This should be called when a message is delivered to track the mapping
     * 
     * @param deliveryTag the AMQP delivery tag
     * @param topic the Kafka topic
     * @param partition the Kafka partition
     * @param offset the Kafka offset
     */
    public void registerDeliveryTag(long deliveryTag, String topic, int partition, long offset) {
        deliveryTagToRecord.put(deliveryTag, new ConsumerRecord(topic, partition, offset));
        log.debug("Registered deliveryTag {} for topic={}, partition={}, offset={}", 
            deliveryTag, topic, partition, offset);
    }
    
    @Override
    public void acknowledge(long deliveryTag, boolean multiple) {
        try {
            if (multiple) {
                // Acknowledge all messages up to and including this delivery tag
                acknowledgeMultiple(deliveryTag);
            } else {
                // Acknowledge single message
                acknowledgeSingle(deliveryTag);
            }
        } catch (Exception e) {
            log.error("Failed to acknowledge deliveryTag: {}, multiple: {}", deliveryTag, multiple, e);
            throw new RuntimeException("Kafka acknowledgment failed", e);
        }
    }
    
    @Override
    public void nack(long deliveryTag, boolean multiple, boolean requeue) {
        try {
            if (requeue) {
                // For requeue, we don't acknowledge - let Kafka redeliver
                log.info("Nack with requeue for deliveryTag: {}, multiple: {} - message will be redelivered", 
                    deliveryTag, multiple);
                // In Kafka Share Groups, not acknowledging means the message will be redelivered
                if (multiple) {
                    removeMultipleRecords(deliveryTag);
                } else {
                    deliveryTagToRecord.remove(deliveryTag);
                }
            } else {
                // For no requeue, acknowledge to prevent redelivery (message is discarded)
                log.info("Nack without requeue for deliveryTag: {}, multiple: {} - message will be discarded", 
                    deliveryTag, multiple);
                acknowledge(deliveryTag, multiple);
            }
        } catch (Exception e) {
            log.error("Failed to nack deliveryTag: {}, multiple: {}, requeue: {}", 
                deliveryTag, multiple, requeue, e);
            throw new RuntimeException("Kafka nack failed", e);
        }
    }
    
    @Override
    public void reject(long deliveryTag, boolean requeue) {
        try {
            if (requeue) {
                // For requeue, don't acknowledge - let Kafka redeliver
                log.info("Reject with requeue for deliveryTag: {} - message will be redelivered", deliveryTag);
                deliveryTagToRecord.remove(deliveryTag);
            } else {
                // For no requeue, acknowledge to prevent redelivery (message is discarded)
                log.info("Reject without requeue for deliveryTag: {} - message will be discarded", deliveryTag);
                acknowledgeSingle(deliveryTag);
            }
        } catch (Exception e) {
            log.error("Failed to reject deliveryTag: {}, requeue: {}", deliveryTag, requeue, e);
            throw new RuntimeException("Kafka reject failed", e);
        }
    }
    
    /**
     * Acknowledges a single message using Kafka Share Consumer
     */
    private void acknowledgeSingle(long deliveryTag) {
        ConsumerRecord record = deliveryTagToRecord.remove(deliveryTag);
        if (record == null) {
            log.warn("Attempted to acknowledge unknown deliveryTag: {}", deliveryTag);
            return;
        }
        
        // Commit the offset for this specific record in the Share Group
        TopicPartition topicPartition = new TopicPartition(record.topic, record.partition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(record.offset + 1));
        
        shareConsumer.commitSync(offsets);
        log.debug("Acknowledged Kafka message: topic={}, partition={}, offset={}", 
            record.topic, record.partition, record.offset);
    }
    
    /**
     * Acknowledges multiple messages up to and including the delivery tag
     */
    private void acknowledgeMultiple(long deliveryTag) {
        Map<TopicPartition, Long> maxOffsets = new HashMap<>();
        
        // Find all records up to and including the delivery tag
        deliveryTagToRecord.entrySet().removeIf(entry -> {
            if (entry.getKey() <= deliveryTag) {
                ConsumerRecord record = entry.getValue();
                TopicPartition tp = new TopicPartition(record.topic, record.partition);
                maxOffsets.compute(tp, (k, v) -> v == null ? record.offset : Math.max(v, record.offset));
                return true;
            }
            return false;
        });
        
        // Commit the maximum offset for each topic-partition
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        maxOffsets.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset + 1)));
        
        if (!offsets.isEmpty()) {
            shareConsumer.commitSync(offsets);
            log.debug("Acknowledged {} Kafka messages up to deliveryTag: {}", offsets.size(), deliveryTag);
        }
    }
    
    /**
     * Removes multiple records from tracking without acknowledging
     */
    private void removeMultipleRecords(long deliveryTag) {
        deliveryTagToRecord.entrySet().removeIf(entry -> entry.getKey() <= deliveryTag);
    }
    
    /**
     * Internal class to track Kafka consumer record information
     */
    private static class ConsumerRecord {
        final String topic;
        final int partition;
        final long offset;
        
        ConsumerRecord(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }
}
