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
import com.aok.core.ConsumerContainer;
import com.aok.core.storage.message.Message;
import com.aok.core.storage.message.MessageDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka-based message consumption service using Kafka Share Groups (KIP-932).
 * Uses a single main thread to poll all share consumers for efficiency.
 * Supports manual acknowledgment for noAck=false consumers.
 */
@Slf4j
public class KafkaConsumeService implements ConsumeService {
    
    // Map: consumerTag -> ShareConsumerInfo
    private final Map<String, ShareConsumerInfo> activeConsumers = new ConcurrentHashMap<>();
    private final Properties kafkaConsumerConfig;
    private final ConsumerContainer consumerContainer;
    private final Thread pollingThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Duration pollTimeout = Duration.ofMillis(100);
    
    public KafkaConsumeService(Properties kafkaConsumerConfig, ConsumerContainer consumerContainer) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.consumerContainer = consumerContainer;
        this.pollingThread = new Thread(this::pollAllConsumers, "kafka-share-consumer-poller");
        this.pollingThread.setDaemon(true);
    }
    
    /**
     * Starts the polling thread
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            pollingThread.start();
            log.info("KafkaConsumeService started");
        }
    }
    
    @Override
    public void startConsuming(Consumer consumer, MessageHandler messageHandler) {
        String consumerTag = consumer.getConsumerTag();
        String queueName = consumer.getQueueName();
        
        if (activeConsumers.containsKey(consumerTag)) {
            log.warn("Consumer {} is already active, skipping start", consumerTag);
            return;
        }
        
        log.info("Starting consumer {} for queue {}", consumerTag, queueName);
        
        // Create Kafka share consumer for this AMQP consumer
        KafkaShareConsumer<String, Message> kafkaShareConsumer = createKafkaShareConsumer(consumerTag, queueName);
        
        // Subscribe to the queue topic
        String topic = CommonUtils.generateKey(consumer.getChannel().getConnection().getVhost(), queueName);
        kafkaShareConsumer.subscribe(Collections.singletonList(topic));
        
        // Store consumer info
        ShareConsumerInfo info = new ShareConsumerInfo(kafkaShareConsumer, consumer, messageHandler);
        activeConsumers.put(consumerTag, info);
        
        log.debug("Consumer {} started successfully, total active consumers: {}", 
            consumerTag, activeConsumers.size());
    }
    
    @Override
    public void stopConsuming(String consumerTag) {
        ShareConsumerInfo info = activeConsumers.remove(consumerTag);
        if (info != null) {
            log.info("Stopping consumer {}", consumerTag);
            try {
                info.kafkaShareConsumer.close();
                log.debug("Consumer {} stopped, total active consumers: {}", 
                    consumerTag, activeConsumers.size());
            } catch (Exception e) {
                log.error("Error closing Kafka share consumer for {}", consumerTag, e);
            }
        } else {
            log.warn("Consumer {} not found in active consumers", consumerTag);
        }
    }
    
    /**
     * Acknowledges a message for manual ack consumers.
     * This is called when receiveBasicAck is invoked by the AMQP client.
     * 
     * @param consumerTag the consumer tag
     * @param topic the Kafka topic
     * @param partition the Kafka partition
     * @param offset the Kafka offset
     */
    public void acknowledgeMessage(String consumerTag, String topic, int partition, long offset) {
        ShareConsumerInfo info = activeConsumers.get(consumerTag);
        if (info != null) {
            try {
                // Acknowledge the specific record in the share group
                // Note: We create a minimal ConsumerRecord for acknowledgment
                org.apache.kafka.clients.consumer.ConsumerRecord<String, Message> record = 
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                        topic, partition, offset, null, null);
                info.kafkaShareConsumer.acknowledge(record);
                log.debug("Acknowledged message for consumer {}: topic={}, partition={}, offset={}", 
                    consumerTag, topic, partition, offset);
            } catch (Exception e) {
                log.error("Failed to acknowledge message for consumer {}", consumerTag, e);
            }
        } else {
            log.warn("Cannot acknowledge - consumer {} not found", consumerTag);
        }
    }
    
    /**
     * Creates a Kafka share consumer for the given consumer tag and queue
     */
    private KafkaShareConsumer<String, Message> createKafkaShareConsumer(String consumerTag, String queueName) {
        Properties props = new Properties();
        props.putAll(kafkaConsumerConfig);
        
        // Use share group protocol (KIP-932)
        // Share group ID is based on queue name to allow multiple consumers to share consumption
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "share-" + queueName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        // Disable auto-commit - we use manual acknowledgment for both auto-ack and manual-ack consumers
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new KafkaShareConsumer<>(props);
    }
    
    /**
     * Main polling loop that polls all active share consumers in a single thread
     */
    private void pollAllConsumers() {
        log.info("Kafka share consumer polling thread started");
        
        while (running.get()) {
            try {
                // Create a snapshot of active consumers to avoid ConcurrentModificationException
                // when consumers are added/removed by other threads
                Map<String, ShareConsumerInfo> snapshot = new java.util.HashMap<>(activeConsumers);
                
                // Poll each active consumer
                for (Map.Entry<String, ShareConsumerInfo> entry : snapshot.entrySet()) {
                    String consumerTag = entry.getKey();
                    ShareConsumerInfo info = entry.getValue();
                    
                    if (!info.consumer.isActive()) {
                        log.debug("Consumer {} is not active, skipping poll", consumerTag);
                        continue;
                    }
                    
                    try {
                        // Poll for messages with short timeout
                        ConsumerRecords<String, Message> records = info.kafkaShareConsumer.poll(pollTimeout);
                        
                        for (ConsumerRecord<String, Message> record : records) {
                            try {
                                // Deliver message to AMQP client via callback
                                info.messageHandler.handleMessage(
                                    record.value(),
                                    record.topic(),
                                    record.partition(),
                                    record.offset()
                                );
                                
                                info.consumer.incrementMessageCount();
                                
                                // For auto-ack consumers (noAck=true), acknowledge immediately
                                if (info.consumer.isNoAck()) {
                                    info.kafkaShareConsumer.acknowledge(record);
                                    log.debug("Auto-acknowledged message for consumer {}: offset={}", 
                                        consumerTag, record.offset());
                                }
                                // For manual ack consumers (noAck=false), acknowledgment happens via receiveBasicAck
                                
                                log.trace("Delivered message from offset {} to consumer {}", 
                                    record.offset(), consumerTag);
                            } catch (Exception e) {
                                log.error("Error delivering message to consumer {}", consumerTag, e);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error polling consumer {}", consumerTag, e);
                    }
                }
                
                // Small sleep to avoid tight loop when no consumers
                if (activeConsumers.isEmpty()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                log.info("Polling thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in polling loop", e);
            }
        }
        
        log.info("Kafka share consumer polling thread stopped");
    }
    
    /**
     * Shuts down the consume service and all active consumers
     */
    public void shutdown() {
        log.info("Shutting down KafkaConsumeService with {} active consumers", activeConsumers.size());
        
        // Stop polling thread
        running.set(false);
        pollingThread.interrupt();
        
        // Close all active consumers
        activeConsumers.values().forEach(info -> {
            try {
                info.kafkaShareConsumer.close();
            } catch (Exception e) {
                log.error("Error closing Kafka share consumer", e);
            }
        });
        activeConsumers.clear();
        
        log.info("KafkaConsumeService shutdown complete");
    }
    
    /**
     * Information about an active share consumer
     */
    private static class ShareConsumerInfo {
        final KafkaShareConsumer<String, Message> kafkaShareConsumer;
        final Consumer consumer;
        final MessageHandler messageHandler;
        
        ShareConsumerInfo(KafkaShareConsumer<String, Message> kafkaShareConsumer, 
                         Consumer consumer, MessageHandler messageHandler) {
            this.kafkaShareConsumer = kafkaShareConsumer;
            this.consumer = consumer;
            this.messageHandler = messageHandler;
        }
    }
}
