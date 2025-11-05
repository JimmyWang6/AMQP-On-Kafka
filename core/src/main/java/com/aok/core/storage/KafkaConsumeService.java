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
import com.aok.core.storage.message.MessageDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Kafka-based message consumption service using Kafka Share Groups (KIP-932).
 * Each consumer gets messages from a queue using share group semantics,
 * allowing multiple consumers to share consumption of messages.
 */
@Slf4j
public class KafkaConsumeService implements ConsumeService {
    
    private final Map<String, ConsumerTask> activeTasks = new ConcurrentHashMap<>();
    private final ExecutorService executorService;
    private final Properties kafkaConsumerConfig;
    
    public KafkaConsumeService(Properties kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("kafka-consumer-thread");
            t.setDaemon(true);
            return t;
        });
    }
    
    @Override
    public void startConsuming(Consumer consumer, MessageHandler messageHandler) {
        String consumerTag = consumer.getConsumerTag();
        String queueName = consumer.getQueueName();
        
        if (activeTasks.containsKey(consumerTag)) {
            log.warn("Consumer {} is already active, skipping start", consumerTag);
            return;
        }
        
        log.info("Starting consumer {} for queue {}", consumerTag, queueName);
        
        // Create Kafka share consumer for this AMQP consumer
        KafkaConsumer<String, Message> kafkaConsumer = createKafkaConsumer(consumerTag, queueName);
        
        // Subscribe to the queue topic
        String topic = CommonUtils.generateKey(consumer.getChannel().getConnection().getVhost(), queueName);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        
        // Create consumer task
        ConsumerTask task = new ConsumerTask(kafkaConsumer, consumer, messageHandler);
        
        // Start task and store for management
        task.future = executorService.submit(task);
        activeTasks.put(consumerTag, task);
        
        log.debug("Consumer {} started successfully", consumerTag);
    }
    
    @Override
    public void stopConsuming(String consumerTag) {
        ConsumerTask task = activeTasks.remove(consumerTag);
        if (task != null) {
            log.info("Stopping consumer {}", consumerTag);
            task.stop();
        } else {
            log.warn("Consumer {} not found in active tasks", consumerTag);
        }
    }
    
    /**
     * Creates a Kafka share consumer for the given consumer tag and queue
     */
    private KafkaConsumer<String, Message> createKafkaConsumer(String consumerTag, String queueName) {
        Properties props = new Properties();
        props.putAll(kafkaConsumerConfig);
        
        // Use share group protocol (KIP-932)
        // Share group ID is based on queue name to allow multiple consumers to share consumption
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "share-" + queueName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit for ack control
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new KafkaConsumer<>(props);
    }
    
    /**
     * Shuts down the consume service and all active consumers
     */
    public void shutdown() {
        log.info("Shutting down KafkaConsumeService with {} active consumers", activeTasks.size());
        
        // Stop all active consumers
        activeTasks.values().forEach(ConsumerTask::stop);
        activeTasks.clear();
        
        // Shutdown executor
        executorService.shutdown();
    }
    
    /**
     * Task that runs in background thread to poll Kafka and deliver messages
     */
    private static class ConsumerTask implements Runnable {
        private final KafkaConsumer<String, Message> kafkaConsumer;
        private final Consumer amqpConsumer;
        private final MessageHandler messageHandler;
        private volatile boolean running = true;
        private volatile Future<?> future;
        
        ConsumerTask(KafkaConsumer<String, Message> kafkaConsumer, Consumer amqpConsumer, 
                    MessageHandler messageHandler) {
            this.kafkaConsumer = kafkaConsumer;
            this.amqpConsumer = amqpConsumer;
            this.messageHandler = messageHandler;
        }
        
        @Override
        public void run() {
            log.info("Consumer task started for {}", amqpConsumer.getConsumerTag());
            
            try {
                while (running && amqpConsumer.isActive()) {
                    // Poll for messages with timeout
                    ConsumerRecords<String, Message> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    
                    for (ConsumerRecord<String, Message> record : records) {
                        if (!running || !amqpConsumer.isActive()) {
                            break;
                        }
                        
                        try {
                            // Deliver message to AMQP client via callback
                            messageHandler.handleMessage(
                                record.value(), 
                                record.topic(), 
                                record.partition(), 
                                record.offset()
                            );
                            
                            amqpConsumer.incrementMessageCount();
                            
                            log.debug("Delivered message from offset {} to consumer {}", 
                                record.offset(), amqpConsumer.getConsumerTag());
                        } catch (Exception e) {
                            log.error("Error delivering message to consumer {}", 
                                amqpConsumer.getConsumerTag(), e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Consumer task error for {}", amqpConsumer.getConsumerTag(), e);
            } finally {
                try {
                    kafkaConsumer.close();
                    log.info("Consumer task stopped for {}", amqpConsumer.getConsumerTag());
                } catch (Exception e) {
                    log.error("Error closing Kafka consumer", e);
                }
            }
        }
        
        void stop() {
            running = false;
            kafkaConsumer.wakeup();
        }
    }
}
