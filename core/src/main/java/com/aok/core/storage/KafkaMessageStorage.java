package com.aok.core.storage;

import com.aok.core.storage.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka-based message storage implementation.
 * Uses ProducerPool to manage Kafka producers efficiently.
 * Each queue gets its own Kafka topic with a shared producer per queue.
 */
@Slf4j
public class KafkaMessageStorage implements IStorage {

    private final ProducerPool producerPool;

    public KafkaMessageStorage(ProducerPool producerPool) {
        this.producerPool = producerPool;
    }

    @Override
    public void produce(Message message) {
        // Generate unique key for vhost + queue combination
        String key = CommonUtils.generateKey(message.getVhost(), message.getQueue());
        
        // Get or create producer for this queue
        // ProducerPool.getProducer() will automatically initialize a new producer if one doesn't exist
        Producer<String, Message> producer = producerPool.getProducer(key);
        
        if (producer == null) {
            log.error("Failed to get producer for key: {}", key);
            throw new RuntimeException("Failed to get Kafka producer for queue: " + message.getQueue());
        }
        
        // Create Kafka record with the queue key as the topic
        ProducerRecord<String, Message> record = new ProducerRecord<>(key, message);
        
        // Send message to Kafka asynchronously
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send message to Kafka topic {} for queue {}: {}", 
                    key, message.getQueue(), exception.getMessage(), exception);
            } else {
                log.debug("Message sent to Kafka topic {} at offset {} for queue {}", 
                    key, metadata.offset(), message.getQueue());
            }
        });
    }
}
