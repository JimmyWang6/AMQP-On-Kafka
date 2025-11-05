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
     * Acknowledges a Kafka record for a consumer
     * 
     * @param consumerTag the consumer tag
     * @param recordRef the record reference to acknowledge
     */
    void acknowledgeRecord(String consumerTag, Object recordRef);
    
    /**
     * Callback interface for handling consumed messages
     */
    interface MessageHandler {
        /**
         * Called when a message is received from the backend
         * 
         * @param message the received message
         * @param recordRef reference to the backend record (for acknowledgment)
         */
        void handleMessage(Message message, Object recordRef);
    }
}

