package com.aok.core.storage.message;

import lombok.Data;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;

import java.io.Serializable;
import java.util.Map;

/**
 * Message object for Kafka storage containing all AMQP message data
 */
@Data
public class Message implements Serializable {
    
    private static final long serialVersionUID = 1L;

    // Routing information
    private String vhost;
    private String queue;
    private String exchange;
    private String routingKey;
    
    // Message body
    private byte[] body;
    
    // Message properties from AMQP BasicContentHeaderProperties
    private String contentType;
    private String contentEncoding;
    private Map<String, Object> headers;
    private Integer deliveryMode;
    private Integer priority;
    private String correlationId;
    private String replyTo;
    private String expiration;
    private String messageId;
    private Long timestamp;
    private String type;
    private String userId;
    private String appId;
    
    // Additional flags
    private boolean mandatory;
    private boolean immediate;
    
    // Message size
    private long bodySize;
}
