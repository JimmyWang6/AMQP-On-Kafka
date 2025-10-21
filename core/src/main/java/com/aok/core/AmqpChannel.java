/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.aok.core;

import com.aok.core.storage.message.Message;
import com.aok.meta.Binding;
import com.aok.meta.Exchange;
import com.aok.meta.ExchangeType;
import com.aok.meta.Queue;
import com.aok.meta.service.BindingService;
import com.aok.meta.service.ExchangeService;
import com.aok.meta.service.QueueService;
import com.aok.meta.service.VhostService;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

import java.util.List;

@Slf4j
public class AmqpChannel implements ServerChannelMethodProcessor {
    
    private final AmqpConnection connection;

    @Getter
    private final int channelId;

    private final VhostService vhostService;

    private final BindingService bindingService;

    private final ExchangeService exchangeService;

    private  final QueueService queueService;
    
    // Message publishing state
    private volatile IncomingMessage currentMessage;
    
    // Acknowledgment tracking
    private final UnacknowledgedMessageMap unacknowledgedMessageMap;
    
    // Transaction state
    private boolean transactionMode = false;
    
    AmqpChannel(
        AmqpConnection connection,
        int channelId,
        VhostService vhostService,
        ExchangeService exchangeService,
        QueueService queueService,
        BindingService bindingService
    ) {
        this.connection = connection;
        this.channelId = channelId;
        this.vhostService = vhostService;
        this.exchangeService = exchangeService;
        this.queueService = queueService;
        this.bindingService = bindingService;
        // Initialize with null AckService for now - will be set when Kafka integration is complete
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap(null);
    }

    private void process(Runnable action) {
        try {
            action.run();
        } catch (AmqpException ex) {
            int errorCode = ex.getErrorCode();
            String message = ex.getMessage();
            if (ex.shouldCloseConnection()) {
                connection.sendConnectionClose(errorCode, message, channelId);
            } else {
                // Close channel on error
                closeChannel(errorCode, message);
            }
        } catch (Exception e) {
            log.error("channel {} process exception {}", channelId, e);
            connection.sendConnectionClose(AmqpException.Codes.INTERNAL_ERROR, e.getMessage(), channelId);
        }
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        AccessRequestOkBody response = connection.getRegistry().createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(1));
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        String exchangeName = exchange == null ? StringUtil.EMPTY_STRING : exchange.toString();
        String vhost = connection.getVhost();
        exchangeService.addExchange(vhost, exchangeName, ExchangeType.value(AMQShortString.toString(type)), autoDelete, durable, internal, FieldTable.convertToMap(arguments));
        connection.writeFrame(connection.getRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        process(() -> {
            String vhost = connection.getVhost();
            Exchange cur = exchangeService.getExchange(vhost, AMQShortString.toString(exchange));
            if (cur != null) {
                exchangeService.deleteExchange(cur);
                log.info("delete exchange {} success", exchange);
            }
            connection.writeFrame(connection.getRegistry().createExchangeDeleteOkBody().generateFrame(channelId));
        });
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        process(() -> {
            String exchangeName = exchange == null ? StringUtil.EMPTY_STRING : exchange.toString();
            String queueName = queue == null ? StringUtil.EMPTY_STRING : queue.toString();
            Exchange e = exchangeService.getExchange(connection.getVhost(), exchangeName);
            Queue q = queueService.getQueue(connection.getVhost(), queueName);
            if (e == null || q == null) {
                ExchangeBoundOkBody exchangeBoundOkBody = connection.getRegistry()
                        .createExchangeBoundOkBody(ExchangeBoundOkBody.OK, AMQShortString.validValueOf("Exchange or Queue not found"));
                connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
                return;
            }
            bindingService.bind(AMQShortString.toString(exchange), AMQShortString.toString(queue), AMQShortString.toString(routingKey));
            ExchangeBoundOkBody exchangeBoundOkBody = connection.getRegistry()
                    .createExchangeBoundOkBody(ExchangeBoundOkBody.OK, null);
            connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
        process(() -> {
            Queue cur = queueService.getQueue(connection.getVhost(), queue.toString());
            if (cur != null) {
                if (passive) {
                    QueueDeclareOkBody responseBody = connection.getRegistry().createQueueDeclareOkBody(AMQShortString.createAMQShortString(cur.getName()), 0,0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                    return;
                } else {
                    if (cur.getDurable() != durable) {
                        throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, "Queue already exist with different durable", true);
                    }
                    if (cur.getAutoDelete() != autoDelete) {
                        throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, "Queue already exist with different autoDelete", true);
                    }
                    if (cur.getExclusive() != exclusive) {
                        throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, "Queue already exist with different exclusive", true);
                    }
                    QueueDeclareOkBody responseBody = connection.getRegistry().createQueueDeclareOkBody(AMQShortString.createAMQShortString(cur.getName()), 0,0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                    return;
                }
            }
            Queue q = queueService.addQueue(
                connection.getVhost(),
                AMQShortString.toString(queue),
                exclusive,
                autoDelete,
                durable,
                FieldTable.convertToMap(arguments)
            );
            log.info("queue declare success: {}", q);
            QueueDeclareOkBody responseBody = connection.getRegistry().createQueueDeclareOkBody(AMQShortString.createAMQShortString(q.getName()), 0,0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, boolean nowait, FieldTable arguments) {
        process(() -> connection.writeFrame(connection.getRegistry().createQueueBindOkBody().generateFrame(channelId)));
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        process(() -> {

        });
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        process(() -> {
            String vhost = connection.getVhost();
            Queue cur = queueService.getQueue(vhost, AMQShortString.toString(queue));
            if (cur != null) {
                queueService.deleteQueue(cur);
                log.info("delete queue {} success", queue);
            }
            connection.writeFrame(connection.getRegistry().createQueueDeleteOkBody(0).generateFrame(channelId));
        });
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, FieldTable arguments) {
        process(() -> {
            final QueueUnbindOkBody responseBody = connection.getRegistry().createQueueUnbindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        process(() -> connection.writeFrame(connection.getRegistry().createBasicRecoverSyncOkBody().generateFrame(channelId)));
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        process(() -> connection.writeFrame(connection.getRegistry().createBasicQosOkBody().generateFrame(channelId)));
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {
        process(() -> {
            final AMQMethodBody responseBody = connection.getRegistry().createBasicConsumeOkBody(
                AMQShortString.createAMQShortString("ctag"));
            connection.writeFrame(responseBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        connection.writeFrame(connection.getRegistry().createBasicCancelOkBody(consumerTag).generateFrame(channelId));
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory, boolean immediate) {
        process(() -> {
            log.debug("Received basic.publish: exchange={}, routingKey={}, mandatory={}, immediate={}", 
                exchange, routingKey, mandatory, immediate);
            // Initialize new incoming message
            currentMessage = new IncomingMessage(exchange, routingKey, mandatory, immediate);
            // Message header and content will be received in subsequent calls
        });
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        connection.writeFrame(connection.getRegistry().createBasicGetEmptyBody(null).generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        connection.writeFrame(connection.getRegistry().createChannelFlowOkBody(true).generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        log.debug("Received channel.flow-ok: active={}", active);
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        log.info("Received channel.close: code={}, text={}, classId={}, methodId={}", 
            replyCode, replyText, classId, methodId);
        // Clear unacknowledged messages
        unacknowledgedMessageMap.clear();
        // Send close-ok response
        connection.writeFrame(connection.getRegistry().createChannelCloseOkBody().generateFrame(channelId));
        // Close the channel
        connection.closeChannel(this);
    }

    @Override
    public void receiveChannelCloseOk() {
        log.info("Received channel.close-ok for channel {}", channelId);
        // Clear unacknowledged messages
        unacknowledgedMessageMap.clear();
        connection.closeChannel(this);
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        process(() -> {
            if (currentMessage == null) {
                log.error("Received message content without basic.publish");
                throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, 
                    "Message content received without basic.publish", false);
            }
            
            // Add content body frame to the current message
            long currentSize = currentMessage.addContentBodyFrame(new ContentBody(data));
            
            // Check if we've received the complete message
            if (currentMessage.isComplete()) {
                publishMessage();
            }
        });
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        process(() -> {
            log.debug("Received message header: bodySize={}", bodySize);
            if (currentMessage == null) {
                log.error("Received message header without basic.publish");
                throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, 
                    "Message header received without basic.publish", false);
            }
            
            // Set the content header body
            currentMessage.setContentHeaderBody(new ContentHeaderBody(properties, bodySize));
            
            // If body size is 0, publish immediately
            if (bodySize == 0) {
                publishMessage();
            }
        });
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        process(() -> {
            log.debug("Received basic.nack: deliveryTag={}, multiple={}, requeue={}", deliveryTag, multiple, requeue);
            int count = unacknowledgedMessageMap.nack(deliveryTag, multiple, requeue);
            log.info("Nacked {} message(s)", count);
        });
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        process(() -> {
            log.debug("Received basic.ack: deliveryTag={}, multiple={}", deliveryTag, multiple);
            int count = unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
            log.info("Acknowledged {} message(s)", count);
        });
    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        process(() -> {
            log.debug("Received basic.reject: deliveryTag={}, requeue={}", deliveryTag, requeue);
            boolean rejected = unacknowledgedMessageMap.reject(deliveryTag, requeue);
            if (rejected) {
                log.info("Rejected message with deliveryTag: {}", deliveryTag);
            }
        });
    }

    @Override
    public void receiveTxSelect() {
        process(() -> {
            log.debug("Received tx.select, enabling transaction mode");
            transactionMode = true;
            connection.writeFrame(connection.getRegistry().createTxSelectOkBody().generateFrame(channelId));
        });
    }

    @Override
    public void receiveTxCommit() {
        process(() -> {
            log.debug("Received tx.commit");
            if (!transactionMode) {
                throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, 
                    "Channel not in transaction mode", false);
            }
            // In a full implementation, this would commit pending messages
            connection.writeFrame(connection.getRegistry().createTxCommitOkBody().generateFrame(channelId));
        });
    }

    @Override
    public void receiveTxRollback() {
        process(() -> {
            log.debug("Received tx.rollback");
            if (!transactionMode) {
                throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, 
                    "Channel not in transaction mode", false);
            }
            // In a full implementation, this would rollback pending messages
            connection.writeFrame(connection.getRegistry().createTxRollbackOkBody().generateFrame(channelId));
        });
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        process(() -> {
            log.warn("Received confirm.select - publisher confirms not fully supported in AMQP 0-9-1");
            // Publisher confirms is an extension to AMQP 0-9-1 and may not be fully supported
            // Throw an exception to indicate this feature is not implemented
            throw new AmqpException(AmqpException.Codes.PRECONDITION_FAILED, 
                "Publisher confirms are not implemented", false);
        });
    }

    public void closeChannel(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }
    
    /**
     * Publishes the accumulated message to the storage layer (draft version)
     */
    private void publishMessage() {
        if (currentMessage == null) {
            log.warn("publishMessage called but no current message");
            return;
        }
        
        try {
            String exchangeName = currentMessage.getExchange() == null ? "" : currentMessage.getExchange().toString();
            String routingKey = currentMessage.getRoutingKey() == null ? "" : currentMessage.getRoutingKey().toString();
            long bodySize = currentMessage.getExpectedBodySize();
            
            log.info("Publishing message: exchange={}, routingKey={}, bodySize={}", 
                exchangeName, routingKey, bodySize);
            
            // Step 1: Get the exchange (handle default exchange case)
            Exchange exchange = null;
            if (!exchangeName.isEmpty()) {
                exchange = exchangeService.getExchange(connection.getVhost(), exchangeName);
                if (exchange == null) {
                    log.error("Exchange not found: {}", exchangeName);
                    throw new AmqpException(AmqpException.Codes.NOT_FOUND, 
                        "Exchange not found: " + exchangeName, false);
                }
            }
            
            // Step 2: Get bindings for this exchange
            List<Binding> bindings = bindingService.listBindings(connection.getVhost(), exchangeName);
            
            // Step 3: Route message to matching queues based on bindings and routing key
            for (Binding binding : bindings) {
                if (matchesRoutingKey(binding, routingKey, exchange)) {
                    Queue queue = queueService.getQueue(connection.getVhost(), binding.getDestination());
                    if (queue == null) {
                        log.warn("Queue not found: {}", binding.getDestination());
                        continue;
                    }
                    
                    // Step 4: Store message to Kafka (interface call - implementation left unfinished)
                    storeMessageToQueue(queue, currentMessage);
                    
                    log.debug("Message routed to queue: {}", queue.getName());
                }
            }
            
            log.debug("Message published successfully");
            
        } catch (Exception e) {
            log.error("Error publishing message", e);
            throw new AmqpException(AmqpException.Codes.INTERNAL_ERROR, 
                "Failed to publish message: " + e.getMessage(), false);
        } finally {
            // Clear message state
            if (currentMessage != null) {
                currentMessage.dispose();
                currentMessage = null;
            }
        }
    }
    
    /**
     * Matches the routing key against the binding based on exchange type
     * 
     * @param binding the binding to check
     * @param routingKey the routing key from the message
     * @param exchange the exchange (can be null for default exchange)
     * @return true if the routing key matches the binding
     */
    private boolean matchesRoutingKey(Binding binding, String routingKey, Exchange exchange) {
        if (exchange == null) {
            // Default exchange: route directly to queue with name matching routing key
            return binding.getDestination().equals(routingKey);
        }
        
        ExchangeType exchangeType = exchange.getType();
        String bindingKey = binding.getRoutingKey();
        
        switch (exchangeType) {
            case Direct:
                // Direct exchange: exact match
                return routingKey.equals(bindingKey);
                
            case Fanout:
                // Fanout exchange: always match (ignore routing key)
                return true;
                
            case Topic:
                // Topic exchange: pattern matching with wildcards
                return matchesTopicPattern(routingKey, bindingKey);
                
            case Headers:
                // Headers exchange: not implemented in this draft
                log.warn("Headers exchange not implemented");
                return false;
                
            default:
                return false;
        }
    }
    
    /**
     * Matches a routing key against a topic pattern with wildcards
     * * (star) can substitute for exactly one word
     * # (hash) can substitute for zero or more words
     * 
     * @param routingKey the routing key to match
     * @param pattern the binding pattern
     * @return true if the routing key matches the pattern
     */
    private boolean matchesTopicPattern(String routingKey, String pattern) {
        String[] routingParts = routingKey.split("\\.");
        String[] patternParts = pattern.split("\\.");
        
        return matchesTopicPatternRecursive(routingParts, 0, patternParts, 0);
    }
    
    private boolean matchesTopicPatternRecursive(String[] routingParts, int routingIndex, 
                                                  String[] patternParts, int patternIndex) {
        // If we've consumed both patterns, it's a match
        if (routingIndex >= routingParts.length && patternIndex >= patternParts.length) {
            return true;
        }
        
        // If pattern is exhausted but routing key isn't, no match (unless last pattern was #)
        if (patternIndex >= patternParts.length) {
            return false;
        }
        
        String patternPart = patternParts[patternIndex];
        
        // Handle # (zero or more words)
        if ("#".equals(patternPart)) {
            // # at the end matches everything
            if (patternIndex == patternParts.length - 1) {
                return true;
            }
            // Try matching zero words (skip #) or one+ words
            if (matchesTopicPatternRecursive(routingParts, routingIndex, patternParts, patternIndex + 1)) {
                return true;
            }
            if (routingIndex < routingParts.length) {
                return matchesTopicPatternRecursive(routingParts, routingIndex + 1, patternParts, patternIndex);
            }
            return false;
        }
        
        // If routing key is exhausted but pattern isn't, no match
        if (routingIndex >= routingParts.length) {
            return false;
        }
        
        // Handle * (exactly one word) or exact match
        if ("*".equals(patternPart) || patternPart.equals(routingParts[routingIndex])) {
            return matchesTopicPatternRecursive(routingParts, routingIndex + 1, patternParts, patternIndex + 1);
        }
        
        return false;
    }
    
    /**
     * Stores the message to a queue via Kafka
     * Sends message to Kafka where 1 queue shares the same Kafka producer.
     * Messages are consumed using Kafka Share Consumer (1 AMQP consumer).
     * 
     * @param queue the target queue
     * @param incomingMessage the message to store
     */
    private void storeMessageToQueue(Queue queue, IncomingMessage incomingMessage) {
        try {
            // Create message object for storage
            Message message = new Message();
            
            // Set routing information
            message.setVhost(connection.getVhost());
            message.setQueue(queue.getName());
            message.setExchange(incomingMessage.getExchange() != null ? incomingMessage.getExchange().toString() : "");
            message.setRoutingKey(incomingMessage.getRoutingKey() != null ? incomingMessage.getRoutingKey().toString() : "");
            
            // Set message flags
            message.setMandatory(incomingMessage.isMandatory());
            message.setImmediate(incomingMessage.isImmediate());
            
            // Set message body
            QpidByteBuffer bodyBuffer = incomingMessage.getBodyBuffer();
            if (bodyBuffer != null && bodyBuffer.hasRemaining()) {
                byte[] bodyBytes = new byte[bodyBuffer.remaining()];
                bodyBuffer.get(bodyBytes);
                message.setBody(bodyBytes);
                message.setBodySize(bodyBytes.length);
                // Reset buffer position for potential reuse
                bodyBuffer.position(bodyBuffer.position() - bodyBytes.length);
            } else {
                message.setBody(new byte[0]);
                message.setBodySize(0);
            }
            
            // Set message properties from AMQP BasicContentHeaderProperties
            BasicContentHeaderProperties properties = incomingMessage.getProperties();
            if (properties != null) {
                message.setContentType(properties.getContentTypeAsString());
                message.setContentEncoding(properties.getEncodingAsString());
                message.setDeliveryMode((int) properties.getDeliveryMode());
                message.setPriority((int) properties.getPriority());
                message.setCorrelationId(properties.getCorrelationIdAsString());
                message.setReplyTo(properties.getReplyToAsString());
                // Expiration is stored as long (milliseconds) in AMQP 0-8
                long expiration = properties.getExpiration();
                message.setExpiration(expiration > 0 ? String.valueOf(expiration) : null);
                message.setMessageId(properties.getMessageIdAsString());
                message.setType(properties.getTypeAsString());
                message.setUserId(properties.getUserIdAsString());
                message.setAppId(properties.getAppIdAsString());
                
                // Set timestamp
                if (properties.getTimestamp() > 0) {
                    message.setTimestamp(properties.getTimestamp());
                } else {
                    message.setTimestamp(System.currentTimeMillis());
                }
                
                // Set headers - BasicContentHeaderProperties doesn't have headers in AMQP 0-8/0-9
                // Headers are typically stored in a separate headers exchange
                // For now, initialize empty map
                message.setHeaders(new java.util.HashMap<>());
            } else {
                // Set default timestamp if no properties
                message.setTimestamp(System.currentTimeMillis());
            }
            
            // Store to Kafka using the producer service
            // The ProduceService will use Kafka producer to send the message
            // Each queue has its own Kafka topic
            // Kafka Share Groups will be used for consumption (KIP-932)
            connection.getStorage().produce(message);
            
            log.debug("Message stored to Kafka for queue: {} with body size: {} bytes", 
                queue.getName(), message.getBodySize());
        } catch (Exception e) {
            log.error("Failed to store message to Kafka for queue: {}", queue.getName(), e);
            throw new AmqpException(AmqpException.Codes.INTERNAL_ERROR, 
                "Failed to store message: " + e.getMessage(), false);
        }
    }
}
