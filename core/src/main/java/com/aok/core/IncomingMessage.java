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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;

/**
 * Handles the assembly of an incoming AMQP message from multiple frames.
 * An AMQP message consists of:
 * 1. Basic.Publish method frame (contains exchange and routing key)
 * 2. Content header frame (contains message properties and body size)
 * 3. One or more content body frames (contains actual message data)
 */
@Slf4j
@Getter
public class IncomingMessage {
    
    private final AMQShortString exchange;
    private final AMQShortString routingKey;
    private final boolean mandatory;
    private final boolean immediate;
    
    private ContentHeaderBody contentHeaderBody;
    private QpidByteBuffer bodyBuffer;
    private long receivedBodySize = 0;
    
    public IncomingMessage(AMQShortString exchange, AMQShortString routingKey, 
                          boolean mandatory, boolean immediate) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.mandatory = mandatory;
        this.immediate = immediate;
    }
    
    /**
     * Sets the content header containing message properties and expected body size
     */
    public void setContentHeaderBody(ContentHeaderBody contentHeaderBody) {
        this.contentHeaderBody = contentHeaderBody;
    }
    
    /**
     * Adds a content body frame to the message and returns the current total size
     * 
     * @param contentBody the content body frame to add
     * @return the current total size of received body data
     */
    public long addContentBodyFrame(ContentBody contentBody) {
        QpidByteBuffer payload = contentBody.getPayload();
        
        if (bodyBuffer == null) {
            bodyBuffer = payload;
        } else {
            // Concatenate buffers, disposing the original
            QpidByteBuffer original = bodyBuffer;
            bodyBuffer = QpidByteBuffer.concatenate(original, payload);
            original.dispose();
        }
        
        receivedBodySize += payload.remaining();
        return receivedBodySize;
    }
    
    /**
     * Checks if the complete message has been received
     */
    public boolean isComplete() {
        if (contentHeaderBody == null) {
            return false;
        }
        long expectedSize = contentHeaderBody.getBodySize();
        return receivedBodySize >= expectedSize;
    }
    
    /**
     * Gets the expected total body size from the content header
     */
    public long getExpectedBodySize() {
        return contentHeaderBody != null ? contentHeaderBody.getBodySize() : 0;
    }
    
    /**
     * Gets the message properties from the content header
     */
    public BasicContentHeaderProperties getProperties() {
        return contentHeaderBody != null ? contentHeaderBody.getProperties() : null;
    }
    
    /**
     * Disposes of the body buffer to free memory
     */
    public void dispose() {
        if (bodyBuffer != null) {
            bodyBuffer.dispose();
            bodyBuffer = null;
        }
    }
}
