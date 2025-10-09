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

import com.aok.core.storage.ProduceService;
import com.aok.meta.service.BindingService;
import com.aok.meta.service.ExchangeService;
import com.aok.meta.service.QueueService;
import com.aok.meta.service.VhostService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolHeaderException;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.US_ASCII;

@Slf4j
public class AmqpConnection extends AmqpCommandDecoder implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    @Getter
    private String vhost;

    private final VhostService vhostService;

    private final ExchangeService exchangeService;

    private final QueueService queueService;

    private final BindingService bindingService;

    private AmqpBrokerDecoder brokerDecoder;
    
    private AmqpChannel amqpChannel;

    private final ProduceService storage;
    
    private final ConcurrentHashMap<Integer, AmqpChannel> channels = new ConcurrentHashMap<>();

    @Getter
    private final MethodRegistry registry = new MethodRegistry(AmqpConstants.PROTOCOL_VERSION);

    private volatile int currentClassId;

    private volatile int currentMethodId;

    AmqpConnection(VhostService vhostService, ExchangeService exchangeService, QueueService queueService, BindingService bindingService, ProduceService produceService) {
        this.vhostService = vhostService;
        this.exchangeService = exchangeService;
        this.queueService = queueService;
        this.bindingService = bindingService;
        this.storage = produceService;
    }

    @Getter
    private ChannelHandlerContext ctx;
    
    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response, AMQShortString locale) {
        // unfinished
        ConnectionTuneBody tuneBody = registry.createConnectionTuneBody(200, 100000L, 100);
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        // unfinished
        ConnectionTuneBody tuneBody = registry.createConnectionTuneBody(200, 100000L, 100);
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {

    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        this.vhost = AMQShortString.toString(virtualHost);
        AMQMethodBody responseBody = registry.createConnectionOpenOkBody(virtualHost);
        writeFrame(responseBody.generateFrame(0));
    }

    @Override
    public void receiveChannelOpen(int channelId) {
        ChannelOpenOkBody response = registry.createChannelOpenOkBody();
        addChannel(new AmqpChannel(this, channelId, vhostService, exchangeService, queueService, bindingService));
        writeFrame(response.generateFrame(channelId));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return AmqpConstants.PROTOCOL_VERSION;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        return getChannel(channelId);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {

    }

    @Override
    public void receiveConnectionCloseOk() {
        
    }

    @Override
    public void receiveHeartbeat() {

    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        ProtocolVersion protocolVersion = AmqpConstants.PROTOCOL_VERSION;
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion();
            AMQMethodBody responseBody = registry.createConnectionStartBody(
                protocolVersion.getMajorVersion(),
                pv.getActualMinorVersion(),
                null,
                "PLAIN token".getBytes(US_ASCII),
                "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (AMQProtocolHeaderException e) {
            log.info("Received unsupported protocol initiation for protocol version: {}, {}", getProtocolVersion(), e.getMessage());
            writeFrame(new ProtocolInitiation(ProtocolVersion.v0_91));
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            log.info("Received unsupported protocol initiation for protocol version: {}, {}", getProtocolVersion(), e.getMessage());
            writeFrame(new ProtocolInitiation(ProtocolVersion.v0_91));
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        this.currentClassId = classId;
        this.currentMethodId = methodId;
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.brokerDecoder = new AmqpBrokerDecoder(this);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws AMQFrameDecodingException, IOException {
        ByteBuf buffer = (ByteBuf) msg;
        brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
    }

    public void writeFrame(AMQDataBlock frame) {
        final ChannelFuture future = getCtx().writeAndFlush(frame);
        future.addListener(f -> {});
    }

    private void addChannel(AmqpChannel channel) {
        this.channels.put(channel.getChannelId(), channel);
    }

    public AmqpChannel getChannel(int channelId) {
        return channels.get(channelId);
    }

    public void sendConnectionClose(int errorCode, String message, int channelId) {
        sendConnectionClose(channelId, new AMQFrame(0, new ConnectionCloseBody(getProtocolVersion(),
            errorCode, AMQShortString.validValueOf(message), currentClassId, currentMethodId)));
    }

    private void sendConnectionClose(int channelId, AMQFrame frame) {
        writeFrame(frame);
    }

    public void closeChannelAndWriteFrame(AmqpChannel channel, int cause, String message) {
        closeChannel(channel);
        writeFrame(new AMQFrame(channel.getChannelId(),
            registry.createChannelCloseBody(cause,
                AMQShortString.validValueOf(message),
                currentClassId,
                currentMethodId)));
    }

    protected void closeChannel(AmqpChannel channel) {

    }

}
