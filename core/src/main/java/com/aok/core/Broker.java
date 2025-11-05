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

import com.aok.core.config.AmqpConfig;
import com.aok.core.storage.ConsumeService;
import com.aok.core.storage.IStorage;
import com.aok.core.storage.KafkaConsumeService;
import com.aok.core.storage.KafkaMessageStorage;
import com.aok.core.storage.ProduceService;
import com.aok.core.storage.ProducerPool;
import com.aok.meta.container.KafkaMetaContainer;
import com.aok.meta.container.MetaContainer;
import com.aok.meta.service.BindingService;
import com.aok.meta.service.ExchangeService;
import com.aok.meta.service.QueueService;
import com.aok.meta.service.VhostService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Broker {

    private AmqpConfig amqpConfig;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
    
    public void startUp(String []args) throws IOException {
        OptionParser optionParser = new OptionParser();
        ConfigLoader loader = new ConfigLoader();
        if (args.length < 1) {
            CommandLineUtils.printUsageAndExit(optionParser, "No configuration file specified");
        }
        Properties properties = loader.load(args[0]);
        this.amqpConfig = AmqpConfig.fromProperties(properties);
        if (args.length > 1) {
            OptionSet options = optionParser.parse(args);
            optionParser.parse(Arrays.copyOfRange(args, 1, args.length));
            if (!options.nonOptionArguments().isEmpty()) {
                String nonArgs = options.nonOptionArguments().stream()
                    .map(Object::toString)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
                CommandLineUtils.printUsageAndExit(optionParser, "Found non argument parameters: " + nonArgs);
            }
            //TODO override properties with command line args
        }
        start();
    }

    private void start() {
        EventLoopGroup eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        EventLoopGroup eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("NettyServerNIOSelector_"));
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        IStorage storage = new KafkaMessageStorage(new ProducerPool());
        ProduceService produceService = new ProduceService(storage);
        
        // Create consumer container for statistics and management
        ConsumerContainer consumerContainer = new ConsumerContainer();
        
        // Create Kafka consumer configuration for ConsumeService
        Properties kafkaConsumerConfig = new Properties();
        kafkaConsumerConfig.put("bootstrap.servers", this.amqpConfig.getKafkaBootstrapServers());
        KafkaConsumeService consumeService = new KafkaConsumeService(kafkaConsumerConfig, consumerContainer);
        
        // Start the consume service polling thread
        consumeService.start();
        
        MetaContainer metaContainer = new KafkaMetaContainer(this.amqpConfig.getKafkaBootstrapServers());
        metaContainer.start();
        VhostService vhostService = new VhostService(metaContainer);
        QueueService queueService = new QueueService(metaContainer);
        ExchangeService exchangeService = new ExchangeService(metaContainer);
        BindingService bindingService = new BindingService(metaContainer);
        // create default vhost
        vhostService.addVhost("/");
        serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupSelector)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(amqpConfig.getServerHost(), amqpConfig.getServerPort()))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    final ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast("encoder", new AmqpEncoder());
                    pipeline.addLast("handler", new AmqpConnection(vhostService, exchangeService, queueService, bindingService, produceService, consumeService, consumerContainer));
                }
            });
        try {
            serverBootstrap.bind().sync();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        log.info("Broker start up success, listening on port {}", amqpConfig.getServerPort());
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Broker broker = new Broker();
        broker.startUp(args);
        broker.awaitShutdown();
    }
}
