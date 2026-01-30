package com.gateway;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.nio.file.Files;
import java.nio.file.Path;

public class GatewayServer {

    private final int port;

    public GatewayServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        String luaScript = Files.readString(Path.of(getClass().getResource("/request_rate_limiter.lua").toURI()));
        RateLimiter rateLimiter = new RateLimiter(connection, luaScript);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // type of channel to handle the connections
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel channel) {
                            ChannelPipeline channelPipeline = channel.pipeline();
                            channelPipeline.addLast(new HttpServerCodec());
                            channelPipeline.addLast(new HttpObjectAggregator(65536));
                            channelPipeline.addLast(new RateLimitHandler(rateLimiter));
                        }
                    });

            System.out.println("Server initiated on port: " + port);
            ChannelFuture channelFuture = bootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();

        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            connection.close();
            redisClient.shutdown();
        }
    }
}
