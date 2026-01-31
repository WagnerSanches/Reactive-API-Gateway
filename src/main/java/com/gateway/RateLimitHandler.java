package com.gateway;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

public class RateLimitHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final RateLimiter rateLimiter;
    private final Scheduler scheduler;

    public RateLimitHandler(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
        this.scheduler = Schedulers.boundedElastic();
    }

    public RateLimitHandler(RateLimiter rateLimiter, Scheduler scheduler) {
        this.rateLimiter = rateLimiter;
        this.scheduler = scheduler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
        FullHttpRequest retainedRequest = fullHttpRequest.retain(); // keeping alive

        String clientId = fullHttpRequest.headers().get("User-Agent", "unknown");

        rateLimiter.isAllowed(clientId)
                .subscribeOn(scheduler)
                .subscribe(allowed -> {
                    try {
                        if(allowed) {
                            channelHandlerContext.fireChannelRead(retainedRequest);
                        } else {
                            sendErrorResponse(channelHandlerContext);
                        }
                    } finally {
                        ReferenceCountUtil.release(retainedRequest);
                    }
                }, error -> {
                    ReferenceCountUtil.release(retainedRequest);
                    channelHandlerContext.close();
                });
    }

    private void sendErrorResponse(ChannelHandlerContext channelHandlerContext) {
        byte[] body = "Rate limit exceeded!".getBytes(StandardCharsets.UTF_8);

        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.TOO_MANY_REQUESTS,
                channelHandlerContext.alloc().buffer().writeBytes(body)
        );

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length));

        channelHandlerContext.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
