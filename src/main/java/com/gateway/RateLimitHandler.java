package com.gateway;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import reactor.core.scheduler.Schedulers;

public class RateLimitHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final RateLimiter rateLimiter;

    public RateLimitHandler(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {

        String clientId = fullHttpRequest.headers().get("User-Agent", "unknown");

        rateLimiter.isAllowed(clientId)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(allowed -> {
                   if(allowed) {
                       channelHandlerContext.fireChannelRead(fullHttpRequest.retain());
                   } else {
                        sendErrorResponse(channelHandlerContext);
                   }
                });
    }

    private void sendErrorResponse(ChannelHandlerContext channelHandlerContext) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.TOO_MANY_REQUESTS,
                channelHandlerContext.alloc().buffer().writeBytes("Rate limit exceeded!".getBytes())
        );

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content());

        channelHandlerContext.writeAndFlush(response);
    }
}
