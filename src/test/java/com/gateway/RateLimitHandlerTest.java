package com.gateway;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class RateLimitHandlerTest {

    @Test
    void shouldAllowRequestWhenRateLimiterReturnsTrue() throws InterruptedException {
        RateLimiter mockLimiter = Mockito.mock(RateLimiter.class);

        when(mockLimiter.isAllowed(anyString())).thenReturn(Mono.just(true));

        RateLimitHandler handler = new RateLimitHandler(mockLimiter, Schedulers.immediate());
        EmbeddedChannel channel = new EmbeddedChannel(
                handler,
                new ChannelInboundHandlerAdapter()
        );

        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");

        // Act
        channel.writeInbound(request);

        Object forwarded = channel.readInbound();
        assertNotNull(forwarded, "The request was never forwarded to the next handler.");
        assertTrue(forwarded instanceof FullHttpRequest);
        System.out.println(forwarded instanceof FullHttpRequest);
    }

    @Test
    void shouldBlockRequestWhenRateLimiterReturnsFalse()
    {
        RateLimiter mockLimiter = Mockito.mock(RateLimiter.class);
        when(mockLimiter.isAllowed(anyString())).thenReturn(Mono.just(false));

        RateLimitHandler handler = new RateLimitHandler(mockLimiter, Schedulers.immediate());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        FullHttpRequest fullHttpRequest = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"
        );

        channel.writeInbound(fullHttpRequest);

        Object forwarded = channel.readInbound();
        assertNull(forwarded, "Request should NOT be passed forward when rate limited had been reached");

        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response, "A response should have been sent back");
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
    }
}
