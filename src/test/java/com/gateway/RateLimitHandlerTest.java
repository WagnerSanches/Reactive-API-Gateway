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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class RateLimitHandlerTest {

    @Test
    void shouldAllowRequestWhenRateLimiterReturnsTrue() throws InterruptedException {
        // Arrange
        RateLimiter mockLimiter = Mockito.mock(RateLimiter.class);
        // Use Mono.just(true) so the decision is ready immediately
        when(mockLimiter.isAllowed(anyString())).thenReturn(Mono.just(true));

        RateLimitHandler handler = new RateLimitHandler(mockLimiter);
        EmbeddedChannel channel = new EmbeddedChannel(
                handler,
                new ChannelInboundHandlerAdapter() // Acts as the 'Backend' storage
        );

        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");

        // Act
        channel.writeInbound(request);

        // Wait up to 2 seconds for the async task to complete
        long start = System.currentTimeMillis();
        while (channel.readInbound() == null && System.currentTimeMillis() - start < 2000) {
            channel.runPendingTasks(); // Process tasks in the channel's event loop
            Thread.sleep(10); // Briefly yield the test thread
        }

        // Re-read after the loop
        // Note: channel.readInbound() consumes the message, so we check the result of the loop
        // If it's still null, we check the channel again one last time
        Object forwarded = channel.readInbound();

        // Assert
        assertNotNull(forwarded, "The request was never forwarded to the next handler.");

        // Clean up
        if (forwarded != null) {
            io.netty.util.ReferenceCountUtil.release(forwarded);
        }
    }

    @Test
    void shouldBlockRequestWhenRateLimiterReturnsFalse()
    {

    }
}
