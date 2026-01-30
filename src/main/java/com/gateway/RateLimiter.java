package com.gateway;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import reactor.core.publisher.Mono;

import java.time.Instant;

public class RateLimiter {
    private final StatefulRedisConnection<String, String> connection;
    private final String luaScript;

    public RateLimiter(StatefulRedisConnection<String, String> connection, String luaScript)
    {
        this.connection = connection;
        this.luaScript = luaScript;
    }

    public Mono<Boolean> isAllowed(String clientId) {
        String capacity = "10";
        String refillRate = "1";
        String now = String.valueOf(Instant.now().getEpochSecond());

        return Mono.fromDirect(connection.reactive().eval(
                luaScript,
                ScriptOutputType.INTEGER,
                new String[]{clientId},
                capacity, refillRate, now
        )).map(result -> result.equals(1L));
    }
}