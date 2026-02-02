##  Reactive API Gateway
Building a high-performance, non-blocking entry point for microservices that
protects backend resources using a distributed Rate Limiter.

## Architecture
- Netty: The event-driven networking engine that handles connections without
  blocking threads.
- Project Reactor: The library providing Mono/Flux to handle the "wait" for
  Redis results asynchronously.
- Lettuce: A thread-safe, reactive Redis client.


## Verification
Unit Testing: Used `EmbeddedChannel` to simulate a pipeline.

Sync Testing: Injected `Schedulers.immediate()` into the handler during
  tests to avoid "null" results from async race conditions.