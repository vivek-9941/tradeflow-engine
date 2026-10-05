# C++ Matching Engine Design

This document outlines the core architecture and design choices for the high-performance C++ matching engine, which replaces the legacy Java-based engine.

## 1. Threading and Sharding
- **Symbol Sharding**: The matching engine is sharded by symbol. Each symbol (e.g., INFY, TCS) has exactly one dedicated writer thread.
- **Lock-Free Communication**: Communication between the networking layer (gRPC/Kafka) and the engine threads uses lock-free queues (e.g., Single-Producer Single-Consumer or Multi-Producer Single-Consumer queues).
- **Bounded Queues & Backpressure**: All queues are bounded. If a queue fills up due to overload, the system will explicitly reject new orders rather than growing unbounded and causing latency spikes or OOMs.

## 2. Order Book Architecture
- **Integer Math**: Prices are represented as integers (in paise) and quantities as whole-share integers. This completely eliminates floating-point math on the hot path and removes the need for `EPSILON` comparisons.
- **Flat Array for Price Levels**: Instead of a tree map (like Java's `ConcurrentSkipListMap`), price levels are stored in a pre-allocated flat array. A bitmap (or similar bitset) is used to track the presence of active price levels to find the best bid/ask in `O(1)` time using hardware instructions like `ffs`/`ctz`.
- **Intrusive Linked Lists**: Orders at a given price level are linked together using an intrusive linked list.
- **Memory Pooling**: Orders are allocated from a pre-allocated object pool at startup. There is **zero heap allocation** on the hot path.

## 3. Communication and Integration
- **gRPC replaces REST**: The communication between `order-service` and the matching engine will switch from HTTP/REST to gRPC for lower latency and better binary serialization.
- **Idempotency**: The engine is idempotent on `orderId`. This means that if `order-service` does not receive a response (e.g., due to a network timeout) and retries the same order, the engine will safely ignore the duplicate or return the previous result.

## 4. Deterministic Trade IDs
- Trade IDs are generated deterministically using the format `TRD-<shard>-<seq>` (e.g., `TRD-INFY-000001`).
- Determinism ensures that if the engine is restarted and replays a sequence of events, it generates the exact same Trade IDs.
- Downstream consumers (ledger-service, risk-service) are already idempotent on `tradeId` and will safely deduplicate these replayed events.

## 5. Rollout Strategy
- **Feature Flag**: The existing Java engine will remain in the codebase, hidden behind a feature flag.
- **Parity Testing**: The Java engine serves as the source of truth for parity testing to verify the C++ engine's correctness.
- **Rollback**: If the C++ engine encounters critical issues in production, the system can instantly roll back to the Java engine via the feature flag.
