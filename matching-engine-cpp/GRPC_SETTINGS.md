# gRPC Server Settings

The Matching Engine gRPC server exposes the core engine asynchronously via the `grpc::CallbackService` API. To extract maximum throughput and minimize tail latency, the server should be configured with the following parameters:

## 1. Max Concurrent Streams
- **Parameter**: `grpc::MaxConcurrentStreams(int)`
- **Recommended Value**: `1024` or higher (depending on expected client fan-out)
- **Justification**: A low value can cause clients to block while waiting for streams to open. Since the matching engine handles ingestion purely asynchronously via the `MpscQueue`, it can easily tolerate a high number of concurrent streams without consuming OS threads.

## 2. Keepalive Settings
- **Parameters**: 
  - `grpc::KeepAliveTime(milliseconds)`: e.g., `10000` (10s)
  - `grpc::KeepAliveTimeout(milliseconds)`: e.g., `5000` (5s)
  - `grpc::KeepAlivePermitWithoutCalls(bool)`: `true`
- **Justification**: Maintaining persistent connections is vital for a high-throughput trading system to avoid the TCP handshake and TLS overhead on the critical path. Dropped connections should be pruned aggressively (hence the 5s timeout) to prevent memory leaks and zombie streams.

## 3. Max Message Size
- **Parameter**: `grpc::MaxReceiveMessageSize(int)`, `grpc::MaxSendMessageSize(int)`
- **Recommended Value**: `4194304` (4 MB) is usually the default and perfectly adequate.
- **Justification**: Trading commands (`SubmitOrderRequest`) are exceptionally small (< 100 bytes). A 4MB limit provides plenty of headroom without exposing the system to memory exhaustion via massive arbitrary payloads.

## 4. Thread Counts for Completion Queues (CQs) and Reply Threads
- **CQ Thread Count**: By default, gRPC spins up one Completion Queue thread per CPU core. For the matching engine, this is optimal because ingress parsing happens on the CQ threads before hitting the lock-free `MpscQueue`.
  - *Recommendation*: Use default gRPC thread counts (typically `std::thread::hardware_concurrency()`) for the ingestion path.
- **Reply Thread Count**: The `MatchingService` owns its own `ReplyLoop` thread which reads from the `reply_ring_` and calls `reactor->Finish()`.
  - *Recommendation*: Start with **1 dedicated Reply Thread per Shard**. If the network layer forms a bottleneck, this can be scaled to a small thread pool, but maintaining SPSC (Single-Producer Single-Consumer) invariants between the Shard and the Reply Thread is necessary for zero-allocation speed.
