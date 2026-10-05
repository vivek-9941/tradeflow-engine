#pragma once

#include "engine/order_book.h"
#include "engine/mpsc_queue.h"
#include "engine/spsc_ring.h"
#include "engine/runtime_cmd.h"
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <string>

namespace engine {

enum class WaitStrategy {
    BUSY_SPIN,
    YIELD,
    HYBRID // spin N, yield M, then sleep ~50us
};

struct ShardConfig {
    uint32_t shard_id;
    WaitStrategy wait_strategy = WaitStrategy::HYBRID;
    int core_pin = -1; // -1 means no pinning
    uint32_t batch_size = 256;
};

// POD for pushing level snapshots
struct LevelSnapshot {
    Price price;
    Qty qty;
    uint32_t order_count;
};

struct DepthSnapshot {
    uint32_t symbol_idx;
    uint64_t ts_ns;
    uint32_t bid_count;
    uint32_t ask_count;
    LevelSnapshot bids[10];
    LevelSnapshot asks[10];
};

struct RuntimeEvent {
    enum Type { TRADE, CANCEL } type;
    union {
        Fill fill;
        struct {
            char order_id[48];
            char user_id[32];
            uint64_t ts_ns;
            Qty remaining_qty;
        } cancel;
    };
};

class Shard {
public:
    Shard(const ShardConfig& config);
    ~Shard();

    void AddSymbol(uint32_t symbol_idx, const OrderBookConfig& ob_config);
    
    // Starts the shard loop and background threads (snapshot)
    void Start();
    
    // Gracefully stops and joins
    void Stop();

    // Called by gRPC threads
    bool TryEnqueueCmd(const RuntimeCmd& cmd);

    // Snapshot accessor for REST/gRPC
    DepthSnapshot GetDepth(uint32_t symbol_idx);

    // Exposed queues for reply threads / event handlers (simple pointers for demo)
    SpscRing<RuntimeReply, 8192>& GetReplyRing() { return reply_ring_; }
    SpscRing<RuntimeEvent, 65536>& GetEventRing() { return event_ring_; }

private:
    void ShardLoop();
    void SnapshotLoop();
    
    void ProcessCommand(const RuntimeCmd& cmd, uint64_t engine_ts);
    void EmitSnapshot(uint32_t symbol_idx, uint64_t engine_ts);
    void ApplyWaitStrategy(uint32_t iter_idle);
    void PinThread(int core_id, const std::string& name);

    class EgressSink : public EventSink {
    public:
        explicit EgressSink(SpscRing<RuntimeEvent, 65536>& ring) : ring_(ring) {}
        void OnTrade(const Fill& fill) override;
        void OnCancel(std::string_view orderId, std::string_view userId, uint64_t ts_ns, Qty remaining_qty) override;
    private:
        SpscRing<RuntimeEvent, 65536>& ring_;
    };

    ShardConfig config_;
    std::atomic<bool> running_{false};
    std::thread shard_thread_;
    std::thread snapshot_thread_;

    uint64_t monotonic_seq_ = 0;

    std::vector<std::unique_ptr<OrderBook>> books_;
    
    // MPSC for ingress
    MpscQueue<RuntimeCmd, 8192> ingress_queue_;

    // SPSC for egress
    SpscRing<RuntimeReply, 8192> reply_ring_;
    SpscRing<RuntimeEvent, 65536> event_ring_;
    SpscRing<DepthSnapshot, 1024> snapshot_ring_;
    
    // Snapshot storage (Mutex protected for readers)
    std::mutex snapshot_mutex_;
    std::vector<DepthSnapshot> latest_snapshots_;
};

} // namespace engine
