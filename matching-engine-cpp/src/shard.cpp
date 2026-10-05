#include "engine/shard.h"
#include <chrono>
#include <iostream>
#include <thread>
#include <cstring>
#include <algorithm>

#if defined(__linux__)
#include <pthread.h>
#endif

namespace engine {

inline uint64_t GetMonotonicTimeNs() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

void Shard::EgressSink::OnTrade(const Fill& fill) {
    RuntimeEvent ev;
    ev.type = RuntimeEvent::TRADE;
    ev.fill = fill;
    // For simplicity, spinning on full event ring. 
    // In production, backpressure or a larger ring is needed.
    while (!ring_.try_push(ev)) {
        // Spin
    }
}

void Shard::EgressSink::OnCancel(std::string_view orderId, std::string_view userId, uint64_t ts_ns, Qty remaining_qty) {
    RuntimeEvent ev;
    ev.type = RuntimeEvent::CANCEL;
    size_t oid_len = std::min(orderId.size(), sizeof(ev.cancel.order_id) - 1);
    std::memcpy(ev.cancel.order_id, orderId.data(), oid_len);
    ev.cancel.order_id[oid_len] = '\0';
    
    size_t uid_len = std::min(userId.size(), sizeof(ev.cancel.user_id) - 1);
    std::memcpy(ev.cancel.user_id, userId.data(), uid_len);
    ev.cancel.user_id[uid_len] = '\0';

    ev.cancel.ts_ns = ts_ns;
    ev.cancel.remaining_qty = remaining_qty;

    while (!ring_.try_push(ev)) {
        // Spin
    }
}

Shard::Shard(const ShardConfig& config)
    : config_(config) {
}

Shard::~Shard() {
    Stop();
}

void Shard::AddSymbol(uint32_t symbol_idx, const OrderBookConfig& ob_config) {
    if (symbol_idx >= books_.size()) {
        books_.resize(symbol_idx + 1);
        latest_snapshots_.resize(symbol_idx + 1);
    }
    books_[symbol_idx] = std::make_unique<OrderBook>(ob_config);
    latest_snapshots_[symbol_idx].symbol_idx = symbol_idx;
}

void Shard::Start() {
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true)) {
        shard_thread_ = std::thread(&Shard::ShardLoop, this);
        snapshot_thread_ = std::thread(&Shard::SnapshotLoop, this);
    }
}

void Shard::Stop() {
    if (running_.exchange(false)) {
        if (shard_thread_.joinable()) shard_thread_.join();
        if (snapshot_thread_.joinable()) snapshot_thread_.join();
    }
}

void Shard::PinThread(int core_id, const std::string& name) {
#if defined(__linux__)
    if (core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
#endif
}

bool Shard::TryEnqueueCmd(const RuntimeCmd& cmd) {
    return ingress_queue_.try_push(cmd);
}

DepthSnapshot Shard::GetDepth(uint32_t symbol_idx) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    if (symbol_idx < latest_snapshots_.size()) {
        return latest_snapshots_[symbol_idx];
    }
    return DepthSnapshot{};
}

void Shard::ApplyWaitStrategy(uint32_t iter_idle) {
    if (config_.wait_strategy == WaitStrategy::BUSY_SPIN) {
        // Just spin
    } else if (config_.wait_strategy == WaitStrategy::YIELD) {
        std::this_thread::yield();
    } else { // HYBRID
        if (iter_idle < 100) {
            // spin
        } else if (iter_idle < 1000) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
}

void Shard::ShardLoop() {
    PinThread(config_.core_pin, "shard_" + std::to_string(config_.shard_id));
    
    EgressSink sink(event_ring_);
    uint32_t idle_iters = 0;
    
    // rate limit snapshots to every ~50ms
    uint64_t last_snapshot_time = GetMonotonicTimeNs();
    const uint64_t SNAPSHOT_INTERVAL_NS = 50'000'000; 

    while (running_.load(std::memory_order_relaxed)) {
        uint32_t processed = 0;
        RuntimeCmd cmd;
        
        while (processed < config_.batch_size && ingress_queue_.try_pop(cmd)) {
            idle_iters = 0;
            uint64_t ts_ns = GetMonotonicTimeNs();
            
            // Assign sequential timestamp and sequence ID to guarantee deterministic outcomes
            // overriding the ingress timestamp
            cmd.ts_ns = ts_ns;
            
            ProcessCommand(cmd, ts_ns);
            processed++;
        }
        
        if (processed == 0) {
            idle_iters++;
            ApplyWaitStrategy(idle_iters);
        } else {
            uint64_t now = GetMonotonicTimeNs();
            if (now - last_snapshot_time > SNAPSHOT_INTERVAL_NS) {
                // Emit snapshots for all active symbols
                for (uint32_t i = 0; i < books_.size(); ++i) {
                    if (books_[i]) {
                        EmitSnapshot(i, now);
                    }
                }
                last_snapshot_time = now;
            }
        }
    }
}

void Shard::ProcessCommand(const RuntimeCmd& cmd, uint64_t engine_ts) {
    RuntimeReply reply;
    reply.reply_handle = cmd.reply_handle;
    reply.success = false;
    
    if (cmd.symbol_idx >= books_.size() || !books_[cmd.symbol_idx]) {
        reply.result.status = OrderStatus::REJECTED;
        reply.result.reject_reason = "UNKNOWN_SYMBOL";
    } else {
        OrderBook& book = *books_[cmd.symbol_idx];
        EgressSink sink(event_ring_);
        
        if (cmd.is_cancel) {
            reply.success = book.Cancel(cmd.order_id, engine_ts, sink);
        } else {
            NewOrderCmd noc;
            noc.symbol_idx = cmd.symbol_idx;
            noc.orderId = cmd.order_id;
            noc.userId = cmd.user_id;
            noc.side = cmd.side;
            noc.type = cmd.type;
            noc.price = cmd.price;
            noc.qty = cmd.qty;
            noc.ts_ns = engine_ts; // Use engine clock
            noc.seq = ++monotonic_seq_;
            noc.self_trade_prevention = cmd.stp;
            
            reply.result = book.Submit(noc, sink);
            reply.success = true;
        }
    }
    
    while (!reply_ring_.try_push(reply)) {
        // Wait for gRPC thread to drain.
    }
}

void Shard::EmitSnapshot(uint32_t symbol_idx, uint64_t engine_ts) {
    auto depth = books_[symbol_idx]->GetDepth(10);
    DepthSnapshot snap;
    snap.symbol_idx = symbol_idx;
    snap.ts_ns = engine_ts;
    
    snap.bid_count = std::min((uint32_t)depth.bids.size(), 10u);
    for (uint32_t i = 0; i < snap.bid_count; ++i) {
        snap.bids[i].price = depth.bids[i].price;
        snap.bids[i].qty = depth.bids[i].qty;
        snap.bids[i].order_count = depth.bids[i].order_count;
    }
    
    snap.ask_count = std::min((uint32_t)depth.asks.size(), 10u);
    for (uint32_t i = 0; i < snap.ask_count; ++i) {
        snap.asks[i].price = depth.asks[i].price;
        snap.asks[i].qty = depth.asks[i].qty;
        snap.asks[i].order_count = depth.asks[i].order_count;
    }
    
    // Best-effort push, snapshot thread might be slow
    snapshot_ring_.try_push(snap);
}

void Shard::SnapshotLoop() {
    PinThread(-1, "snap_" + std::to_string(config_.shard_id)); // Normally not pinned
    
    while (running_.load(std::memory_order_relaxed)) {
        DepthSnapshot snap;
        bool processed = false;
        while (snapshot_ring_.try_pop(snap)) {
            processed = true;
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snap.symbol_idx < latest_snapshots_.size()) {
                latest_snapshots_[snap.symbol_idx] = snap;
            }
        }
        
        if (!processed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

} // namespace engine
