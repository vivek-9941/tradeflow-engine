#include "engine/shard.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <hdr_histogram.h>
#include <atomic>

using namespace engine;

#pragma pack(push, 1)
struct BinaryCmd {
    uint8_t is_cancel;
    char orderId[48];
    char userId[32];
    uint8_t side;
    uint8_t type;
    int64_t price;
    int64_t qty;
    uint64_t ts_ns;
    uint64_t seq;
};
#pragma pack(pop)

inline uint64_t get_time_ns() {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: latency_harness <workload.bin>\n";
        return 1;
    }

    std::ifstream in(argv[1], std::ios::binary | std::ios::ate);
    if (!in) {
        std::cerr << "Failed to open " << argv[1] << "\n";
        return 1;
    }
    
    size_t size = in.tellg();
    in.seekg(0);
    size_t num_cmds = size / sizeof(BinaryCmd);
    std::vector<BinaryCmd> cmds(num_cmds);
    in.read(reinterpret_cast<char*>(cmds.data()), size);
    
    ShardConfig shard_cfg{0, WaitStrategy::HYBRID, -1, 256};
    Shard shard(shard_cfg);
    OrderBookConfig ob_cfg{5, 100000, 0.30, 2000000, 1000000};
    shard.AddSymbol(0, ob_cfg);
    
    shard.Start();
    
    struct hdr_histogram* hist;
    hdr_init(1, 1000000000LL, 3, &hist); // 1ns to 1sec

    std::vector<uint64_t> intended_times(num_cmds, 0);

    // Reply Thread
    std::atomic<size_t> replies_received{0};
    std::atomic<bool> all_sent{false};
    
    size_t warmup_count = num_cmds / 10;
    
    std::thread reply_thread([&]() {
        RuntimeReply reply;
        while (!all_sent.load() || replies_received.load() < num_cmds) {
            if (shard.GetReplyRing().try_pop(reply)) {
                size_t idx = reply.reply_handle;
                if (idx >= warmup_count) {
                    uint64_t end_time = get_time_ns();
                    uint64_t latency = end_time - intended_times[idx];
                    hdr_record_value(hist, latency);
                }
                replies_received++;
            } else {
                std::this_thread::yield();
            }
        }
    });

    // Warm-up: send first 10% cmds as fast as possible
    for (size_t i = 0; i < warmup_count; ++i) {
        auto& c = cmds[i];
        RuntimeCmd noc{};
        noc.is_cancel = c.is_cancel;
        noc.symbol_idx = 0;
        std::memcpy(noc.order_id, c.orderId, 48);
        std::memcpy(noc.user_id, c.userId, 32);
        noc.side = static_cast<Side>(c.side);
        noc.type = static_cast<OrderType>(c.type);
        noc.price = c.price;
        noc.qty = c.qty;
        noc.reply_handle = i;
        
        while (!shard.TryEnqueueCmd(noc)) {
            std::this_thread::yield();
        }
    }

    // Wait for warmup to finish processing
    while (replies_received.load() < warmup_count) {
        std::this_thread::yield();
    }

    // Measurement loop (Open Loop) - 100k cmds/sec -> 10us between intended send times
    uint64_t rate_ns = 10'000; 
    uint64_t start_time = get_time_ns() + 1'000'000; // 1ms from now

    for (size_t i = warmup_count; i < num_cmds; ++i) {
        uint64_t intended = start_time + (i - warmup_count) * rate_ns;
        intended_times[i] = intended;
        
        // Spin wait to match intended send time
        while (get_time_ns() < intended) {}

        auto& c = cmds[i];
        RuntimeCmd noc{};
        noc.is_cancel = c.is_cancel;
        noc.symbol_idx = 0;
        std::memcpy(noc.order_id, c.orderId, 48);
        std::memcpy(noc.user_id, c.userId, 32);
        noc.side = static_cast<Side>(c.side);
        noc.type = static_cast<OrderType>(c.type);
        noc.price = c.price;
        noc.qty = c.qty;
        noc.reply_handle = i;
        
        // Measure enqueue time + full round trip (including backpressure waiting)
        while (!shard.TryEnqueueCmd(noc)) {}
    }
    
    all_sent = true;
    reply_thread.join();
    shard.Stop();

    std::cout << "Latency Harness Results (Async Runtime, Intended send time):\n"
              << "P50 (ns): " << hdr_value_at_percentile(hist, 50.0) << "\n"
              << "P90 (ns): " << hdr_value_at_percentile(hist, 90.0) << "\n"
              << "P99 (ns): " << hdr_value_at_percentile(hist, 99.0) << "\n"
              << "P99.9(ns): " << hdr_value_at_percentile(hist, 99.9) << "\n"
              << "Max (ns): " << hdr_max(hist) << "\n"
              << "Throughput: 100,000 cmds/sec (fixed pacing)\n";

    hdr_close(hist);
    return 0;
}
