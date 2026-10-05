#include "engine/order_book.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <hdr_histogram.h>

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

class NullSink : public EventSink {
public:
    void OnTrade(const Fill&) override {}
    void OnCancel(std::string_view, std::string_view, uint64_t, Qty) override {}
};

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
    
    OrderBookConfig cfg{5, 100000, 0.30, 2000000, 1000000};
    OrderBook book(cfg);
    NullSink sink;
    
    struct hdr_histogram* hist;
    hdr_init(1, 1000000000LL, 3, &hist); // 1ns to 1sec

    // Warm-up: send first 10% cmds as fast as possible
    size_t warmup_count = num_cmds / 10;
    for (size_t i = 0; i < warmup_count; ++i) {
        auto& c = cmds[i];
        if (c.is_cancel) {
            book.Cancel(c.orderId, c.ts_ns, sink);
        } else {
            NewOrderCmd noc{1, c.orderId, c.userId, static_cast<Side>(c.side), static_cast<OrderType>(c.type), c.price, c.qty, c.ts_ns, c.seq, false};
            book.Submit(noc, sink);
        }
    }

    // Measurement loop (Open Loop) - 100k cmds/sec -> 10us between intended send times
    uint64_t rate_ns = 10'000; 
    uint64_t start_time = get_time_ns() + 1'000'000; // 1ms from now

    for (size_t i = warmup_count; i < num_cmds; ++i) {
        uint64_t intended_time = start_time + (i - warmup_count) * rate_ns;
        
        // Spin wait
        while (get_time_ns() < intended_time) {
            // tight loop
        }

        auto& c = cmds[i];
        
        if (c.is_cancel) {
            book.Cancel(c.orderId, c.ts_ns, sink);
        } else {
            NewOrderCmd noc{1, c.orderId, c.userId, static_cast<Side>(c.side), static_cast<OrderType>(c.type), c.price, c.qty, c.ts_ns, c.seq, false};
            book.Submit(noc, sink);
        }
        
        uint64_t end_time = get_time_ns();
        uint64_t latency = end_time - intended_time; // Includes coordinated omission
        hdr_record_value(hist, latency);
    }

    std::cout << "Latency Harness Results (Open-loop, Intended send time):\n"
              << "P50 (ns): " << hdr_value_at_percentile(hist, 50.0) << "\n"
              << "P90 (ns): " << hdr_value_at_percentile(hist, 90.0) << "\n"
              << "P99 (ns): " << hdr_value_at_percentile(hist, 99.0) << "\n"
              << "P99.9(ns): " << hdr_value_at_percentile(hist, 99.9) << "\n"
              << "Max (ns): " << hdr_max(hist) << "\n"
              << "Throughput: 100,000 cmds/sec (fixed pacing)\n";

    hdr_close(hist);
    return 0;
}
