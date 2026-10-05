#include "engine/types.h"
#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>

using namespace engine;

#pragma pack(push, 1)
struct BinaryCmd {
    uint8_t is_cancel; // 0 = NewOrder, 1 = Cancel
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

void copy_str(char* dest, const std::string& src, size_t max_len) {
    size_t len = std::min(src.size(), max_len - 1);
    std::memcpy(dest, src.data(), len);
    dest[len] = '\0';
}

void generate_workload(const std::string& profile, const std::string& outfile) {
    std::ofstream out(outfile, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open " << outfile << "\n";
        return;
    }

    std::mt19937 gen(1337); // Seeded for determinism
    
    int num_events = 1'000'000;
    double p_cancel, p_sweep;
    if (profile == "light") {
        p_cancel = 0.10; p_sweep = 0.01;
    } else if (profile == "mixed") {
        p_cancel = 0.25; p_sweep = 0.05;
    } else if (profile == "sweep-heavy") {
        p_cancel = 0.10; p_sweep = 0.20;
    } else {
        std::cerr << "Unknown profile\n"; return;
    }

    std::uniform_real_distribution<> prob(0.0, 1.0);
    std::uniform_int_distribution<Price> spread_dist(1, 5); // ticks from touch
    std::uniform_int_distribution<Price> sweep_dist(1, 10); // sweep depth
    std::uniform_int_distribution<Qty> qty_dist(1, 100);
    std::uniform_int_distribution<int> side_dist(0, 1);

    Price touch = 100000; // 1000 INR
    Price tick = 5;       // 5 paise

    std::vector<std::string> live_orders;

    for (int i = 0; i < num_events; ++i) {
        BinaryCmd cmd{};
        cmd.seq = i;
        cmd.ts_ns = i * 1000ULL; // 1us between events
        
        double r = prob(gen);
        if (r < p_cancel && !live_orders.empty()) {
            std::uniform_int_distribution<size_t> idx_dist(0, live_orders.size() - 1);
            size_t idx = idx_dist(gen);
            cmd.is_cancel = 1;
            copy_str(cmd.orderId, live_orders[idx], 48);
            live_orders[idx] = live_orders.back();
            live_orders.pop_back();
        } else {
            cmd.is_cancel = 0;
            std::string oid = "O" + std::to_string(i);
            copy_str(cmd.orderId, oid, 48);
            copy_str(cmd.userId, "U1", 32);
            cmd.side = side_dist(gen);
            cmd.qty = qty_dist(gen);
            cmd.type = static_cast<uint8_t>(OrderType::LIMIT);

            if (r < p_cancel + p_sweep) {
                // Marketable sweep
                Price depth = sweep_dist(gen) * tick;
                cmd.price = (cmd.side == 0) ? (touch + depth) : (touch - depth);
                // Sweeps might rest, don't add to live for simplicity in generator
            } else {
                // Near touch resting
                Price dist = spread_dist(gen) * tick;
                cmd.price = (cmd.side == 0) ? (touch - dist) : (touch + dist);
                live_orders.push_back(oid);
            }
        }
        
        out.write(reinterpret_cast<const char*>(&cmd), sizeof(cmd));
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: workload_generator <light|mixed|sweep-heavy> <outfile.bin>\n";
        return 1;
    }
    generate_workload(argv[1], argv[2]);
    return 0;
}
