#pragma once

#include "engine/types.h"
#include <cstdint>

namespace engine {

// POD for multi-threaded ingestion
struct RuntimeCmd {
    uint8_t is_cancel;      // 0 = NewOrder, 1 = Cancel
    uint32_t symbol_idx;    // Target symbol/shard
    
    char order_id[48];
    char user_id[32];
    uint64_t order_id_hash; // Precomputed hash of order_id
    
    Side side;
    OrderType type;
    Price price;
    Qty qty;
    bool stp;               // Self-Trade Prevention

    uint64_t ingress_ts_ns; // When the gRPC layer received it
    uint32_t reply_handle;  // Index into a pre-allocated reply-slot table to signal the waiting thread
};

// POD for reply from shard to gRPC thread
struct RuntimeReply {
    uint32_t reply_handle;
    MatchResult result;
    bool success; // For cancel ops
};

} // namespace engine
