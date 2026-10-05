#pragma once

#include "engine/types.h"
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <cstring>
#include <cassert>

namespace engine {

struct alignas(32) HotOrder {
    uint32_t price_index;
    Qty remaining_qty;
    Handle prev_handle;
    Handle next_handle;
    Side side;
    OrderType type;
    OrderStatus status;
    bool self_trade_prevention;
};
static_assert(sizeof(HotOrder) <= 64, "HotOrder exceeds 64 bytes");

struct ColdOrder {
    char order_id[48];
    char user_id[32];
    uint64_t ts_ns;
    uint64_t seq;
    
    void set_order_id(std::string_view id) {
        size_t len = std::min(id.size(), sizeof(order_id) - 1);
        std::memcpy(order_id, id.data(), len);
        order_id[len] = '\0';
    }

    void set_user_id(std::string_view id) {
        size_t len = std::min(id.size(), sizeof(user_id) - 1);
        std::memcpy(user_id, id.data(), len);
        user_id[len] = '\0';
    }
    
    std::string_view get_order_id() const {
        return std::string_view(order_id);
    }
    
    std::string_view get_user_id() const {
        return std::string_view(user_id);
    }
};

class OrderPool {
public:
    explicit OrderPool(uint32_t capacity) : capacity_(capacity), hot_data_(capacity), cold_data_(capacity) {
        free_head_ = 0;
        for (uint32_t i = 0; i < capacity; ++i) {
            hot_data_[i].next_handle = (i + 1 < capacity) ? (i + 1) : kInvalidHandle;
        }
    }

    Handle Allocate() {
        if (free_head_ == kInvalidHandle) {
            return kInvalidHandle;
        }
        Handle handle = free_head_;
        free_head_ = hot_data_[handle].next_handle;
        return handle;
    }

    void Free(Handle handle) {
        assert(handle < capacity_);
        hot_data_[handle].next_handle = free_head_;
        free_head_ = handle;
    }

    HotOrder& GetHot(Handle handle) {
        return hot_data_[handle];
    }
    const HotOrder& GetHot(Handle handle) const {
        return hot_data_[handle];
    }

    ColdOrder& GetCold(Handle handle) {
        return cold_data_[handle];
    }
    const ColdOrder& GetCold(Handle handle) const {
        return cold_data_[handle];
    }

    uint32_t capacity() const { return capacity_; }

private:
    uint32_t capacity_;
    Handle free_head_;
    std::vector<HotOrder> hot_data_;
    std::vector<ColdOrder> cold_data_;
};

}  // namespace engine
