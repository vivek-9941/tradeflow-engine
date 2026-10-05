#pragma once

#include "engine/types.h"
#include <array>
#include <string_view>
#include <cstring>
#include <cstdint>

namespace engine {

class ZeroAllocDedupeWindow {
public:
    explicit ZeroAllocDedupeWindow(uint32_t capacity) : capacity_(capacity) {
        // power of 2 size
        size_ = 1;
        while (size_ < capacity * 2) {
            size_ <<= 1;
        }
        mask_ = size_ - 1;
        table_.resize(size_);
        for (auto& e : table_) e.valid = false;
        
        insert_order_.resize(capacity_);
        insert_head_ = 0;
    }

    void Add(std::string_view order_id, const MatchResult& result) {
        // Find existing or empty
        uint64_t h = Hash(order_id);
        uint32_t idx = static_cast<uint32_t>(h & mask_);
        
        for (uint32_t i = 0; i < size_; ++i) {
            uint32_t current_idx = (idx + i) & mask_;
            Entry& e = table_[current_idx];
            
            if (!e.valid) {
                // Not found, insert new
                // Check if we need to evict oldest
                if (count_ >= capacity_) {
                    EvictOldest();
                }
                
                e.valid = true;
                e.hash = h;
                size_t len = std::min(order_id.size(), sizeof(e.order_id) - 1);
                std::memcpy(e.order_id, order_id.data(), len);
                e.order_id[len] = '\0';
                e.status = result.status;
                e.filled_qty = result.filled_qty;
                e.remaining_qty = result.remaining_qty;
                
                // Track insertion order
                insert_order_[insert_head_] = current_idx;
                insert_head_ = (insert_head_ + 1) % capacity_;
                count_++;
                return;
            } else if (e.hash == h && std::string_view(e.order_id) == order_id) {
                // Update existing
                e.status = result.status;
                e.filled_qty = result.filled_qty;
                e.remaining_qty = result.remaining_qty;
                return;
            }
        }
    }

    bool TryLookup(std::string_view order_id, MatchResult& out_result) const {
        uint64_t h = Hash(order_id);
        uint32_t idx = static_cast<uint32_t>(h & mask_);
        
        for (uint32_t i = 0; i < size_; ++i) {
            uint32_t current_idx = (idx + i) & mask_;
            const Entry& e = table_[current_idx];
            
            if (!e.valid) {
                return false;
            }
            if (e.hash == h && std::string_view(e.order_id) == order_id) {
                out_result.status = e.status;
                out_result.filled_qty = e.filled_qty;
                out_result.remaining_qty = e.remaining_qty;
                out_result.duplicate = true;
                return true;
            }
        }
        return false;
    }

private:
    struct Entry {
        bool valid;
        uint64_t hash;
        char order_id[48];
        OrderStatus status;
        Qty filled_qty;
        Qty remaining_qty;
    };

    uint64_t Hash(std::string_view key) const {
        uint64_t hash = 0xcbf29ce484222325ULL;
        for (char c : key) {
            hash ^= static_cast<uint64_t>(c);
            hash *= 0x100000001b3ULL;
        }
        return hash;
    }

    void EvictOldest() {
        uint32_t idx_to_evict = insert_order_[insert_head_];
        table_[idx_to_evict].valid = false;
        count_--;
        // Open addressing hash table eviction properly requires tombstones or backward shifting.
        // For simplicity in a bounded cache, backward shifting is ideal to avoid tombstone buildup.
        uint32_t i = (idx_to_evict + 1) & mask_;
        uint32_t empty_idx = idx_to_evict;
        
        while (table_[i].valid) {
            uint32_t desired = table_[i].hash & mask_;
            // If the element at i would prefer to be at or before empty_idx (considering wrap-around)
            bool shift = (i > empty_idx) ? (desired <= empty_idx || desired > i)
                                         : (desired <= empty_idx && desired > i);
            if (shift) {
                table_[empty_idx] = table_[i];
                table_[i].valid = false;
                
                // Update insertion order pointer to point to new location
                for (uint32_t j = 0; j < capacity_; ++j) {
                    if (insert_order_[j] == i) {
                        insert_order_[j] = empty_idx;
                        break;
                    }
                }
                
                empty_idx = i;
            }
            i = (i + 1) & mask_;
        }
    }

    uint32_t capacity_;
    uint32_t size_;
    uint32_t mask_;
    uint32_t count_ = 0;
    std::vector<Entry> table_;
    
    std::vector<uint32_t> insert_order_; // Circular buffer tracking table indices
    uint32_t insert_head_ = 0;
};

}  // namespace engine
