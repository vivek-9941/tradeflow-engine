#pragma once

#include "engine/types.h"
#include <vector>
#include <cstdint>
#include <string_view>
#include <functional>

namespace engine {

// A simple open-addressing hash table matching orderId (hash + actual string check) to Handle.
// Using tombstone for deletions.
class OrderIndex {
public:
    explicit OrderIndex(uint32_t capacity) {
        // Size must be power of 2 for fast modulo, or we just use modulo. Let's make it a bit larger to keep load factor low.
        size_ = 1;
        while (size_ < capacity * 2) {
            size_ <<= 1;
        }
        mask_ = size_ - 1;
        table_.resize(size_, {0, kInvalidHandle});
    }

    // Insert or update
    bool Insert(std::string_view order_id, Handle handle) {
        uint64_t h = Hash(order_id);
        uint32_t idx = static_cast<uint32_t>(h & mask_);
        
        for (uint32_t i = 0; i < size_; ++i) {
            uint32_t current_idx = (idx + i) & mask_;
            Entry& e = table_[current_idx];
            if (e.handle == kInvalidHandle || e.handle == kTombstoneHandle) {
                e.hash = h;
                e.handle = handle;
                return true;
            }
            // If it already exists, update it. Note: We assume caller verified it's the same string if hash matches.
            if (e.hash == h) {
                e.handle = handle;
                return true;
            }
        }
        return false; // Table full
    }

    // Lookup
    // Since we only store the hash, the caller MUST verify that the order_id string in the pool
    // matches to handle collisions. This returns the first matching hash.
    Handle FindByHash(std::string_view order_id) const {
        uint64_t h = Hash(order_id);
        uint32_t idx = static_cast<uint32_t>(h & mask_);
        
        for (uint32_t i = 0; i < size_; ++i) {
            uint32_t current_idx = (idx + i) & mask_;
            const Entry& e = table_[current_idx];
            if (e.handle == kInvalidHandle) {
                break; // Empty slot, not found
            }
            if (e.handle != kTombstoneHandle && e.hash == h) {
                return e.handle; // Caller must verify string equality!
            }
        }
        return kInvalidHandle;
    }

    // Delete
    void EraseByHash(std::string_view order_id, Handle expected_handle) {
        uint64_t h = Hash(order_id);
        uint32_t idx = static_cast<uint32_t>(h & mask_);
        
        for (uint32_t i = 0; i < size_; ++i) {
            uint32_t current_idx = (idx + i) & mask_;
            Entry& e = table_[current_idx];
            if (e.handle == kInvalidHandle) {
                break; // Not found
            }
            if (e.hash == h && e.handle == expected_handle) {
                e.handle = kTombstoneHandle;
                return;
            }
        }
    }

private:
    static constexpr Handle kTombstoneHandle = 0xFFFFFFFE;

    struct Entry {
        uint64_t hash;
        Handle handle;
    };

    uint64_t Hash(std::string_view key) const {
        // FNV-1a 64-bit
        uint64_t hash = 0xcbf29ce484222325ULL;
        for (char c : key) {
            hash ^= static_cast<uint64_t>(c);
            hash *= 0x100000001b3ULL;
        }
        return hash;
    }

    uint32_t size_;
    uint32_t mask_;
    std::vector<Entry> table_;
};

}  // namespace engine
