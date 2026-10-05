#pragma once

#include "engine/types.h"
#include <vector>
#include <cstdint>
#include <bit>
#include <stdexcept>

namespace engine {

struct PriceLevel {
    Handle head = kInvalidHandle;
    Handle tail = kInvalidHandle;
    Qty total_qty = 0;
    uint32_t order_count = 0;
};

// A simple 2-level 64-ary hierarchical bitmap.
// Supports up to 64 * 64 = 4096 levels.
// (For wider ranges, a 3-level bitmap up to 262144 levels can be used, but keeping it 2-level for now).
class HierarchicalBitmap {
public:
    explicit HierarchicalBitmap(uint32_t num_bits) : num_bits_(num_bits) {
        if (num_bits > 4096) {
            throw std::invalid_argument("Bitmap capacity exceeded. Max 4096 levels supported by 2-level bitmap.");
        }
        words_.resize(64, 0);
        root_ = 0;
    }

    void Set(uint32_t index) {
        if (index >= num_bits_) return;
        uint32_t word_idx = index >> 6;
        uint32_t bit_idx = index & 63;
        words_[word_idx] |= (1ULL << bit_idx);
        root_ |= (1ULL << word_idx);
    }

    void Clear(uint32_t index) {
        if (index >= num_bits_) return;
        uint32_t word_idx = index >> 6;
        uint32_t bit_idx = index & 63;
        words_[word_idx] &= ~(1ULL << bit_idx);
        if (words_[word_idx] == 0) {
            root_ &= ~(1ULL << word_idx);
        }
    }

    // Find highest set bit (for Best Bid)
    int32_t FindHighest() const {
        if (root_ == 0) return -1;
        uint32_t highest_word_idx = 63 - std::countl_zero(root_);
        uint32_t highest_bit_idx = 63 - std::countl_zero(words_[highest_word_idx]);
        return static_cast<int32_t>((highest_word_idx << 6) | highest_bit_idx);
    }

    // Find lowest set bit (for Best Ask)
    int32_t FindLowest() const {
        if (root_ == 0) return -1;
        uint32_t lowest_word_idx = std::countr_zero(root_);
        uint32_t lowest_bit_idx = std::countr_zero(words_[lowest_word_idx]);
        return static_cast<int32_t>((lowest_word_idx << 6) | lowest_bit_idx);
    }

private:
    uint32_t num_bits_;
    uint64_t root_;
    std::vector<uint64_t> words_;
};

}  // namespace engine
