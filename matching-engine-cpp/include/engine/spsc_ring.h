#pragma once

#include <atomic>
#include <cstddef>
#include <cassert>
#include <type_traits>
#include <new>

#if defined(__cpp_lib_hardware_interference_size)
    constexpr size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
    constexpr size_t CACHE_LINE_SIZE = 64;
#endif

namespace engine {

// Single-Producer Single-Consumer lock-free ring buffer.
// Capacity N must be a power of 2.
template <typename T, size_t N>
class SpscRing {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert((N > 0) && ((N & (N - 1)) == 0), "N must be a power of 2");

public:
    SpscRing() : tail_(0), head_(0), cached_head_(0), cached_tail_(0) {}

    // Disallow copy/move
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    // Tries to push an element. Returns false if full.
    bool try_push(const T& item) {
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = current_tail + 1;

        // If the ring appears full based on our cached head...
        if (next_tail - cached_head_ > N) {
            // Memory Order Acquire: ensure we see any item reads before the head update 
            // by the consumer. (Though relaxed is often enough if the consumer uses release, 
            // acquire ensures we see all state related to the consumed slot).
            cached_head_ = head_.load(std::memory_order_acquire);
            if (next_tail - cached_head_ > N) {
                return false; // Actually full
            }
        }

        buffer_[current_tail & (N - 1)] = item;
        
        // Memory Order Release: ensures the item is completely written to memory
        // before the tail index is incremented and visible to the consumer.
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Tries to pop an element. Returns false if empty.
    bool try_pop(T& item) {
        size_t current_head = head_.load(std::memory_order_relaxed);

        // If the ring appears empty based on our cached tail...
        if (current_head == cached_tail_) {
            // Memory Order Acquire: ensures we see the complete item written by the 
            // producer before we read the new tail.
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (current_head == cached_tail_) {
                return false; // Actually empty
            }
        }

        item = buffer_[current_head & (N - 1)];

        // Memory Order Release: ensures the item is completely read before the
        // head index is incremented and visible to the producer (freeing the slot).
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

private:
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_;
    alignas(CACHE_LINE_SIZE) size_t cached_head_;

    alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_;
    alignas(CACHE_LINE_SIZE) size_t cached_tail_;

    alignas(CACHE_LINE_SIZE) T buffer_[N];
};

} // namespace engine
