#pragma once

#include <atomic>
#include <cstddef>
#include <cassert>
#include <type_traits>
#include <new>
#include <vector>

namespace engine {

// Bounded Multi-Producer Single-Consumer queue based on Dmitry Vyukov's bounded MPMC queue.
// Capacity N must be a power of 2.
template <typename T, size_t N>
class MpscQueue {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert((N > 0) && ((N & (N - 1)) == 0), "N must be a power of 2");

public:
    MpscQueue() {
        for (size_t i = 0; i < N; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    // Tries to push an element. Returns false if full.
    bool try_push(const T& data) {
        Cell* cell;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &cells_[pos & (N - 1)];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Queue full
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        cell->data = data;
        // Release: ensures data is visible before sequence is updated for consumer.
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // Tries to pop an element. Returns false if empty.
    // Note: Single consumer assumption makes this slightly simpler than MPMC,
    // but we use the MPMC algorithm logic for safety. Since it's MPSC,
    // only one thread calls try_pop, so we don't strictly need a CAS on dequeue_pos_.
    bool try_pop(T& data) {
        Cell* cell;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        
        cell = &cells_[pos & (N - 1)];
        size_t seq = cell->sequence.load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
        
        if (dif == 0) {
            // Found item
            data = cell->data;
            // Relaxed update of dequeue pos, as we are the only consumer
            dequeue_pos_.store(pos + 1, std::memory_order_relaxed);
            // Release: marks cell as free for producers
            cell->sequence.store(pos + N, std::memory_order_release);
            return true;
        } else {
            return false; // Queue empty
        }
    }

private:
    struct alignas(64) Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    alignas(64) Cell cells_[N];
    alignas(64) std::atomic<size_t> enqueue_pos_;
    alignas(64) std::atomic<size_t> dequeue_pos_;
};

} // namespace engine
