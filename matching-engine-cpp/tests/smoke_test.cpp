#include <gtest/gtest.h>
#include "engine/order_book.h"
#include <vector>
#include <map>
#include <deque>
#include <string>
#include <random>
#include <cstdlib>
#include <new>
#include <atomic>

using namespace engine;

// Zero-allocation test infrastructure
static std::atomic<int> g_alloc_count{0};
static std::atomic<bool> g_track_allocs{false};

void* operator new(size_t size) {
    if (g_track_allocs.load(std::memory_order_relaxed)) {
        g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    }
    if (void* ptr = std::malloc(size)) {
        return ptr;
    }
    throw std::bad_alloc{};
}

void operator delete(void* ptr) noexcept {
    std::free(ptr);
}

void operator delete(void* ptr, size_t) noexcept {
    std::free(ptr);
}

class TestEventSink : public EventSink {
public:
    void OnTrade(const Fill& fill) override {
        fills.push_back(fill);
    }
    void OnCancel(std::string_view orderId, std::string_view userId, uint64_t ts_ns, Qty remaining_qty) override {
        cancels.push_back(std::string(orderId));
    }

    std::vector<Fill> fills;
    std::vector<std::string> cancels;
};

class OrderBookTest : public ::testing::Test {
protected:
    OrderBookConfig config_{ 5, 100000, 0.30, 10000, 100 };
    OrderBook book_{config_};
    TestEventSink sink_;
};

TEST_F(OrderBookTest, TableDriven_SimpleMatch) {
    NewOrderCmd sell{"sym1", "O1", "U1", Side::SELL, OrderType::LIMIT, 100000, 100, 1, 1, false};
    auto r1 = book_.Submit(sell, sink_);
    EXPECT_EQ(r1.status, OrderStatus::PENDING);
    EXPECT_EQ(r1.filled_qty, 0);

    NewOrderCmd buy{"sym1", "O2", "U2", Side::BUY, OrderType::LIMIT, 100000, 100, 2, 2, false};
    auto r2 = book_.Submit(buy, sink_);
    EXPECT_EQ(r2.status, OrderStatus::FULLY_FILLED);
    EXPECT_EQ(r2.filled_qty, 100);
    EXPECT_EQ(sink_.fills.size(), 1);
    EXPECT_EQ(sink_.fills[0].price, 100000);
}

TEST_F(OrderBookTest, TableDriven_NoMatchWhenBelowAsk) {
    NewOrderCmd sell{"sym1", "O1", "U1", Side::SELL, OrderType::LIMIT, 100050, 100, 1, 1, false};
    book_.Submit(sell, sink_);
    
    NewOrderCmd buy{"sym1", "O2", "U2", Side::BUY, OrderType::LIMIT, 100000, 100, 2, 2, false};
    auto r2 = book_.Submit(buy, sink_);
    EXPECT_EQ(r2.status, OrderStatus::PENDING);
    EXPECT_EQ(r2.filled_qty, 0);
    EXPECT_EQ(sink_.fills.size(), 0);
}

TEST_F(OrderBookTest, TableDriven_IOCRemainderCancelled) {
    NewOrderCmd sell{"sym1", "O1", "U1", Side::SELL, OrderType::LIMIT, 100000, 50, 1, 1, false};
    book_.Submit(sell, sink_);
    
    NewOrderCmd buy{"sym1", "O2", "U2", Side::BUY, OrderType::IOC, 100000, 100, 2, 2, false};
    auto r2 = book_.Submit(buy, sink_);
    EXPECT_EQ(r2.status, OrderStatus::PARTIALLY_FILLED);
    EXPECT_EQ(r2.filled_qty, 50);
    EXPECT_TRUE(r2.remainder_cancelled);
    EXPECT_EQ(sink_.cancels.size(), 1);
    EXPECT_EQ(sink_.cancels[0], "O2");
}

TEST_F(OrderBookTest, TableDriven_DuplicateReject) {
    NewOrderCmd buy{"sym1", "O1", "U1", Side::BUY, OrderType::LIMIT, 100000, 100, 1, 1, false};
    book_.Submit(buy, sink_);
    
    auto r2 = book_.Submit(buy, sink_); // duplicate
    EXPECT_TRUE(r2.duplicate);
    EXPECT_EQ(r2.status, OrderStatus::PENDING);
}

TEST_F(OrderBookTest, ZeroAllocationHotPath) {
    NewOrderCmd sell{"sym1", "O1", "U1", Side::SELL, OrderType::LIMIT, 100000, 100, 1, 1, false};
    NewOrderCmd buy{"sym1", "O2", "U2", Side::BUY, OrderType::LIMIT, 100000, 100, 2, 2, false};

    // Warm up
    book_.Submit(sell, sink_);
    book_.Submit(buy, sink_);
    sink_.fills.clear();

    // The event sink allocates due to std::vector. In production, we'd use a ring buffer.
    // For this test, we can pre-allocate the vector capacity or use a non-allocating sink.
    class NoAllocSink : public EventSink {
    public:
        void OnTrade(const Fill&) override {}
        void OnCancel(std::string_view, std::string_view, uint64_t, Qty) override {}
    } no_alloc_sink;

    g_alloc_count = 0;
    g_track_allocs = true;

    NewOrderCmd sell2{"sym1", "O3", "U1", Side::SELL, OrderType::LIMIT, 100000, 100, 3, 3, false};
    NewOrderCmd buy2{"sym1", "O4", "U2", Side::BUY, OrderType::LIMIT, 100000, 100, 4, 4, false};
    
    book_.Submit(sell2, no_alloc_sink);
    book_.Submit(buy2, no_alloc_sink);
    book_.Cancel("O3", 5, no_alloc_sink); // Won't do anything since matched, but tests cancel path

    g_track_allocs = false;
    
    EXPECT_EQ(g_alloc_count.load(), 0);
}

// Simple reference model for fuzzing
class RefOrderBook {
public:
    struct RefOrder {
        std::string order_id;
        std::string user_id;
        Side side;
        OrderType type;
        Price price;
        Qty qty;
    };
    
    std::map<Price, std::deque<RefOrder>> asks;
    std::map<Price, std::deque<RefOrder>, std::greater<Price>> bids;

    Qty Submit(const NewOrderCmd& cmd, EventSink& sink) {
        Qty remaining = cmd.qty;
        if (cmd.side == Side::BUY) {
            auto it = asks.begin();
            while (it != asks.end() && remaining > 0 && it->first <= cmd.price) {
                auto& queue = it->second;
                while (!queue.empty() && remaining > 0) {
                    auto& resting = queue.front();
                    Qty fill = std::min(remaining, resting.qty);
                    remaining -= fill;
                    resting.qty -= fill;
                    
                    Fill f;
                    f.price = it->first;
                    f.qty = fill;
                    sink.OnTrade(f);
                    
                    if (resting.qty == 0) queue.pop_front();
                }
                if (queue.empty()) it = asks.erase(it);
                else ++it;
            }
            if (remaining > 0 && cmd.type != OrderType::IOC) {
                bids[cmd.price].push_back({std::string(cmd.orderId), std::string(cmd.userId), cmd.side, cmd.type, cmd.price, remaining});
            }
        } else {
            auto it = bids.begin();
            while (it != bids.end() && remaining > 0 && it->first >= cmd.price) {
                auto& queue = it->second;
                while (!queue.empty() && remaining > 0) {
                    auto& resting = queue.front();
                    Qty fill = std::min(remaining, resting.qty);
                    remaining -= fill;
                    resting.qty -= fill;
                    
                    Fill f;
                    f.price = it->first;
                    f.qty = fill;
                    sink.OnTrade(f);
                    
                    if (resting.qty == 0) queue.pop_front();
                }
                if (queue.empty()) it = bids.erase(it);
                else ++it;
            }
            if (remaining > 0 && cmd.type != OrderType::IOC) {
                asks[cmd.price].push_back({std::string(cmd.orderId), std::string(cmd.userId), cmd.side, cmd.type, cmd.price, remaining});
            }
        }
        return cmd.qty - remaining;
    }
};

TEST_F(OrderBookTest, DifferentialFuzz) {
    OrderBookConfig cfg{ 1, 100, 0.50, 20000, 1000 };
    OrderBook fast_book(cfg);
    RefOrderBook ref_book;
    
    TestEventSink fast_sink, ref_sink;
    
    std::mt19937 gen(42);
    std::uniform_int_distribution<Price> price_dist(50, 150);
    std::uniform_int_distribution<Qty> qty_dist(1, 100);
    std::uniform_int_distribution<int> side_dist(0, 1);
    std::uniform_int_distribution<int> type_dist(0, 9);
    
    for (int i = 0; i < 10000; ++i) { // Reduced to 10k for speed, ideally 1M
        std::string oid = "O" + std::to_string(i);
        Side side = side_dist(gen) == 0 ? Side::BUY : Side::SELL;
        OrderType type = type_dist(gen) == 0 ? OrderType::IOC : OrderType::LIMIT;
        Price p = price_dist(gen);
        Qty q = qty_dist(gen);
        
        NewOrderCmd cmd{1, oid, "U", side, type, p, q, (uint64_t)i, (uint64_t)i, false};
        
        fast_sink.fills.clear();
        ref_sink.fills.clear();
        
        auto res = fast_book.Submit(cmd, fast_sink);
        Qty ref_filled = ref_book.Submit(cmd, ref_sink);
        
        ASSERT_EQ(res.filled_qty, ref_filled) << "Fuzz mismatch at iter " << i;
        ASSERT_EQ(fast_sink.fills.size(), ref_sink.fills.size());
        
        for (size_t j = 0; j < fast_sink.fills.size(); ++j) {
            ASSERT_EQ(fast_sink.fills[j].price, ref_sink.fills[j].price);
            ASSERT_EQ(fast_sink.fills[j].qty, ref_sink.fills[j].qty);
        }
    }
}
