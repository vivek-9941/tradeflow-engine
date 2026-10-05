#include <benchmark/benchmark.h>
#include "engine/order_book.h"
#include <string>

using namespace engine;

class BenchSink : public EventSink {
public:
    void OnTrade(const Fill&) override {}
    void OnCancel(std::string_view, std::string_view, uint64_t, Qty) override {}
};

static void BM_AddResting(benchmark::State& state) {
    OrderBookConfig cfg{5, 100000, 0.30, 2000000, 1000000};
    BenchSink sink;
    
    uint64_t seq = 0;
    for (auto _ : state) {
        state.PauseTiming();
        OrderBook book(cfg);
        state.ResumeTiming();
        
        for (int i = 0; i < state.range(0); ++i) {
            std::string oid = "O" + std::to_string(++seq);
            NewOrderCmd noc{1, oid, "U1", Side::BUY, OrderType::LIMIT, 99000, 100, seq, seq, false};
            book.Submit(noc, sink);
        }
    }
}
BENCHMARK(BM_AddResting)->Range(100, 10000);

static void BM_MatchOneLevel(benchmark::State& state) {
    OrderBookConfig cfg{5, 100000, 0.30, 2000000, 1000000};
    BenchSink sink;
    
    for (auto _ : state) {
        state.PauseTiming();
        OrderBook book(cfg);
        NewOrderCmd resting{1, "REST", "U1", Side::SELL, OrderType::LIMIT, 100000, 100, 1, 1, false};
        book.Submit(resting, sink);
        NewOrderCmd active{1, "ACT", "U2", Side::BUY, OrderType::IOC, 100000, 100, 2, 2, false};
        state.ResumeTiming();
        
        book.Submit(active, sink);
    }
}
BENCHMARK(BM_MatchOneLevel);

static void BM_Sweep10Levels(benchmark::State& state) {
    OrderBookConfig cfg{5, 100000, 0.30, 2000000, 1000000};
    BenchSink sink;
    
    for (auto _ : state) {
        state.PauseTiming();
        OrderBook book(cfg);
        for (int i = 0; i < 10; ++i) {
            std::string oid = "R" + std::to_string(i);
            NewOrderCmd resting{1, oid, "U1", Side::SELL, OrderType::LIMIT, 100000 + i*5, 100, 1, 1, false};
            book.Submit(resting, sink);
        }
        NewOrderCmd active{1, "ACT", "U2", Side::BUY, OrderType::IOC, 100050, 1000, 2, 2, false};
        state.ResumeTiming();
        
        book.Submit(active, sink);
    }
}
BENCHMARK(BM_Sweep10Levels);

static void BM_Cancel(benchmark::State& state) {
    OrderBookConfig cfg{5, 100000, 0.30, 2000000, 1000000};
    BenchSink sink;
    
    for (auto _ : state) {
        state.PauseTiming();
        OrderBook book(cfg);
        NewOrderCmd resting{1, "REST", "U1", Side::SELL, OrderType::LIMIT, 100000, 100, 1, 1, false};
        book.Submit(resting, sink);
        state.ResumeTiming();
        
        book.Cancel("REST", 2, sink);
    }
}
BENCHMARK(BM_Cancel);
