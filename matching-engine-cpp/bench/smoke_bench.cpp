#include <benchmark/benchmark.h>

#include "engine/version.h"

static void BM_Noop(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize(engine::kVersionMajor);
    }
}
BENCHMARK(BM_Noop);
