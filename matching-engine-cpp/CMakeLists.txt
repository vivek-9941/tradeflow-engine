cmake_minimum_required(VERSION 3.20)
project(matching-engine-cpp
    VERSION   0.1.0
    LANGUAGES CXX
)

# ── C++20 ────────────────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Optional: -march=native for benchmarks on the local machine ──────────────
option(ENABLE_NATIVE_ARCH "Add -march=native (NOT portable, don't use in Docker)" OFF)

# ── Common flags ─────────────────────────────────────────────────────────────
add_compile_options(-Wall -Wextra -Wpedantic -Werror)

# ── Release: LTO + frame pointers for profiling ─────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    include(CheckIPOSupported)
    check_ipo_supported(RESULT ipo_supported OUTPUT ipo_error)
    if(ipo_supported)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    else()
        message(WARNING "LTO not supported: ${ipo_error}")
    endif()
    add_compile_options(-O3 -fno-omit-frame-pointer)
endif()

if(ENABLE_NATIVE_ARCH)
    add_compile_options(-march=native)
endif()

# ── vcpkg integration ───────────────────────────────────────────────────────
# vcpkg is wired via CMAKE_TOOLCHAIN_FILE in CMakePresets.json
find_package(GTest CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)

# ── Source library ───────────────────────────────────────────────────────────
add_library(engine_lib STATIC src/order_book.cpp src/shard.cpp)
target_include_directories(engine_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# ── Main executable ─────────────────────────────────────────────────────────
add_executable(matching_engine src/main.cpp)
target_link_libraries(matching_engine PRIVATE engine_lib)

# ── Tests ────────────────────────────────────────────────────────────────────
enable_testing()
add_executable(engine_tests tests/smoke_test.cpp)
target_link_libraries(engine_tests PRIVATE engine_lib GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(engine_tests)

# ── Benchmarks ───────────────────────────────────────────────────────────────
add_executable(engine_bench bench/smoke_bench.cpp)
target_link_libraries(engine_bench PRIVATE engine_lib benchmark::benchmark_main)

add_executable(workload_generator bench/workload_generator.cpp)
target_link_libraries(workload_generator PRIVATE engine_lib)

add_executable(latency_harness bench/latency_harness.cpp)
target_link_libraries(latency_harness PRIVATE engine_lib hdrhistogram-c::hdrhistogram-c)

