#include "engine/matching_service.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

std::atomic<bool> shutdown_requested{false};

void HandleSignal(int) {
    shutdown_requested.store(true, std::memory_order_relaxed);
}

std::string GetListenAddress() {
    const char* address = std::getenv("MATCHING_ENGINE_CPP_ADDRESS");
    return address == nullptr || *address == '\0' ? "0.0.0.0:50051" : address;
}

}  // namespace

int main() {
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    engine::Shard shard({.shard_id = 0});
    shard.AddSymbol(0, {
        .tick_size_paise = 1,
        .reference_price_paise = 10000,
        .band_percent = 0.30,
        .max_live_orders = 100000,
        .dedupe_window_size = 65536,
    });
    shard.Start();

    engine::MatchingService service(shard);
    service.StartReplyThread();

    grpc::ServerBuilder builder;
    const std::string address = GetListenAddress();
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "Failed to start C++ matching engine gRPC server at " << address << '\n';
        service.StopReplyThread();
        shard.Stop();
        return EXIT_FAILURE;
    }

    std::cout << "C++ matching engine listening at " << address << std::endl;
    while (!shutdown_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server->Shutdown();
    service.StopReplyThread();
    shard.Stop();
    return EXIT_SUCCESS;
}
