#pragma once

#include "matching.grpc.pb.h"
#include "engine/shard.h"
#include <grpcpp/grpcpp.h>
#include <vector>
#include <atomic>
#include <memory>
#include <string>

namespace engine {

// Pre-allocated slot for inflight gRPC calls
struct RpcReplySlot {
    std::atomic<bool> in_use{false};
    grpc::ServerUnaryReactor* reactor{nullptr};
    
    // We use a union or void* to support multiple response types, 
    // or just direct pointers since the reactor owns the response buffer lifecycle.
    trade::matching::SubmitOrderResponse* submit_resp{nullptr};
    trade::matching::CancelOrderResponse* cancel_resp{nullptr};
};

class MatchingService : public trade::matching::MatchingEngine::CallbackService {
public:
    MatchingService(Shard& shard, size_t max_inflight = 8192);
    ~MatchingService();

    // Starts the background thread that drains the shard's reply_ring_
    void StartReplyThread();
    void StopReplyThread();

    grpc::ServerUnaryReactor* SubmitOrder(
        grpc::CallbackServerContext* context,
        const trade::matching::SubmitOrderRequest* request,
        trade::matching::SubmitOrderResponse* response) override;

    grpc::ServerUnaryReactor* CancelOrder(
        grpc::CallbackServerContext* context,
        const trade::matching::CancelOrderRequest* request,
        trade::matching::CancelOrderResponse* response) override;

    grpc::ServerUnaryReactor* GetDepth(
        grpc::CallbackServerContext* context,
        const trade::matching::DepthRequest* request,
        trade::matching::DepthResponse* response) override;

private:
    uint32_t AllocateSlot();
    void FreeSlot(uint32_t slot_idx);
    void ReplyLoop();

    Shard& shard_;
    
    std::vector<RpcReplySlot> reply_slots_;
    std::atomic<uint32_t> next_slot_{0};
    
    std::atomic<bool> running_{false};
    std::thread reply_thread_;
};

} // namespace engine
