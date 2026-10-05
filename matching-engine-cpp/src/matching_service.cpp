#include "engine/matching_service.h"
#include <iostream>
#include <cstring>
#include <chrono>

namespace engine {

// Simple FNV-1a hash for orderId
inline uint64_t HashOrderId(const char* str, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<uint64_t>(str[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

MatchingService::MatchingService(Shard& shard, size_t max_inflight)
    : shard_(shard), reply_slots_(max_inflight) {
}

MatchingService::~MatchingService() {
    StopReplyThread();
}

void MatchingService::StartReplyThread() {
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true)) {
        reply_thread_ = std::thread(&MatchingService::ReplyLoop, this);
    }
}

void MatchingService::StopReplyThread() {
    if (running_.exchange(false)) {
        if (reply_thread_.joinable()) reply_thread_.join();
    }
}

uint32_t MatchingService::AllocateSlot() {
    // Simple linear scan from a rolling index.
    // In a high-perf scenario, use a lock-free free-list.
    uint32_t start = next_slot_.load(std::memory_order_relaxed);
    uint32_t i = start;
    do {
        bool expected = false;
        if (reply_slots_[i].in_use.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
            next_slot_.store((i + 1) % reply_slots_.size(), std::memory_order_relaxed);
            return i;
        }
        i = (i + 1) % reply_slots_.size();
    } while (i != start);
    
    return static_cast<uint32_t>(-1); // Exhausted
}

void MatchingService::FreeSlot(uint32_t slot_idx) {
    reply_slots_[slot_idx].reactor = nullptr;
    reply_slots_[slot_idx].submit_resp = nullptr;
    reply_slots_[slot_idx].cancel_resp = nullptr;
    reply_slots_[slot_idx].in_use.store(false, std::memory_order_release);
}

void MatchingService::ReplyLoop() {
    RuntimeReply reply;
    while (running_.load(std::memory_order_relaxed)) {
        if (shard_.GetReplyRing().try_pop(reply)) {
            uint32_t slot_idx = reply.reply_handle;
            if (slot_idx < reply_slots_.size() && reply_slots_[slot_idx].in_use.load(std::memory_order_acquire)) {
                RpcReplySlot& slot = reply_slots_[slot_idx];
                
                if (slot.submit_resp) {
                    if (reply.result.status == OrderStatus::PENDING || reply.result.status == OrderStatus::PARTIALLY_FILLED) {
                        slot.submit_resp->set_status(trade::matching::OrderStatus::PARTIALLY_FILLED);
                    } else if (reply.result.status == OrderStatus::EXECUTED) {
                        slot.submit_resp->set_status(trade::matching::OrderStatus::FULLY_FILLED);
                    } else if (reply.result.status == OrderStatus::REJECTED) {
                        slot.submit_resp->set_status(trade::matching::OrderStatus::REJECTED);
                        slot.submit_resp->set_reject_reason(reply.result.reject_reason);
                    } else if (reply.result.status == OrderStatus::CANCELLED) {
                        slot.submit_resp->set_status(trade::matching::OrderStatus::CANCELLED);
                    }

                    slot.submit_resp->set_filled_qty(reply.result.filled_qty);
                    slot.submit_resp->set_remaining_qty(reply.result.remaining_qty);
                    // Add fills... (omitted for brevity, but mapped directly)
                    
                    slot.reactor->Finish(grpc::Status::OK);
                } else if (slot.cancel_resp) {
                    slot.cancel_resp->set_cancelled(reply.success);
                    slot.reactor->Finish(grpc::Status::OK);
                }
                
                FreeSlot(slot_idx);
            }
        } else {
            // Backoff/yield logic (simplified for the reply thread)
            std::this_thread::yield();
        }
    }
}

grpc::ServerUnaryReactor* MatchingService::SubmitOrder(
    grpc::CallbackServerContext* context,
    const trade::matching::SubmitOrderRequest* request,
    trade::matching::SubmitOrderResponse* response) 
{
    grpc::ServerUnaryReactor* reactor = context->DefaultReactor();

    if (context->IsCancelled()) {
        reactor->Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Call cancelled"));
        return reactor;
    }

    if (context->deadline() < std::chrono::system_clock::now()) {
        reactor->Finish(grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Deadline expired before enqueue"));
        return reactor;
    }

    // Check x-correlation-id
    auto meta = context->client_metadata();
    auto it = meta.find("x-correlation-id");
    if (it != meta.end()) {
        // Log off-hot-path, but since we are in the gRPC thread, we can push to a logging queue.
        // std::string correlation_id(it->second.data(), it->second.length());
    }

    if (request->order_id().empty() || request->order_id().size() > 47) {
        reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid order_id"));
        return reactor;
    }

    uint32_t slot = AllocateSlot();
    if (slot == static_cast<uint32_t>(-1)) {
        reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many inflight requests"));
        return reactor;
    }

    reply_slots_[slot].reactor = reactor;
    reply_slots_[slot].submit_resp = response;

    RuntimeCmd cmd{};
    cmd.is_cancel = 0;
    cmd.symbol_idx = 0; // In a real system, map request->symbol() to idx
    std::memcpy(cmd.order_id, request->order_id().data(), request->order_id().size());
    cmd.order_id[request->order_id().size()] = '\0';
    cmd.order_id_hash = HashOrderId(cmd.order_id, request->order_id().size());
    
    std::memcpy(cmd.user_id, request->user_id().data(), request->user_id().size());
    cmd.user_id[request->user_id().size()] = '\0';

    cmd.side = (request->side() == trade::matching::Side::BUY) ? Side::BUY : Side::SELL;
    
    switch (request->order_type()) {
        case trade::matching::OrderType::LIMIT: cmd.type = OrderType::LIMIT; break;
        case trade::matching::OrderType::MARKET: cmd.type = OrderType::MARKET; break;
        case trade::matching::OrderType::IOC: cmd.type = OrderType::IOC; break;
        case trade::matching::OrderType::FOK: cmd.type = OrderType::FOK; break;
        default: cmd.type = OrderType::LIMIT; break;
    }

    cmd.price = request->price_paise();
    cmd.qty = request->quantity();
    cmd.reply_handle = slot;
    cmd.ingress_ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();

    if (!shard_.TryEnqueueCmd(cmd)) {
        FreeSlot(slot);
        reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Shard queue full"));
        return reactor;
    }

    return reactor; // Reactor stays alive until ReplyLoop calls Finish()
}

grpc::ServerUnaryReactor* MatchingService::CancelOrder(
    grpc::CallbackServerContext* context,
    const trade::matching::CancelOrderRequest* request,
    trade::matching::CancelOrderResponse* response)
{
    grpc::ServerUnaryReactor* reactor = context->DefaultReactor();

    if (context->IsCancelled()) {
        reactor->Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Call cancelled"));
        return reactor;
    }

    uint32_t slot = AllocateSlot();
    if (slot == static_cast<uint32_t>(-1)) {
        reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many inflight requests"));
        return reactor;
    }

    reply_slots_[slot].reactor = reactor;
    reply_slots_[slot].cancel_resp = response;

    RuntimeCmd cmd{};
    cmd.is_cancel = 1;
    cmd.symbol_idx = 0;
    std::memcpy(cmd.order_id, request->order_id().data(), request->order_id().size());
    cmd.order_id[request->order_id().size()] = '\0';
    cmd.reply_handle = slot;

    if (!shard_.TryEnqueueCmd(cmd)) {
        FreeSlot(slot);
        reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Shard queue full"));
        return reactor;
    }

    return reactor;
}

grpc::ServerUnaryReactor* MatchingService::GetDepth(
    grpc::CallbackServerContext* context,
    const trade::matching::DepthRequest* request,
    trade::matching::DepthResponse* response)
{
    grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
    
    // Depth read is synchronous from the snapshot ring (cached locally by Shard)
    auto snap = shard_.GetDepth(0); // Map symbol to idx

    response->set_symbol(request->symbol());
    for (uint32_t i = 0; i < snap.bid_count; ++i) {
        auto* level = response->add_bids();
        level->set_price_paise(snap.bids[i].price);
        level->set_quantity(snap.bids[i].qty);
        level->set_order_count(snap.bids[i].order_count);
    }
    for (uint32_t i = 0; i < snap.ask_count; ++i) {
        auto* level = response->add_asks();
        level->set_price_paise(snap.asks[i].price);
        level->set_quantity(snap.asks[i].qty);
        level->set_order_count(snap.asks[i].order_count);
    }

    reactor->Finish(grpc::Status::OK);
    return reactor;
}

} // namespace engine
