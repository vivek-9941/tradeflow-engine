#pragma once

#include <cstdint>
#include <string_view>
#include <array>

namespace engine {

using Price = int64_t;    // paise (rupees * 100)
using Qty = int64_t;      // whole shares
using Handle = uint32_t;  // index into the order pool

constexpr Handle kInvalidHandle = 0xFFFFFFFF;
constexpr size_t kMaxFills = 64;

enum class Side : uint8_t {
    BUY,
    SELL
};

enum class OrderType : uint8_t {
    LIMIT,
    IOC,
    GTD
};

enum class OrderStatus : uint8_t {
    PENDING,
    VALIDATING,
    APPROVED,
    REJECTED,
    ROUTED,
    PARTIALLY_FILLED,
    FULLY_FILLED,
    CANCELLED,
    EXPIRED,
    EXECUTED,
    FAILED
};

struct NewOrderCmd {
    uint32_t symbol_idx;
    std::string_view orderId;
    std::string_view userId;
    Side side;
    OrderType type;
    Price price;
    Qty qty;
    uint64_t ts_ns;
    uint64_t seq;
    bool self_trade_prevention = false; // default OFF
};

struct Fill {
    uint64_t trade_seq;
    std::string_view buy_orderId;
    std::string_view sell_orderId;
    std::string_view buyer;
    std::string_view seller;
    Price price;
    Qty qty;
    uint64_t ts_ns;
};

struct MatchResult {
    OrderStatus status = OrderStatus::REJECTED;
    Qty filled_qty = 0;
    Qty remaining_qty = 0;
    size_t fill_count = 0;
    std::array<Fill, kMaxFills> fills{};
    bool fills_truncated = false;
    bool remainder_cancelled = false;
    bool duplicate = false;
    std::string_view reject_reason;
};

class EventSink {
public:
    virtual ~EventSink() = default;
    virtual void OnTrade(const Fill& fill) = 0;
    virtual void OnCancel(std::string_view orderId, std::string_view userId, uint64_t ts_ns, Qty remaining_qty) = 0;
};

struct OrderBookConfig {
    Price tick_size_paise;
    Price reference_price_paise;
    double band_percent; // e.g., 0.30 for 30%
    uint32_t max_live_orders;
    uint32_t dedupe_window_size;
};

}  // namespace engine
