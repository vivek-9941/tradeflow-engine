#pragma once

#include "engine/types.h"
#include "engine/order_pool.h"
#include "engine/order_index.h"
#include "engine/price_level.h"
#include "engine/dedupe_window.h"
#include <vector>
#include <string_view>

namespace engine {

class OrderBook {
public:
    explicit OrderBook(const OrderBookConfig& config);

    MatchResult Submit(const NewOrderCmd& cmd, EventSink& sink);
    bool Cancel(std::string_view order_id, uint64_t ts_ns, EventSink& sink);

private:
    // Core helpers
    uint32_t PriceToIndex(Price price) const;
    Price IndexToPrice(uint32_t index) const;

    void MatchBuy(const NewOrderCmd& cmd, MatchResult& result, EventSink& sink);
    void MatchSell(const NewOrderCmd& cmd, MatchResult& result, EventSink& sink);

    void AddToBook(Handle handle);
    void RemoveFromBook(Handle handle);
    void ReduceRestingOrder(Handle handle, Qty filled_qty, EventSink& sink);

    void FormatTradeId(uint64_t seq, char* out_buf, size_t out_len) const;

    OrderBookConfig config_;
    Price min_price_;
    Price max_price_;
    uint32_t num_levels_;

    OrderPool pool_;
    OrderIndex index_;
    ZeroAllocDedupeWindow dedupe_;

    std::vector<PriceLevel> buy_levels_;
    std::vector<PriceLevel> sell_levels_;

    HierarchicalBitmap buy_bitmap_;
    HierarchicalBitmap sell_bitmap_;

    uint32_t active_orders_ = 0;
};

}  // namespace engine
