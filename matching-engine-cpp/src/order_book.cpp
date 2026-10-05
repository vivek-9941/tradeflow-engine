#include "engine/order_book.h"
#include <charconv>
#include <algorithm>

namespace engine {

OrderBook::OrderBook(const OrderBookConfig& config)
    : config_(config),
      pool_(config.max_live_orders),
      index_(config.max_live_orders),
      dedupe_(config.dedupe_window_size),
      buy_bitmap_(4096),
      sell_bitmap_(4096) {

    Price band = (config.reference_price_paise * config.band_percent);
    min_price_ = config.reference_price_paise - band;
    max_price_ = config.reference_price_paise + band;
    
    // Ensure min_price and max_price are on tick
    min_price_ = (min_price_ / config.tick_size_paise) * config.tick_size_paise;
    max_price_ = (max_price_ / config.tick_size_paise) * config.tick_size_paise;

    num_levels_ = (max_price_ - min_price_) / config.tick_size_paise + 1;
    if (num_levels_ > 4096) {
        throw std::invalid_argument("Levels exceed bitmap capacity (4096)");
    }

    buy_levels_.resize(num_levels_);
    sell_levels_.resize(num_levels_);
}

uint32_t OrderBook::PriceToIndex(Price price) const {
    return (price - min_price_) / config_.tick_size_paise;
}

Price OrderBook::IndexToPrice(uint32_t index) const {
    return min_price_ + index * config_.tick_size_paise;
}

MatchResult OrderBook::Submit(const NewOrderCmd& cmd, EventSink& sink) {
    MatchResult result;
    
    if (dedupe_.TryLookup(cmd.orderId, result)) {
        return result; // Duplicate
    }

    if (cmd.qty <= 0) {
        result.reject_reason = "INVALID_QTY";
        return result;
    }
    if (cmd.price < min_price_ || cmd.price > max_price_) {
        result.reject_reason = "PRICE_OUT_OF_BANDS";
        return result;
    }
    if ((cmd.price - min_price_) % config_.tick_size_paise != 0) {
        result.reject_reason = "OFF_TICK_PRICE";
        return result;
    }
    if (active_orders_ >= config_.max_live_orders) {
        result.reject_reason = "REJECT_BOOK_FULL";
        return result;
    }
    
    result.remaining_qty = cmd.qty;
    result.filled_qty = 0;
    
    if (cmd.side == Side::BUY) {
        MatchBuy(cmd, result, sink);
    } else if (cmd.side == Side::SELL) {
        MatchSell(cmd, result, sink);
    } else {
        result.reject_reason = "UNKNOWN_SIDE";
        return result;
    }

    if (result.remaining_qty > 0) {
        if (cmd.type == OrderType::IOC) {
            result.status = result.filled_qty > 0 ? OrderStatus::PARTIALLY_FILLED : OrderStatus::CANCELLED;
            result.remainder_cancelled = true;
            sink.OnCancel(cmd.orderId, cmd.userId, cmd.ts_ns, result.remaining_qty);
        } else {
            // LIMIT or GTD -> Rests in book
            result.status = result.filled_qty > 0 ? OrderStatus::PARTIALLY_FILLED : OrderStatus::PENDING;
            
            Handle handle = pool_.Allocate();
            if (handle == kInvalidHandle) {
                // Should not happen as we checked active_orders_ < capacity
                result.reject_reason = "REJECT_BOOK_FULL";
                return result;
            }
            
            HotOrder& hot = pool_.GetHot(handle);
            hot.price_index = PriceToIndex(cmd.price);
            hot.remaining_qty = result.remaining_qty;
            hot.side = cmd.side;
            hot.type = cmd.type;
            hot.status = result.status;
            hot.self_trade_prevention = cmd.self_trade_prevention;
            
            ColdOrder& cold = pool_.GetCold(handle);
            cold.set_order_id(cmd.orderId);
            cold.set_user_id(cmd.userId);
            cold.ts_ns = cmd.ts_ns;
            cold.seq = cmd.seq;
            
            index_.Insert(cmd.orderId, handle);
            AddToBook(handle);
            active_orders_++;
        }
    } else {
        result.status = OrderStatus::FULLY_FILLED;
    }

    dedupe_.Add(cmd.orderId, result);
    return result;
}

void OrderBook::FormatTradeId(uint64_t seq, char* out_buf, size_t out_len) const {
    // TRD-<shard>-<seq> -> For simplicity, using config_.symbol_idx or just TRD-S-seq
    // Since we don't store shard name, just use TRD-0-seq
    std::memset(out_buf, 0, out_len);
    snprintf(out_buf, out_len, "TRD-0-%llu", (unsigned long long)seq);
}

void OrderBook::MatchBuy(const NewOrderCmd& cmd, MatchResult& result, EventSink& sink) {
    while (result.remaining_qty > 0) {
        int32_t best_ask_idx = sell_bitmap_.FindLowest();
        if (best_ask_idx < 0) break;
        
        Price resting_price = IndexToPrice(best_ask_idx);
        if (resting_price > cmd.price) break;

        PriceLevel& level = sell_levels_[best_ask_idx];
        Handle resting_handle = level.head;
        
        while (resting_handle != kInvalidHandle && result.remaining_qty > 0) {
            HotOrder& resting_hot = pool_.GetHot(resting_handle);
            ColdOrder& resting_cold = pool_.GetCold(resting_handle);
            
            if (cmd.self_trade_prevention && resting_cold.get_user_id() == cmd.userId) {
                // STP policy: Cancel resting order
                Handle to_cancel = resting_handle;
                resting_handle = resting_hot.next_handle; // Advance before removing
                Qty cancel_qty = pool_.GetHot(to_cancel).remaining_qty;
                sink.OnCancel(pool_.GetCold(to_cancel).get_order_id(), pool_.GetCold(to_cancel).get_user_id(), cmd.ts_ns, cancel_qty);
                RemoveFromBook(to_cancel);
                index_.EraseByHash(pool_.GetCold(to_cancel).get_order_id(), to_cancel);
                pool_.Free(to_cancel);
                active_orders_--;
                continue;
            }

            Qty fill_qty = std::min(result.remaining_qty, resting_hot.remaining_qty);
            
            Fill fill;
            fill.trade_seq = cmd.seq;
            fill.buy_orderId = cmd.orderId;
            fill.sell_orderId = resting_cold.get_order_id();
            fill.buyer = cmd.userId;
            fill.seller = resting_cold.get_user_id();
            fill.price = resting_price;
            fill.qty = fill_qty;
            fill.ts_ns = cmd.ts_ns;
            
            sink.OnTrade(fill);
            
            if (result.fill_count < kMaxFills) {
                result.fills[result.fill_count++] = fill;
            } else {
                result.fills_truncated = true;
            }
            
            result.remaining_qty -= fill_qty;
            result.filled_qty += fill_qty;
            
            Handle next_handle = resting_hot.next_handle;
            ReduceRestingOrder(resting_handle, fill_qty, sink);
            resting_handle = next_handle;
        }
    }
}

void OrderBook::MatchSell(const NewOrderCmd& cmd, MatchResult& result, EventSink& sink) {
    while (result.remaining_qty > 0) {
        int32_t best_bid_idx = buy_bitmap_.FindHighest();
        if (best_bid_idx < 0) break;
        
        Price resting_price = IndexToPrice(best_bid_idx);
        if (resting_price < cmd.price) break;

        PriceLevel& level = buy_levels_[best_bid_idx];
        Handle resting_handle = level.head;
        
        while (resting_handle != kInvalidHandle && result.remaining_qty > 0) {
            HotOrder& resting_hot = pool_.GetHot(resting_handle);
            ColdOrder& resting_cold = pool_.GetCold(resting_handle);
            
            if (cmd.self_trade_prevention && resting_cold.get_user_id() == cmd.userId) {
                // STP policy: Cancel resting order
                Handle to_cancel = resting_handle;
                resting_handle = resting_hot.next_handle;
                Qty cancel_qty = pool_.GetHot(to_cancel).remaining_qty;
                sink.OnCancel(pool_.GetCold(to_cancel).get_order_id(), pool_.GetCold(to_cancel).get_user_id(), cmd.ts_ns, cancel_qty);
                RemoveFromBook(to_cancel);
                index_.EraseByHash(pool_.GetCold(to_cancel).get_order_id(), to_cancel);
                pool_.Free(to_cancel);
                active_orders_--;
                continue;
            }

            Qty fill_qty = std::min(result.remaining_qty, resting_hot.remaining_qty);
            
            Fill fill;
            fill.trade_seq = cmd.seq;
            fill.buy_orderId = resting_cold.get_order_id();
            fill.sell_orderId = cmd.orderId;
            fill.buyer = resting_cold.get_user_id();
            fill.seller = cmd.userId;
            fill.price = resting_price;
            fill.qty = fill_qty;
            fill.ts_ns = cmd.ts_ns;
            
            sink.OnTrade(fill);
            
            if (result.fill_count < kMaxFills) {
                result.fills[result.fill_count++] = fill;
            } else {
                result.fills_truncated = true;
            }
            
            result.remaining_qty -= fill_qty;
            result.filled_qty += fill_qty;
            
            Handle next_handle = resting_hot.next_handle;
            ReduceRestingOrder(resting_handle, fill_qty, sink);
            resting_handle = next_handle;
        }
    }
}

void OrderBook::AddToBook(Handle handle) {
    HotOrder& hot = pool_.GetHot(handle);
    uint32_t idx = hot.price_index;
    
    PriceLevel* level = nullptr;
    if (hot.side == Side::BUY) {
        level = &buy_levels_[idx];
        buy_bitmap_.Set(idx);
    } else {
        level = &sell_levels_[idx];
        sell_bitmap_.Set(idx);
    }
    
    hot.prev_handle = level->tail;
    hot.next_handle = kInvalidHandle;
    
    if (level->tail != kInvalidHandle) {
        pool_.GetHot(level->tail).next_handle = handle;
    } else {
        level->head = handle;
    }
    level->tail = handle;
    
    level->total_qty += hot.remaining_qty;
    level->order_count++;
}

void OrderBook::RemoveFromBook(Handle handle) {
    HotOrder& hot = pool_.GetHot(handle);
    uint32_t idx = hot.price_index;
    
    PriceLevel* level = nullptr;
    if (hot.side == Side::BUY) {
        level = &buy_levels_[idx];
    } else {
        level = &sell_levels_[idx];
    }
    
    if (hot.prev_handle != kInvalidHandle) {
        pool_.GetHot(hot.prev_handle).next_handle = hot.next_handle;
    } else {
        level->head = hot.next_handle;
    }
    
    if (hot.next_handle != kInvalidHandle) {
        pool_.GetHot(hot.next_handle).prev_handle = hot.prev_handle;
    } else {
        level->tail = hot.prev_handle;
    }
    
    level->total_qty -= hot.remaining_qty;
    level->order_count--;
    
    if (level->order_count == 0) {
        if (hot.side == Side::BUY) {
            buy_bitmap_.Clear(idx);
        } else {
            sell_bitmap_.Clear(idx);
        }
    }
}

void OrderBook::ReduceRestingOrder(Handle handle, Qty filled_qty, EventSink& sink) {
    HotOrder& hot = pool_.GetHot(handle);
    uint32_t idx = hot.price_index;
    
    PriceLevel* level = nullptr;
    if (hot.side == Side::BUY) {
        level = &buy_levels_[idx];
    } else {
        level = &sell_levels_[idx];
    }
    
    hot.remaining_qty -= filled_qty;
    level->total_qty -= filled_qty;
    
    if (hot.remaining_qty == 0) {
        hot.status = OrderStatus::FULLY_FILLED;
        // The remaining_qty has already been reduced in level, temporarily restore it for RemoveFromBook math
        hot.remaining_qty = filled_qty; // trick to make RemoveFromBook math work out
        level->total_qty += filled_qty;
        
        RemoveFromBook(handle);
        index_.EraseByHash(pool_.GetCold(handle).get_order_id(), handle);
        pool_.Free(handle);
        active_orders_--;
    } else {
        hot.status = OrderStatus::PARTIALLY_FILLED;
    }
}

bool OrderBook::Cancel(std::string_view order_id, uint64_t ts_ns, EventSink& sink) {
    Handle handle = index_.FindByHash(order_id);
    if (handle == kInvalidHandle) {
        return false;
    }
    
    const ColdOrder& cold = pool_.GetCold(handle);
    if (cold.get_order_id() != order_id) {
        return false; // Hash collision not matching string
    }
    
    HotOrder& hot = pool_.GetHot(handle);
    hot.status = OrderStatus::CANCELLED;
    
    sink.OnCancel(order_id, cold.get_user_id(), ts_ns, hot.remaining_qty);
    
    RemoveFromBook(handle);
    index_.EraseByHash(order_id, handle);
    pool_.Free(handle);
    active_orders_--;
    
    return true;
}

}  // namespace engine
