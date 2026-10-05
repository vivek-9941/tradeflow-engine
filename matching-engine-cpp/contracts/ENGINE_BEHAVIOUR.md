# Engine Behaviour Contract

> Derived from static analysis of the Java matching engine on branch `main`.
> File references are relative to the repository root.

---

## Core Matching Semantics

### Price-Time Priority

The order book enforces **price-time priority** (FIFO within each price level):

- **Buy side**: `ConcurrentSkipListMap` with `Comparator.reverseOrder()` — highest bid first
  ([SymbolOrderBook.java:32-33](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))
- **Sell side**: `ConcurrentSkipListMap` with natural ordering — lowest ask first
  ([SymbolOrderBook.java:36-37](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))
- Within a price level: `ConcurrentLinkedQueue<Order>` — FIFO insertion order
- Resting orders are consumed via `queue.peek()` → process → `queue.poll()` on fill

### Execution Price

**Execution always happens at the RESTING order's price**, not the incoming order's price.

- BUY incoming vs SELL resting → executes at `bestAsk` (the resting sell's price)
  ([SymbolOrderBook.java:168](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))
- SELL incoming vs BUY resting → executes at `bestBid` (the resting buy's price)
  ([SymbolOrderBook.java:202](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

### Matching Condition

```
BUY  matches when: bestAsk != null AND bestAsk <= incomingOrder.getPrice()
SELL matches when: bestBid != null AND bestBid >= incomingOrder.getPrice()
```
([SymbolOrderBook.java:56-58, 63-65](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

---

## Match Loop

Method: `SymbolOrderBook.match(Order incomingOrder)` — **synchronized** on the book instance.

```
remainingQty = incomingOrder.getQuantity()
while (remainingQty > EPSILON):
    if BUY:
        bestAsk = sellOrders.firstKey()
        if bestAsk == null || bestAsk > incomingOrder.price: break
        fillQty = min(remainingQty, restingOrder.quantity)
        create TradeExecution
        remainingQty -= fillQty
        reduce or remove resting order
    else (SELL):
        bestBid = buyOrders.firstKey()
        if bestBid == null || bestBid < incomingOrder.price: break
        fillQty = min(remainingQty, restingOrder.quantity)
        create TradeExecution
        remainingQty -= fillQty
        reduce or remove resting order

applyIncomingOrderState(incomingOrder, remainingQty, executions)
return executions
```

- `EPSILON = 1e-9` (used for floating-point zero comparisons)
  ([SymbolOrderBook.java:25](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))
- One incoming order can produce **multiple** `TradeExecution` events (one per price level consumed)

---

## Partial Fill Handling

### Resting Order Reduction

Method: `reduceRestingOrder()` ([SymbolOrderBook.java:307-323](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

- `updatedQty = restingOrder.quantity - fillQty`
- If `updatedQty <= EPSILON`: set status → `FULLY_FILLED`, remove from queue, remove price level if empty
- If `updatedQty > EPSILON`: set status → `PARTIALLY_FILLED`, order stays in queue with reduced quantity

### Incoming Order State After Matching

Method: `applyIncomingOrderState()` ([SymbolOrderBook.java:253-282](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

| Condition | Order Type | Status Set | Action |
|-----------|-----------|------------|--------|
| `remainingQty > EPSILON` | `IOC` | `PARTIALLY_FILLED` (if had fills) or `CANCELLED` (if no fills) | Publish `CancellationEvent`, do NOT add to book |
| `remainingQty > EPSILON` | `LIMIT` or `GTD` | `PARTIALLY_FILLED` (if had fills) or `PENDING` (if no fills) | Create remainder order via `withQuantity(remainingQty)`, add to book |
| `remainingQty <= EPSILON` | any | `FULLY_FILLED` (if had fills) | No further action |

---

## Order Types

| Type  | On Unmatched Remainder | Source |
|-------|------------------------|--------|
| `LIMIT` | Rests in book indefinitely | Default `OrderType` ([Order.java:23](../../../common-module/src/main/java/org/vivek/commonmodule/model/Order.java)) |
| `IOC` (Immediate or Cancel) | Remainder cancelled, `CancellationEvent` published | ([SymbolOrderBook.java:259-265](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java)) |
| `GTD` (Good Till Day) | Rests in book until 15:30 IST expiry by `OrderExpiryScheduler` | Expiry is handled by order-service, not the matching engine |

---

## IOC Cancellation Flow

When an IOC order has unfilled remainder ([SymbolOrderBook.java:259-265](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java)):

1. `incomingOrder.setQuantity(remainingQty)` — set to unfilled portion
2. Status → `PARTIALLY_FILLED` if there were fills, `CANCELLED` if zero fills
3. `publishCancellationEvent(incomingOrder, updatedAt)` — sends to `order-cancelled` topic
4. Order is **never added** to the book (function returns early)

The `CancellationEvent` is published via `cancellationKafkaTemplate.send()` with key = `order.getOrderId()`.

---

## Cancel Order (User-Initiated)

Method: `cancel(String orderId)` → `cancelOrder(String orderId)` ([SymbolOrderBook.java:76-86](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

- **O(n) scan**: Iterates all price levels on both buy and sell sides
- Searches by `orderId.equals(order.getOrderId())`
- If found: removes from queue, sets status → `CANCELLED`, sets `updatedAt`
- Returns `null` if not found (cancel returns `false`)
- The `CancellationEvent` for user-initiated cancels is published by `MatchingController.cancel()`,
  NOT by `SymbolOrderBook` ([MatchingController.java:97-118](../../../matching-engine/src/main/java/org/vivek/matchingengine/controller/MatchingController.java))

---

## Trade ID Generation

```java
"TRD-" + UUID.randomUUID()
```
([SymbolOrderBook.java:372](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/SymbolOrderBook.java))

- Format: `TRD-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Non-deterministic (UUID v4 random)
- **C++ engine will change to deterministic**: `TRD-<shard>-<seq>` per the design document

---

## REST API Contract

### POST /api/v1/match

Request body: `Order` (full JSON)
Response body ([MatchingController.java:46-51](../../../matching-engine/src/main/java/org/vivek/matchingengine/controller/MatchingController.java)):

```json
{
    "matched":      true,
    "fillCount":    2,
    "totalFilled":  15.0,
    "remainingQty": 5.0,
    "executions":   [ /* TradeExecution objects */ ]
}
```

Note: `MatchingEngineResponse` in order-service ([MatchingEngineResponse.java](../../../order-service/src/main/java/org/vivek/order/client/MatchingEngineResponse.java))
only reads `matched`, `fillCount`, `totalFilled`, `remainingQty` (ignores `executions`).

### DELETE /api/v1/orders/{orderId}?symbol={symbol}

Response:
```json
{ "cancelled": true }
```

### GET /api/v1/orderbook/{symbol}

Response: `BookSnapshot` object.

### GET /api/v1/orderbook

Response: List of all `BookSnapshot` objects.

### GET /api/v1/orderbook/{symbol}/depth

Response: `OrderBookDepth` object.

---

## Order Status Values

Enum: `OrderStatus` ([OrderStatus.java](../../../common-module/src/main/java/org/vivek/commonmodule/model/OrderStatus.java))

```
PENDING, VALIDATING, APPROVED, REJECTED, ROUTED,
PARTIALLY_FILLED, FULLY_FILLED, CANCELLED, EXPIRED,
EXECUTED, FAILED
```

Status values set by the matching engine:
- `PENDING` — set on remainder of LIMIT/GTD that had no fills
- `PARTIALLY_FILLED` — set on resting order with reduced quantity, or IOC with fills and remainder
- `FULLY_FILLED` — set on fully consumed resting order, or fully filled incoming order
- `CANCELLED` — set on cancelled order (user-initiated or IOC with zero fills)

Status values set by order-service after receiving `MatchingEngineResponse`:
- `EXECUTED` — when `remainingQty <= EPSILON && totalFilled > EPSILON`
- `PARTIALLY_FILLED` → `CANCELLED` — IOC with partial fill
- `CANCELLED` — IOC with no fill
- `PENDING` — LIMIT/GTD with no fill (resting)

---

## Concurrency Model

- `match()`, `cancel()`, `cancelOrder()`, `snapshot()`, `depth()`, `addRestingOrder()` are all
  **`synchronized`** on the `SymbolOrderBook` instance
- Each symbol has its own `SymbolOrderBook` — no cross-symbol locking
- `OrderBookRegistry` uses `ConcurrentHashMap<String, SymbolOrderBook>` for symbol → book mapping
- Book creation is via `computeIfAbsent` (lazy, thread-safe)

---

## Bootstrap Liquidity

On startup, `OrderBookRegistry.init()` seeds 10 synthetic SELL orders per symbol
([OrderBookRegistry.java:68-96](../../../matching-engine/src/main/java/org/vivek/matchingengine/orderbook/OrderBookRegistry.java)):

| Symbol   | Base Price | SELL prices (base × 1.001 to 1.010) |
|----------|-----------|--------------------------------------|
| INFY     | 1775.0    | 1776.775 .. 1792.75                 |
| TCS      | 3725.0    | 3728.725 .. 3762.25                 |
| RELIANCE | 2920.0    | 2922.92 .. 2949.2                   |
| HDFC     | 1650.0    | 1651.65 .. 1666.5                   |

- User: `LP_BOOTSTRAP`
- Order IDs: `LP-{SYMBOL}-{1..10}`
- Quantities: 11, 12, 13, ..., 20

---

## Deliberate Differences (C++ vs Java)

The C++ engine is designed for absolute minimum latency and predictability. The following deliberate deviations from the Java engine's behaviour have been implemented:

1. **Integer Arithmetic**: 
   - All prices and quantities are integer types (`int64_t`). Prices are represented in paise (1 INR = 100 paise), and quantities in whole shares.
   - This removes floating-point inaccuracies and the need for `EPSILON` comparisons, eliminating the risk of sub-penny stranding.

2. **Deterministic Trade IDs**:
   - The Java engine uses `UUID.randomUUID()` to generate non-deterministic trade IDs. 
   - The C++ engine uses deterministic IDs generated from the format `TRD-<shard>-<seq>`, making replays deterministic and enabling robust deduplication down the line.

3. **Explicit Rejections**:
   - The C++ engine will explicitly reject orders (without throwing exceptions) via a `REJECTED` status if:
     - The price is outside the configured bands (e.g., +/- 30% from a reference price).
     - The price does not align with the `tick_size_paise`.
     - The quantity is `<= 0`.
     - The symbol's active order capacity is exhausted (`REJECT_BOOK_FULL`), to prevent unbounded growth.

4. **In-Engine Deduplication**:
   - To handle idempotent retries from `order-service` without blowing up latency, the engine implements a zero-allocation, fixed-size deduplication window on the hot path. 
   - If a duplicate `orderId` is submitted, it returns the final state (filled, remaining, status) of the previously processed instance. The Java engine had no matching-tier deduplication, relying purely on downstream DB constraints.

5. **Self-Trade Prevention (STP)**:
   - An optional Self-Trade Prevention flag can be passed in `NewOrderCmd`. If enabled, when an incoming order is about to match with a resting order owned by the *same* user, the resting order is cancelled instead. The Java engine did not have this feature.
