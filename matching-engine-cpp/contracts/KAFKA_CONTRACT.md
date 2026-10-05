# Kafka Contract: C++ Matching Engine → Downstream Consumers

> **Source of truth**: This contract is derived by static analysis of the Java codebase
> on branch `main` at the time of this commit. Fields marked with "⚠️ NEEDS LIVE CAPTURE"
> must be verified by running the stack and consuming real messages before implementation.

---

## Topics Produced by the Matching Engine

| Topic              | Key                     | Value Type          | Producer Config          |
|--------------------|-------------------------|---------------------|--------------------------|
| `trade-executed`   | `order.getOrderId()`    | `TradeExecution`    | `JsonSerializer`, `acks=all`, `retries=3`, `idempotence=true` |
| `order-cancelled`  | `order.getOrderId()`    | `CancellationEvent` | `JsonSerializer`, `acks=all`, `retries=3`, `idempotence=true` |

- Topic partitions: 3 each (configured in `KafkaProducerConfig.java`)
- Replication factor: 1

---

## Message Schemas

### TradeExecution (topic: `trade-executed`)

Source: [`TradeExecution.java`](../common-module/src/main/java/org/vivek/commonmodule/model/TradeExecution.java)

```json
{
    "tradeId":       "TRD-<uuid>",
    "buyOrderId":    "ORD-<uuid>",
    "sellOrderId":   "ORD-<uuid>",
    "buyerId":       "U1",
    "sellerId":      "LP_BOOTSTRAP",
    "symbol":        "INFY",
    "quantity":      10.0,
    "executedPrice": 1776.775,
    "executedAt":    "2024-01-15T10:42:31.123456789Z"
}
```

| Field          | Java Type    | JSON Type  | Notes                                              |
|----------------|-------------|------------|-----------------------------------------------------|
| `tradeId`      | `String`     | `string`   | Format: `"TRD-" + UUID.randomUUID()`               |
| `buyOrderId`   | `String`     | `string`   | The orderId of the buy-side order                   |
| `sellOrderId`  | `String`     | `string`   | The orderId of the sell-side (resting) order        |
| `buyerId`      | `String`     | `string`   | userId of buyer                                     |
| `sellerId`     | `String`     | `string`   | userId of seller                                    |
| `symbol`       | `String`     | `string`   | Uppercase symbol (e.g. `"INFY"`)                    |
| `quantity`     | `double`     | `number`   | Fill quantity (may be partial)                      |
| `executedPrice`| `double`     | `number`   | Price of the resting order (NOT the incoming order) |
| `executedAt`   | `Instant`    | `string`   | ISO-8601 with nanoseconds: `Instant.now().toString()` → `"2024-01-15T10:42:31.123456789Z"` |

### CancellationEvent (topic: `order-cancelled`)

Source: [`CancellationEvent.java`](../common-module/src/main/java/org/vivek/commonmodule/model/CancellationEvent.java)

```json
{
    "orderId":     "ORD-<uuid>",
    "userId":      "U1",
    "symbol":      "INFY",
    "cancelledAt": "2024-01-15T10:42:31.123456789Z"
}
```

| Field         | Java Type   | JSON Type  | Notes                                             |
|---------------|------------|------------|---------------------------------------------------|
| `orderId`     | `String`    | `string`   | The cancelled order's ID                          |
| `userId`      | `String`    | `string`   | User who placed the order                         |
| `symbol`      | `String`    | `string`   | Uppercase symbol                                  |
| `cancelledAt` | `Instant`   | `string`   | ISO-8601 with nanoseconds: `Instant.now().toString()` |

### Kafka Message Key

- **`trade-executed`**: `order.getOrderId()` — the orderId of the **incoming** order
  (see `MatchingController.java` line 56: `tradeKafkaTemplate.send(TOPIC, order.getOrderId(), trade)`)
- **`order-cancelled`**: `order.getOrderId()` — the orderId of the cancelled order
  (see `MatchingController.java` line 107 and `SymbolOrderBook.java` line 298)

---

## Timestamp Format

`java.time.Instant.now().toString()` produces ISO-8601 format:
```
2024-01-15T10:42:31.123456789Z
```
- Always UTC (trailing `Z`)
- Nanosecond precision (9 decimal places)
- Jackson serializes `Instant` as this string by default with `JavaTimeModule`

⚠️ **NEEDS LIVE CAPTURE**: Confirm whether Spring Kafka's `JsonSerializer` uses Jackson's
`WRITE_DATES_AS_TIMESTAMPS` (epoch millis) or ISO-8601 string. The `JavaTimeModule` default
is ISO-8601 string, but this must be verified from actual messages.

---

## `__TypeId__` Header Analysis

### How Spring Kafka `JsonSerializer` works

Spring Kafka's `JsonSerializer` **adds a `__TypeId__` header by default** containing the
fully-qualified Java class name. This header tells the `JsonDeserializer` on the consumer
side which class to deserialize into.

### Producer side (matching-engine)

The `KafkaProducerConfig` in the matching engine uses `JsonSerializer` with **default settings**.
There is **no** `JsonSerializer.ADD_TYPE_INFO_HEADERS = false` configured. Therefore, the
producer **DOES add `__TypeId__` headers** to every message:

- `trade-executed` messages will have `__TypeId__: org.vivek.commonmodule.model.TradeExecution`
- `order-cancelled` messages will have `__TypeId__: org.vivek.commonmodule.model.CancellationEvent`

### Consumer side analysis

| Service              | Group ID             | `TRUSTED_PACKAGES`                  | `USE_TYPE_INFO_HEADERS` | `default.type`                               | **Depends on `__TypeId__`?** |
|----------------------|----------------------|--------------------------------------|--------------------------|----------------------------------------------|------------------------------|
| **ledger-service**   | `ledger-group`       | `"*"` (Java config)                 | not set (default=`true`) | not set                                      | **YES** — relies on `__TypeId__` to pick class |
| **risk-service**     | `risk-group`         | `"*"` (Java config)                 | not set (default=`true`) | not set                                      | **YES** — relies on `__TypeId__` to pick class |
| **margin-service**   | `margin-group`       | `"*"` (Java config)                 | not set (default=`true`) | not set                                      | **YES** — relies on `__TypeId__` to pick class |
| **notification-service** | `notification-group` | `"*"` (Java config)            | **`false`**              | not set                                      | **NO** — ignores `__TypeId__` |
| **analytics-service** | `analytics-group`   | `"*"` (Java config)                | **`false`**              | not set                                      | **NO** — ignores `__TypeId__` |
| **market-data-service** | `marketdata-group` | `"org.vivek.commonmodule.model"` (YAML) | not set (default=`true`) | `org.vivek.commonmodule.model.TradeExecution` | **PARTIAL** — has `default.type` fallback |

### Conclusion

**The `__TypeId__` header IS required** for correct deserialization by ledger-service,
risk-service, and margin-service. These three services have `TRUSTED_PACKAGES = "*"` and
default `USE_TYPE_INFO_HEADERS = true`, meaning they use the `__TypeId__` header to determine
which Java class to instantiate.

**The C++ engine MUST produce the `__TypeId__` header** with values:
- `org.vivek.commonmodule.model.TradeExecution` for `trade-executed` topic
- `org.vivek.commonmodule.model.CancellationEvent` for `order-cancelled` topic

Notification-service and analytics-service explicitly set `USE_TYPE_INFO_HEADERS = false` and
will deserialize purely based on JSON structure. Market-data-service has `default.type` set
as a fallback.

---

## Consumer Groups Summary

| Consumer Group       | Topics Consumed                   | Notes                        |
|----------------------|-----------------------------------|------------------------------|
| `ledger-group`       | `trade-executed`, `order-cancelled` | Idempotent via DB `ProcessedEvent` table |
| `risk-group`         | `trade-executed`                  | Position updates             |
| `margin-group`       | `trade-executed`, `order-cancelled` | Margin release               |
| `notification-group` | `trade-executed`, `order-cancelled` | WebSocket push               |
| `analytics-group`    | `trade-executed`, `*.DLT`         | Stats + DLQ monitoring       |
| `marketdata-group`   | `trade-executed`                  | LTP/VWAP update              |

---

## Golden Captures

⚠️ **NOT YET CAPTURED**: Golden Kafka messages (raw key, headers, value) must be captured
by running the full stack with Docker Compose, placing crossing orders, and consuming with
`kafka-console-consumer --print-key --print-headers --print-value`. These captures should
be saved to `contracts/golden/` once the stack is running.

Required captures:
1. `trade-executed` message from a LIMIT order full match
2. `trade-executed` message from a partial fill
3. `order-cancelled` message from an IOC remainder cancellation
4. `order-cancelled` message from a user-initiated cancel (DELETE endpoint)
