# 📊 KlineHub API Documentation

## Overview

KlineHub is a standalone market data service that provides:

* Real-time kline (candlestick) streaming
* Historical kline data
* Internal pair tracking system
* High-performance Redis + PostgreSQL storage

It is designed to:

* Reduce exchange API load
* Serve multiple projects
* Provide stable and reusable market data

---

# 🌐 Base URL

```
http://<host>:8088
```

---

# 🔐 API Types

| Type          | Description                             |
| ------------- | --------------------------------------- |
| `/api/*`      | Public API (for frontend / TradingView) |
| `/internal/*` | Internal API (management, pairs, stats) |
| `/ws/*`       | WebSocket real-time data                |

---

# 📡 1. Public API

## Get Klines (History)

### Endpoint

```
GET /api/klines
```

### Query Params

| Param    | Type   | Required | Example |
| -------- | ------ | -------- | ------- |
| exchange | string | ✅        | binance |
| market   | string | ✅        | futures |
| symbol   | string | ✅        | BTCUSDT |
| interval | string | ✅        | 1m      |
| limit    | int    | ❌        | 100     |

### Example

```
GET /api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1m&limit=10
```

### Response

```json
{
  "bars": [
    {
      "time": 1774379700000,
      "open": 69608.4,
      "high": 69638.3,
      "low": 69603.1,
      "close": 69612.3,
      "volume": 60.691
    }
  ],
  "noData": false
}
```

---

# ⚙️ 2. Internal API

## Health Check

```
GET /internal/health
```

### Response

```json
{
  "status": "ok",
  "redis": "ok",
  "db": "ok",
  "ws": "connected",
  "active_streams_count": 2,
  "tracked_pairs_total": 2,
  "candles_persisted_total": 13
}
```

---

## Stats

```
GET /internal/stats
```

---

## Get All Tracked Pairs

```
GET /internal/pairs
```

---

## Add / Track Pair

```
POST /internal/pairs
```

### Body

```json
{
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "backfill_limit": 300
}
```

---

## Pause Pair

```
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/pause
```

---

## Resume Pair

```
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/resume
```

---

## Delete Pair

```
DELETE /internal/pairs/{exchange}/{market}/{symbol}/{interval}
```

---

# 🔌 3. WebSocket API

## Endpoint

```
ws://<host>:8088/ws/market
```

---

## Connect

### First message

```json
{
  "type": "connected"
}
```

---

## Subscribe

```json
{
  "action": "subscribe",
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m"
}
```

---

## Unsubscribe

```json
{
  "action": "unsubscribe",
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m"
}
```

---

## Response

```json
{
  "type": "subscribed",
  "channel": "kline:binance:futures:BTCUSDT:1m"
}
```

---

## Real-time Kline Event

```json
{
  "type": "kline",
  "channel": "kline:binance:futures:BTCUSDT:1m",
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "is_closed": false,
  "bar": {
    "time": 1774379700000,
    "open": 69608.4,
    "high": 69638.3,
    "low": 69603.1,
    "close": 69612.3,
    "volume": 60.691
  }
}
```

---

# 🧠 Architecture Notes

* Uses **Binance WebSocket streams**
* Stores:

  * Open candles → Redis
  * Closed candles → PostgreSQL
* Backfill system ensures no missing candles
* One stream per symbol+interval (shared across all users)

---

# ⚡ Best Practices

* Do NOT request exchange API directly → use KlineHub
* Use:

  * `/api/klines` → history
  * `/ws/market` → realtime
* Track only needed pairs to reduce load

---

# 🚀 Future Extensions

* Multi-exchange support
* Aggregated intervals (1h → 4h, 1d)
* Price feed (for unrealized PnL)
* Alert system

---

# ✅ Summary

KlineHub is:

* Fast ⚡
* Scalable 📈
* Exchange-efficient 💰
* Reusable across projects 🔄

---
