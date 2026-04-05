# KlineHub API Documentation

## Overview

KlineHub is a standalone market data service that provides:

- Real-time kline streaming
- Historical kline data
- Internal pair tracking
- Redis + PostgreSQL storage

It is designed to:

- Reduce exchange API load
- Serve multiple projects
- Provide stable reusable market data

## Base URL

```
http://<host>:8088
```

---

## API Types

| Type | Description |
| --- | --- |
| `/api/*` | Public API for frontend / TradingView |
| `/internal/*` | Internal API for management and stats |
| `/ws/*` | WebSocket real-time data |

## Public API

### Get Klines

```
GET /api/klines
```

Supported intervals:

- `1m`
- `5m`
- `15m`
- `30m`
- `1h`
- `2h`
- `4h`
- `12h`
- `1d`
- `3d`
- `1w`
- `1M`

Behavior:

- If candles for the requested range already exist in PostgreSQL, the API returns them from DB.
- If candles are missing and `exchange=binance` with `market=spot` or `market=futures`, the API fetches the missing range directly from Binance and stores it in PostgreSQL.
- If the requested interval is not stored directly but can be built from a smaller stored interval, the API aggregates candles from DB and returns the completed bars only.

Query params:

| Param    | Type   | Required | Example |
| -------- | ------ | -------- | ------- |
| exchange | string | ✅        | binance |
| market   | string | ✅        | futures |
| symbol   | string | ✅        | BTCUSDT |
| interval | string | ✅        | 1m      |
| from     | int ms | ❌        | 1743465600000 |
| to       | int ms | ❌        | 1743552000000 |
| limit    | int    | ❌        | 100     |

Quick example:

```
GET /api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1m&limit=10
```

Response:

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

### Test requests

Fast smoke tests:

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1m&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=5m&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=15m&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=30m&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1h&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=2h&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=4h&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=12h&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1d&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=3d&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1w&limit=10"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1M&limit=10"
```

Spot examples:

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=spot&symbol=BTCUSDT&interval=1m&limit=20"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=spot&symbol=ETHUSDT&interval=1d&limit=30"
```

Range examples with explicit timestamps in milliseconds:

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1h&from=1743465600000&to=1743552000000"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1d&from=1740787200000&to=1743379200000"
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1M&from=1735689600000&to=1743465600000"
```

Pair tracking example before websocket / warm cache tests:

```bash
curl -X POST "http://127.0.0.1:8088/internal/pairs" \
  -H "Content-Type: application/json" \
  -d '{"exchange":"binance","market":"futures","symbol":"BTCUSDT","interval":"1m","backfill_limit":300}'
```

## Internal API

### Health Check

```
GET /internal/health
```

Response:

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

### Stats

```
GET /internal/stats
```

### Get All Tracked Pairs

```
GET /internal/pairs
```

### Add / Track Pair

```
POST /internal/pairs
```

Body:

```json
{
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "backfill_limit": 300
}
```

### Pause Pair

```
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/pause
```

### Resume Pair

```
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/resume
```

### Delete Pair

```
DELETE /internal/pairs/{exchange}/{market}/{symbol}/{interval}
```

## WebSocket API

Endpoint:

```
ws://<host>:8088/ws/market
```

### Connect

First message:

```json
{
  "type": "connected"
}
```

### Subscribe

```json
{
  "action": "subscribe",
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m"
}
```

### Unsubscribe

```json
{
  "action": "unsubscribe",
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1m"
}
```

### Response

```json
{
  "type": "subscribed",
  "channel": "kline:binance:futures:BTCUSDT:1m"
}
```

### Real-time Kline Event

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

## Architecture Notes

- Uses Binance WebSocket streams
- Stores:

  - Open candles -> Redis
  - Closed candles -> PostgreSQL
- Backfill loads missing candles from Binance when needed
- Aggregation can build larger candles from smaller intervals already stored in PostgreSQL
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
