# KlineHub API Reference

KlineHub is a standalone market data service for tracking instruments, streaming live candles, backfilling history, and serving normalized kline data over HTTP and WebSocket.

This document is the primary API reference for the current service implementation.

## 1. Overview

KlineHub provides:

- Historical kline API
- Live market WebSocket feed
- Internal pair management API
- Health and runtime diagnostics
- Persistent candle storage in PostgreSQL
- Open and recent candle cache in Redis

Primary use cases:

- Centralized market data source for multiple internal services
- Reduced direct exchange API usage
- Unified candle model across multiple providers
- Fast replay and warm-cache access for frontend and bots

## 2. Base URL

Default reverse-proxied URL:

```text
http://127.0.0.1:8088
```

Generic form:

```text
http://<host>:8088
```

WebSocket base:

```text
ws://<host>:8088
```

## 3. API Surface

| Prefix | Purpose |
| --- | --- |
| `/health` | Public lightweight health check |
| `/api/*` | Public candle history API |
| `/internal/*` | Internal management and runtime stats |
| `/ws/*` | Real-time WebSocket API |

## 4. Supported Providers

### 4.1 Exchanges and markets

| Exchange | Market values |
| --- | --- |
| `binance` | `spot`, `futures` |
| `bybit` | `spot`, `futures` |
| `oanda` | `forex`, `metals`, `stocks` |

### 4.2 Symbol format

| Exchange | Example symbols |
| --- | --- |
| Binance | `BTCUSDT`, `ETHUSDT` |
| Bybit | `BTCUSDT`, `ETHUSDT` |
| OANDA | `EUR_USD`, `XAU_USD`, `SPX500_USD` |

### 4.3 Supported query intervals

The HTTP kline API accepts the following normalized intervals:

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

### 4.4 Provider-specific notes

#### Binance

- Native exchange kline stream
- Direct backfill from exchange REST API
- Supports `spot` and `futures`

#### Bybit

- Native exchange kline stream
- Direct backfill from Bybit REST API
- Supports `spot` and `futures`
- Uses separate public WebSocket endpoints per market internally

#### OANDA

- No native exchange kline stream in this service
- Streaming is based on live pricing stream and candle construction in KlineHub
- Historical backfill is fetched from OANDA REST candles endpoint
- Canonical ingestion interval is `1m`
- Higher intervals are aggregated from stored `1m` candles

## 5. Data Model

Normalized candle payload returned by the public history API:

```json
{
  "time": 1774379700000,
  "open": 69608.4,
  "high": 69638.3,
  "low": 69603.1,
  "close": 69612.3,
  "volume": 60.691
}
```

Field meanings:

| Field | Type | Description |
| --- | --- | --- |
| `time` | integer | Candle open time in Unix milliseconds |
| `open` | number | Open price |
| `high` | number | High price |
| `low` | number | Low price |
| `close` | number | Close price |
| `volume` | number | Traded volume or provider volume equivalent |

## 6. Authentication

The currently implemented API routes shown in this document do not require request authentication by default.

If you later enable internal gateway protection or token validation at the proxy layer, document that separately for your deployment.

## 7. Public Endpoints

### 7.1 `GET /health`

Lightweight public service health endpoint.

#### Request

```http
GET /health
```

#### Response

```json
{
  "status": "ok",
  "service": "market-data-worker-api",
  "redis": "ok",
  "db": "ok"
}
```

#### Response fields

| Field | Type | Description |
| --- | --- | --- |
| `status` | string | Overall service state: `ok` or `degraded` |
| `service` | string | Service identifier |
| `redis` | string | Redis availability: `ok` or `down` |
| `db` | string | PostgreSQL availability: `ok` or `down` |

#### Example

```bash
curl http://127.0.0.1:8088/health
```

### 7.2 `GET /api/klines`

Returns normalized historical candles for a given exchange, market, symbol, and interval.

If candles are missing from local storage, KlineHub may fetch and persist the missing history before responding.

#### Request

```http
GET /api/klines
```

#### Query parameters

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `exchange` | string | Yes | Exchange identifier: `binance`, `bybit`, `oanda` |
| `market` | string | Yes | Market identifier for the exchange |
| `symbol` | string | Yes | Instrument symbol |
| `interval` | string | Yes | Requested normalized interval |
| `from` | integer ms | No | Start timestamp in Unix milliseconds |
| `to` | integer ms | No | End timestamp in Unix milliseconds |
| `limit` | integer | No | Maximum number of returned bars, `1..5000`, default `500` |

#### Behavior

- `exchange` is normalized to lowercase.
- `market` is normalized to lowercase.
- `symbol` is normalized to uppercase.
- Unsupported intervals return an empty payload with `noData=true`.
- If `to` is omitted, the API uses the current interval open time.
- If `from` is omitted, the API calculates a backward window based on `limit`.
- Closed historical candles are served from PostgreSQL.
- Missing historical ranges are backfilled from the provider and stored.
- If an open candle exists in Redis for the requested interval, it is appended or replaces the same timestamp in the response.

#### Aggregation behavior

- Binance and Bybit can provide direct native intervals for backfill.
- OANDA stores canonical `1m` candles and higher intervals are aggregated from them.
- If a smaller source interval already exists locally and can be aggregated to the target interval, KlineHub may serve aggregated data from stored candles.

#### Success response

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

#### Response fields

| Field | Type | Description |
| --- | --- | --- |
| `bars` | array | Array of normalized candle objects |
| `noData` | boolean | `true` when no candles are available |

#### Example: Binance futures

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=futures&symbol=BTCUSDT&interval=1h&limit=100"
```

#### Example: Binance spot

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=binance&market=spot&symbol=ETHUSDT&interval=1d&limit=30"
```

#### Example: Bybit futures

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=bybit&market=futures&symbol=BTCUSDT&interval=1h&limit=100"
```

#### Example: Bybit spot

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=bybit&market=spot&symbol=BTCUSDT&interval=1h&limit=100"
```

#### Example: OANDA forex

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=oanda&market=forex&symbol=EUR_USD&interval=1h&limit=100"
```

#### Example: OANDA metals

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=oanda&market=metals&symbol=XAU_USD&interval=1h&limit=100"
```

#### Example: explicit range

```bash
curl "http://127.0.0.1:8088/api/klines?exchange=bybit&market=futures&symbol=BTCUSDT&interval=1h&from=1710000000000&to=1710500000000&limit=200"
```

#### Empty response example

```json
{
  "bars": [],
  "noData": true
}
```

## 8. Internal Endpoints

Internal endpoints are used for runtime control, tracking, recovery, and observability.

### 8.1 `GET /internal/health`

Returns detailed runtime health, WebSocket state, and last event metadata.

#### Request

```http
GET /internal/health
```

#### Response example

```json
{
  "status": "ok",
  "redis": "ok",
  "db": "ok",
  "ws": "connected",
  "ws_connected": true,
  "ws_connecting": false,
  "ws_reconnect_count": 1,
  "active_streams_count": 2,
  "tracked_pairs_total": 2,
  "tracked_pairs_active": 2,
  "candles_persisted_total": 13,
  "ws_last_error": null,
  "ws_last_message_at": "2026-04-09T11:18:54.112345Z",
  "ws_connected_at": "2026-04-09T11:10:00.003210Z",
  "last_kline_event": {},
  "last_persisted_candle": {}
}
```

#### Response fields

| Field | Type | Description |
| --- | --- | --- |
| `status` | string | Overall internal health status |
| `redis` | string | Redis status |
| `db` | string | Database status |
| `ws` | string | WebSocket connectivity summary |
| `ws_connected` | boolean | Whether worker streams are connected |
| `ws_connecting` | boolean | Whether the stream manager is reconnecting |
| `ws_reconnect_count` | integer | Number of reconnect attempts since startup |
| `active_streams_count` | integer | Number of currently active tracked stream subscriptions |
| `tracked_pairs_total` | integer | Total tracked pairs |
| `tracked_pairs_active` | integer | Active tracked pairs |
| `candles_persisted_total` | integer | Count of persisted closed candles since runtime started |
| `ws_last_error` | string or null | Last stream-layer error |
| `ws_last_message_at` | datetime or null | Timestamp of last received market event |
| `ws_connected_at` | datetime or null | Timestamp when current stream session connected |
| `last_kline_event` | object or null | Last processed event payload |
| `last_persisted_candle` | object or null | Last persisted closed candle metadata |

#### Example

```bash
curl http://127.0.0.1:8088/internal/health
```

### 8.2 `GET /internal/stats`

Returns summarized runtime counters and storage availability.

#### Request

```http
GET /internal/stats
```

#### Response example

```json
{
  "tracked_pairs_total": 3,
  "tracked_pairs_active": 2,
  "tracked_pairs_paused": 1,
  "redis_ok": true,
  "db_ok": true,
  "active_streams_count": 2,
  "ws_connected": true,
  "ws_reconnect_count": 0,
  "candles_persisted_total": 5412
}
```

#### Example

```bash
curl http://127.0.0.1:8088/internal/stats
```

### 8.3 `GET /internal/pairs`

Returns all tracked pairs in the system.

#### Request

```http
GET /internal/pairs
```

#### Response example

```json
{
  "items": [
    {
      "id": 1,
      "exchange": "bybit",
      "market": "futures",
      "symbol": "BTCUSDT",
      "interval": "1h",
      "status": "active",
      "source": "api",
      "priority": 100,
      "created_at": "2026-04-09T11:00:00.000000",
      "updated_at": "2026-04-09T11:00:00.000000"
    }
  ],
  "count": 1
}
```

#### Example

```bash
curl http://127.0.0.1:8088/internal/pairs
```

### 8.4 `POST /internal/pairs`

Creates a new tracked pair or reactivates an existing one.

On success, KlineHub validates the instrument, runs recent backfill, and reloads the stream manager.

#### Request

```http
POST /internal/pairs
Content-Type: application/json
```

#### Body schema

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `exchange` | string | Yes | - | `binance`, `bybit`, `oanda` |
| `market` | string | Yes | - | Exchange market |
| `symbol` | string | Yes | - | Instrument symbol |
| `interval` | string | No | `1h` | Tracked interval |
| `source` | string | No | `api` | Source marker for bookkeeping |
| `priority` | integer | No | `100` | Internal priority field |
| `backfill_limit` | integer | No | `null` | Recent backfill size, max `1000` |

#### Request example: Bybit futures

```json
{
  "exchange": "bybit",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1h",
  "backfill_limit": 300
}
```

#### Request example: Bybit spot

```json
{
  "exchange": "bybit",
  "market": "spot",
  "symbol": "BTCUSDT",
  "interval": "1h",
  "backfill_limit": 300
}
```

#### Request example: OANDA forex

```json
{
  "exchange": "oanda",
  "market": "forex",
  "symbol": "EUR_USD",
  "interval": "1m",
  "backfill_limit": 300
}
```

#### OANDA constraint

Tracked OANDA pairs must use canonical ingestion interval `1m`.

#### Response example

```json
{
  "id": 3,
  "exchange": "bybit",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1h",
  "status": "active",
  "source": "api",
  "priority": 100,
  "created_at": "2026-04-09T11:30:00.000000",
  "updated_at": "2026-04-09T11:30:00.000000"
}
```

#### Example

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs \
  -H 'Content-Type: application/json' \
  -d '{
    "exchange": "bybit",
    "market": "futures",
    "symbol": "BTCUSDT",
    "interval": "1h",
    "backfill_limit": 300
  }'
```

### 8.5 `POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/pause`

Marks a tracked pair as paused and reloads streaming workers.

#### Request

```http
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/pause
```

#### Path parameters

| Name | Description |
| --- | --- |
| `exchange` | Exchange identifier |
| `market` | Market identifier |
| `symbol` | Symbol, path value should match stored symbol |
| `interval` | Tracked interval |

#### Example

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs/bybit/futures/BTCUSDT/1h/pause
```

### 8.6 `POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/resume`

Reactivates a paused pair, repairs missing history, and reloads stream workers.

#### Request

```http
POST /internal/pairs/{exchange}/{market}/{symbol}/{interval}/resume
```

#### Example

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs/bybit/futures/BTCUSDT/1h/resume
```

### 8.7 `DELETE /internal/pairs/{exchange}/{market}/{symbol}/{interval}`

Deletes a tracked pair and reloads stream workers.

#### Request

```http
DELETE /internal/pairs/{exchange}/{market}/{symbol}/{interval}
```

#### Success response

```json
{
  "ok": true,
  "deleted": true
}
```

#### Example

```bash
curl -X DELETE http://127.0.0.1:8088/internal/pairs/bybit/futures/BTCUSDT/1h
```

## 9. WebSocket API

### 9.1 `WS /ws/market`

Real-time subscription endpoint for normalized candle updates.

#### Endpoint

```text
ws://127.0.0.1:8088/ws/market
```

#### Connection behavior

After connect, the server immediately sends:

```json
{
  "type": "connected",
  "message": "market realtime websocket connected"
}
```

#### Client message schema

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `action` | string | Yes | `subscribe` or `unsubscribe` |
| `exchange` | string | Yes | Exchange identifier |
| `market` | string | Yes | Market identifier |
| `symbol` | string | Yes | Symbol |
| `interval` | string | Yes | Interval |

#### Subscribe example

```json
{
  "action": "subscribe",
  "exchange": "bybit",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1h"
}
```

#### Unsubscribe example

```json
{
  "action": "unsubscribe",
  "exchange": "bybit",
  "market": "futures",
  "symbol": "BTCUSDT",
  "interval": "1h"
}
```

#### Subscribe acknowledgment

```json
{
  "type": "subscribed",
  "channel": "kline:bybit:futures:BTCUSDT:1h"
}
```

#### Unsubscribe acknowledgment

```json
{
  "type": "unsubscribed",
  "channel": "kline:bybit:futures:BTCUSDT:1h"
}
```

#### Error responses

Invalid JSON:

```json
{
  "type": "error",
  "message": "invalid json"
}
```

Unsupported action:

```json
{
  "type": "error",
  "message": "unsupported action"
}
```

Missing fields:

```json
{
  "type": "error",
  "message": "exchange, market, symbol, interval are required"
}
```

#### Quick test with `wscat`

```bash
wscat -c ws://127.0.0.1:8088/ws/market
```

Then send:

```json
{"action":"subscribe","exchange":"bybit","market":"futures","symbol":"BTCUSDT","interval":"1h"}
```

## 10. Error Semantics

The service currently mixes framework-native validation responses with direct application exceptions.

Typical outcomes:

| Case | Typical status |
| --- | --- |
| Validation error on query/body | `422 Unprocessable Entity` |
| Missing tracked pair for pause/resume/delete | `404 Not Found` |
| Invalid provider symbol or unsupported exchange/market | `400` or `500` depending on current exception path |

Because some provider errors are raised directly from service code, production deployments should consider adding a consistent exception mapping layer if you want a fully stable external contract.

## 11. Provider Configuration

### 11.1 General service configuration

Example core environment variables:

```env
POSTGRES_DB=marketdata
POSTGRES_USER=marketdata
POSTGRES_PASSWORD=...
REDIS_URL=redis://redis:6379/0
DATABASE_URL=postgresql+asyncpg://marketdata:...@postgres:5432/marketdata
APP_ENV=production
APP_NAME=market-data-worker
APP_HOST=0.0.0.0
APP_PORT=8000
```

### 11.2 Bybit configuration

Optional overrides:

```env
BYBIT_REST_URL=https://api.bybit.com
BYBIT_WS_URL=wss://stream.bybit.com
BYBIT_INSTRUMENTS_CACHE_TTL_SEC=900
```

### 11.3 OANDA configuration

Required for OANDA support:

```env
OANDA_API_TOKEN=...
OANDA_ACCOUNT_ID=...
```

Optional overrides:

```env
OANDA_REST_URL=https://api-fxtrade.oanda.com
OANDA_STREAM_URL=https://stream-fxtrade.oanda.com
OANDA_INSTRUMENTS_CACHE_TTL_SEC=900
OANDA_RECONCILE_INTERVAL_SEC=20
```

## 12. End-to-End Test Recipes

### 12.1 Bybit futures flow

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs \
  -H 'Content-Type: application/json' \
  -d '{"exchange":"bybit","market":"futures","symbol":"BTCUSDT","interval":"1h","backfill_limit":300}'

curl "http://127.0.0.1:8088/api/klines?exchange=bybit&market=futures&symbol=BTCUSDT&interval=1h&limit=100"

curl http://127.0.0.1:8088/internal/health
```

### 12.2 Bybit spot flow

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs \
  -H 'Content-Type: application/json' \
  -d '{"exchange":"bybit","market":"spot","symbol":"BTCUSDT","interval":"1h","backfill_limit":300}'

curl "http://127.0.0.1:8088/api/klines?exchange=bybit&market=spot&symbol=BTCUSDT&interval=1h&limit=100"
```

### 12.3 OANDA forex flow

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs \
  -H 'Content-Type: application/json' \
  -d '{"exchange":"oanda","market":"forex","symbol":"EUR_USD","interval":"1m","backfill_limit":300}'

curl "http://127.0.0.1:8088/api/klines?exchange=oanda&market=forex&symbol=EUR_USD&interval=1h&limit=100"
```

### 12.4 OANDA metals flow

```bash
curl -X POST http://127.0.0.1:8088/internal/pairs \
  -H 'Content-Type: application/json' \
  -d '{"exchange":"oanda","market":"metals","symbol":"XAU_USD","interval":"1m","backfill_limit":300}'

curl "http://127.0.0.1:8088/api/klines?exchange=oanda&market=metals&symbol=XAU_USD&interval=1h&limit=100"
```

## 13. Implementation Notes

Important current implementation details:

- Tracked symbols are persisted in PostgreSQL.
- Closed candles are persisted in PostgreSQL and cached in Redis.
- Open candles are cached in Redis.
- Stream workers are reloaded after pair create, resume, pause, and delete operations.
- OANDA reconciliation runs periodically to correct recent candles.
- Bybit and Binance stream native exchange kline events directly.

## 14. Changelog Guidance

When you add or change routes, update this README in the same change set so the documented contract always matches the deployed API.
