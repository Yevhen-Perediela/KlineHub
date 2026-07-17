from __future__ import annotations

import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/market")
async def market_ws(websocket: WebSocket):
    realtime_service = websocket.app.state.realtime_service
    await realtime_service.connect(websocket)

    try:
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": "market realtime websocket connected",
        }))

        while True:
            raw = await websocket.receive_text()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "invalid json",
                }))
                continue

            action = data.get("action")
            exchange = data.get("exchange")
            market = data.get("market")
            symbol = data.get("symbol")
            interval = data.get("interval")

            if action not in {"subscribe", "unsubscribe"}:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "unsupported action",
                }))
                continue

            if not all([exchange, market, symbol, interval]):
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "exchange, market, symbol, interval are required",
                }))
                continue

            if action == "subscribe":
                channel = await realtime_service.subscribe(
                    websocket,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                )
                await websocket.send_text(json.dumps({
                    "type": "subscribed",
                    "channel": channel,
                }))
            else:
                channel = await realtime_service.unsubscribe(
                    websocket,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                )
                await websocket.send_text(json.dumps({
                    "type": "unsubscribed",
                    "channel": channel,
                }))

    except WebSocketDisconnect:
        await realtime_service.disconnect(websocket)
    except Exception:
        await realtime_service.disconnect(websocket)


@router.websocket("/ws/chart")
async def chart_ws(websocket: WebSocket):
    chart_ws_service = websocket.app.state.chart_ws_service
    await chart_ws_service.handle(websocket)
