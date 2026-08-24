from __future__ import annotations

import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..price_basis import resolve_price_basis

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
            requested_price_basis = data.get("price_basis")

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

            try:
                price_basis = resolve_price_basis(
                    exchange=exchange,
                    market=market,
                    requested_price_basis=requested_price_basis,
                ).value
            except ValueError as exc:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": str(exc),
                }))
                continue

            if action == "subscribe":
                channel = await realtime_service.subscribe(
                    websocket,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    price_basis=price_basis,
                )
                await websocket.send_text(json.dumps({
                    "type": "subscribed",
                    "channel": channel,
                    "price_basis": price_basis,
                }))
            else:
                channel = await realtime_service.unsubscribe(
                    websocket,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    price_basis=price_basis,
                )
                await websocket.send_text(json.dumps({
                    "type": "unsubscribed",
                    "channel": channel,
                    "price_basis": price_basis,
                }))

    except WebSocketDisconnect:
        await realtime_service.disconnect(websocket)
    except Exception:
        await realtime_service.disconnect(websocket)


@router.websocket("/ws/chart")
async def chart_ws(websocket: WebSocket):
    chart_ws_service = websocket.app.state.chart_ws_service
    await chart_ws_service.handle(websocket)
