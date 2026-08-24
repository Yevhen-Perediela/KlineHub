from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..db import get_db
from ..exchanges.bybit import InvalidBybitSymbolError
from ..exchanges.binance_spot import InvalidSpotSymbolError
from ..exchanges.oanda import InvalidOandaInstrumentError
from ..exchanges.okx import InvalidOkxSymbolError, OkxRateLimitError
from ..exchanges.registry import get_adapter
from ..models import TrackedPair
from ..price_basis import resolve_price_basis
from ..schemas import (
    TrackedPairCreate,
    TrackedPairResponse,
    TrackedPairListResponse,
    DeletePairResponse,
)

router = APIRouter(prefix="/internal/pairs", tags=["pairs"])


@router.get("", response_model=TrackedPairListResponse)
async def list_pairs(db: AsyncSession = Depends(get_db)) -> TrackedPairListResponse:
    result = await db.execute(
        select(TrackedPair).order_by(
            TrackedPair.exchange,
            TrackedPair.market,
            TrackedPair.symbol,
            TrackedPair.interval,
            TrackedPair.price_basis,
        )
    )
    items = result.scalars().all()

    return TrackedPairListResponse(
        items=[TrackedPairResponse.model_validate(item) for item in items],
        count=len(items),
    )


@router.post("", response_model=TrackedPairResponse, status_code=status.HTTP_201_CREATED)
async def create_pair(
    payload: TrackedPairCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> TrackedPairResponse:
    symbol = payload.symbol.upper()
    exchange = payload.exchange.lower()
    market = payload.market.lower()
    try:
        price_basis = resolve_price_basis(
            exchange=exchange, market=market, requested_price_basis=payload.price_basis
        ).value
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if exchange in {"bybit", "okx"}:
        try:
            adapter = get_adapter(exchange=exchange, market=market)
            symbol = await adapter.resolve_symbol(market=market, symbol=symbol)  # type: ignore[attr-defined]
        except OkxRateLimitError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except (InvalidBybitSymbolError, InvalidOkxSymbolError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        await request.app.state.backfill_service.validate_pair(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=payload.interval,
            price_basis=price_basis,
        )
    except OkxRateLimitError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (InvalidSpotSymbolError, InvalidBybitSymbolError, InvalidOandaInstrumentError, InvalidOkxSymbolError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    existing_q = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == payload.interval,
            TrackedPair.price_basis == price_basis,
        )
    )
    existing = existing_q.scalar_one_or_none()

    if existing:
        existing.source = payload.source
        existing.priority = payload.priority
        existing.auto_stop_at = None
        if existing.status != "active":
            existing.status = "active"
        existing.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(existing)

        try:
            await request.app.state.backfill_service.backfill_recent_pair(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=payload.interval,
                price_basis=price_basis,
                limit=payload.backfill_limit or settings.default_backfill_limit,
            )
        except OkxRateLimitError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await request.app.state.stream_manager.reload()
        return TrackedPairResponse.model_validate(existing)

    item = TrackedPair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=payload.interval,
        price_basis=price_basis,
        status="active",
        source=payload.source,
        priority=payload.priority,
        auto_stop_at=None,
    )

    db.add(item)
    await db.commit()
    await db.refresh(item)

    try:
        await request.app.state.backfill_service.backfill_recent_pair(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=payload.interval,
            price_basis=price_basis,
            limit=payload.backfill_limit or settings.default_backfill_limit,
        )
    except OkxRateLimitError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    await request.app.state.stream_manager.reload()

    return TrackedPairResponse.model_validate(item)


@router.delete(
    "/{exchange}/{market}/{symbol}/{interval}",
    response_model=DeletePairResponse,
)
async def delete_pair(
    exchange: str,
    market: str,
    symbol: str,
    interval: str,
    request: Request,
    price_basis: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> DeletePairResponse:
    symbol = symbol.upper()
    try:
        resolved_basis = resolve_price_basis(
            exchange=exchange, market=market, requested_price_basis=price_basis
        ).value
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    existing_q = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
            TrackedPair.price_basis == resolved_basis,
        )
    )
    existing = existing_q.scalar_one_or_none()

    if not existing:
        raise HTTPException(status_code=404, detail="Tracked pair not found")

    await db.execute(delete(TrackedPair).where(TrackedPair.id == existing.id))
    await db.commit()

    await request.app.state.stream_manager.reload()

    return DeletePairResponse(ok=True, deleted=True)


@router.post("/{exchange}/{market}/{symbol}/{interval}/pause", response_model=TrackedPairResponse)
async def pause_pair(
    exchange: str,
    market: str,
    symbol: str,
    interval: str,
    request: Request,
    price_basis: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> TrackedPairResponse:
    symbol = symbol.upper()
    try:
        resolved_basis = resolve_price_basis(
            exchange=exchange, market=market, requested_price_basis=price_basis
        ).value
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
            TrackedPair.price_basis == resolved_basis,
        )
    )
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(status_code=404, detail="Tracked pair not found")

    item.status = "paused"
    item.auto_stop_at = None
    item.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(item)

    await request.app.state.stream_manager.reload()

    return TrackedPairResponse.model_validate(item)


@router.post("/{exchange}/{market}/{symbol}/{interval}/resume", response_model=TrackedPairResponse)
async def resume_pair(
    exchange: str,
    market: str,
    symbol: str,
    interval: str,
    request: Request,
    price_basis: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> TrackedPairResponse:
    symbol = symbol.upper()
    try:
        resolved_basis = resolve_price_basis(
            exchange=exchange, market=market, requested_price_basis=price_basis
        ).value
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
            TrackedPair.price_basis == resolved_basis,
        )
    )
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(status_code=404, detail="Tracked pair not found")

    item.status = "active"
    item.source = "api" if item.source == "on_demand" else item.source
    item.auto_stop_at = None
    item.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(item)

    await request.app.state.backfill_service.repair_missing_for_pair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=interval,
        price_basis=resolved_basis,
    )
    await request.app.state.stream_manager.reload()

    return TrackedPairResponse.model_validate(item)
