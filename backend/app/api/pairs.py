from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..db import get_db
from ..models import TrackedPair
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

    await request.app.state.backfill_service.validate_pair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=payload.interval,
    )

    existing_q = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == payload.interval,
        )
    )
    existing = existing_q.scalar_one_or_none()

    if existing:
        if existing.status != "active":
            existing.status = "active"
            existing.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(existing)

        await request.app.state.backfill_service.backfill_recent_pair(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=payload.interval,
            limit=payload.backfill_limit or settings.default_backfill_limit,
        )
        await request.app.state.stream_manager.reload()
        return TrackedPairResponse.model_validate(existing)

    item = TrackedPair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=payload.interval,
        status="active",
        source=payload.source,
        priority=payload.priority,
    )

    db.add(item)
    await db.commit()
    await db.refresh(item)

    await request.app.state.backfill_service.backfill_recent_pair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=payload.interval,
        limit=payload.backfill_limit or settings.default_backfill_limit,
    )
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
    db: AsyncSession = Depends(get_db),
) -> DeletePairResponse:
    symbol = symbol.upper()

    existing_q = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
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
    db: AsyncSession = Depends(get_db),
) -> TrackedPairResponse:
    symbol = symbol.upper()

    result = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
        )
    )
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(status_code=404, detail="Tracked pair not found")

    item.status = "paused"
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
    db: AsyncSession = Depends(get_db),
) -> TrackedPairResponse:
    symbol = symbol.upper()

    result = await db.execute(
        select(TrackedPair).where(
            TrackedPair.exchange == exchange,
            TrackedPair.market == market,
            TrackedPair.symbol == symbol,
            TrackedPair.interval == interval,
        )
    )
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(status_code=404, detail="Tracked pair not found")

    item.status = "active"
    item.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(item)

    await request.app.state.backfill_service.repair_missing_for_pair(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=interval,
    )
    await request.app.state.stream_manager.reload()

    return TrackedPairResponse.model_validate(item)
