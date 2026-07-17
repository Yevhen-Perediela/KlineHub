from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.registry import get_adapter
from ..models import TrackedPair
from ..schemas import (
    RefreshPopularPairsGroupSummary,
    RefreshPopularPairsItem,
    RefreshPopularPairsRequest,
    RefreshPopularPairsResponse,
    RefreshPopularPairsSummary,
)
from .backfill_service import BackfillService
from .exchange_limit_service import record_http_error, record_http_response
from .stream_manager import StreamManager


@dataclass(frozen=True)
class PairKey:
    exchange: str
    market: str
    symbol: str
    interval: str


@dataclass(frozen=True)
class RankedPair:
    exchange: str
    market: str
    symbol: str
    base: str
    quote: str
    score: float


@dataclass(frozen=True)
class RankedBase:
    exchange: str
    market: str
    base: str
    rank: int
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class PairCandidate:
    symbol: str
    base: str
    quote: str


@dataclass(frozen=True)
class DesiredPair:
    key: PairKey
    group: str
    reason: str


@dataclass
class DiffResult:
    to_add: list[DesiredPair] = field(default_factory=list)
    to_resume: list[DesiredPair] = field(default_factory=list)
    unchanged: list[DesiredPair] = field(default_factory=list)
    to_pause: list[DesiredPair] = field(default_factory=list)
    to_delete: list[DesiredPair] = field(default_factory=list)


@dataclass(frozen=True)
class ApplyResult:
    failures: list[RefreshPopularPairsItem]
    applied_changes: int


class PopularPairsService:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        backfill_service: BackfillService,
        stream_manager: StreamManager,
    ) -> None:
        self.session_factory = session_factory
        self.backfill_service = backfill_service
        self.stream_manager = stream_manager

    async def refresh_popular_pairs(
        self,
        *,
        payload: RefreshPopularPairsRequest,
    ) -> RefreshPopularPairsResponse:
        desired_pairs = await self._build_desired_pairs(payload)

        async with self.session_factory() as session:
            current_rows = await session.execute(select(TrackedPair))
            current_items = current_rows.scalars().all()

            current_by_key = {
                PairKey(
                    exchange=item.exchange,
                    market=item.market,
                    symbol=item.symbol,
                    interval=item.interval,
                ): item
                for item in current_items
            }

            diff = self._build_diff(
                desired_pairs=desired_pairs,
                current_by_key=current_by_key,
                mode=payload.mode,
            )

            if payload.dry_run:
                return self._build_response(
                    payload=payload,
                    desired_pairs=desired_pairs,
                    current_total=len(current_items),
                    diff=diff,
                    failures=[],
                    reload_triggered=False,
                )

            apply_result = await self._apply_diff(
                session=session,
                diff=diff,
                current_by_key=current_by_key,
            )

        reload_triggered = apply_result.applied_changes > 0
        if reload_triggered:
            await self.stream_manager.reload()

        return self._build_response(
            payload=payload,
            desired_pairs=desired_pairs,
            current_total=len(current_items),
            diff=diff,
            failures=apply_result.failures,
            reload_triggered=reload_triggered,
        )

    async def _build_desired_pairs(
        self,
        payload: RefreshPopularPairsRequest,
    ) -> dict[PairKey, DesiredPair]:
        crypto_interval = payload.crypto_interval
        oanda_interval = payload.oanda_interval

        tasks = [
            self._build_ranked_crypto_pairs(
                exchange="binance",
                market="spot",
                quotes=payload.binance.quotes,
                base_limit=payload.binance.spot_base_limit,
                interval=crypto_interval,
            ),
            self._build_ranked_crypto_pairs(
                exchange="binance",
                market="futures",
                quotes=payload.binance.quotes,
                base_limit=payload.binance.futures_base_limit,
                interval=crypto_interval,
            ),
            self._build_ranked_crypto_pairs(
                exchange="bybit",
                market="spot",
                quotes=payload.bybit.quotes,
                base_limit=payload.bybit.spot_base_limit,
                interval=crypto_interval,
            ),
            self._build_ranked_crypto_pairs(
                exchange="bybit",
                market="futures",
                quotes=payload.bybit.quotes,
                base_limit=payload.bybit.futures_base_limit,
                interval=crypto_interval,
            ),
            self._build_ranked_crypto_pairs(
                exchange="okx",
                market="spot",
                quotes=payload.okx.quotes,
                base_limit=payload.okx.spot_base_limit,
                interval=crypto_interval,
            ),
            self._build_ranked_crypto_pairs(
                exchange="okx",
                market="futures",
                quotes=payload.okx.quotes,
                base_limit=payload.okx.futures_base_limit,
                interval=crypto_interval,
            ),
            self._build_oanda_pairs(
                market="forex",
                symbols=payload.oanda.forex_symbols if payload.oanda.enable_forex else [],
                interval=oanda_interval,
            ),
            self._build_oanda_pairs(
                market="metals",
                symbols=payload.oanda.metals_symbols if payload.oanda.enable_metals else [],
                interval=oanda_interval,
            ),
        ]

        results = await asyncio.gather(*tasks)
        desired_pairs: dict[PairKey, DesiredPair] = {}
        for group_pairs in results:
            for item in group_pairs:
                desired_pairs[item.key] = item
        return desired_pairs

    async def _build_ranked_crypto_pairs(
        self,
        *,
        exchange: str,
        market: str,
        quotes: list[str],
        base_limit: int,
        interval: str,
    ) -> list[DesiredPair]:
        normalized_quotes = [quote.upper() for quote in quotes]
        ranked_bases = await self._rank_crypto_bases(
            exchange=exchange,
            market=market,
            quotes=normalized_quotes,
            base_limit=base_limit,
        )
        group = f"{exchange}_{market}"
        pairs: list[DesiredPair] = []

        for base in ranked_bases:
            for symbol in base.symbols:
                pairs.append(
                    DesiredPair(
                        key=PairKey(
                            exchange=exchange,
                            market=market,
                            symbol=symbol,
                            interval=interval,
                        ),
                        group=group,
                        reason=f"cmc top base {base.base} rank={base.rank}",
                    )
                )

        return pairs

    async def _build_oanda_pairs(
        self,
        *,
        market: str,
        symbols: list[str],
        interval: str,
    ) -> list[DesiredPair]:
        if interval != "1m":
            raise ValueError("OANDA tracked pairs must use interval=1m")

        adapter = get_adapter(exchange="oanda", market=market)
        instruments = await adapter.list_instruments()
        valid_symbols = {
            item["symbol"]
            for item in instruments
            if item["market"] == market and bool(item["tradeable"])
        }

        desired: list[DesiredPair] = []
        for symbol in symbols:
            normalized_symbol = symbol.upper()
            if normalized_symbol not in valid_symbols:
                continue
            desired.append(
                DesiredPair(
                    key=PairKey(
                        exchange="oanda",
                        market=market,
                        symbol=normalized_symbol,
                        interval=interval,
                    ),
                    group=f"oanda_{market}",
                    reason="curated validated OANDA instrument",
                )
            )
        return desired

    async def _rank_crypto_bases(
        self,
        *,
        exchange: str,
        market: str,
        quotes: list[str],
        base_limit: int,
    ) -> list[RankedBase]:
        cmc_bases = await self._fetch_coinmarketcap_top_bases()
        target_bases = cmc_bases[:base_limit]
        candidates = await self._fetch_exchange_candidates(
            exchange=exchange,
            market=market,
            quotes=quotes,
        )

        by_base: dict[str, dict[str, PairCandidate]] = {}
        for item in candidates:
            by_base.setdefault(item.base, {})[item.quote] = item

        ranked_bases: list[RankedBase] = []
        for rank, base in enumerate(target_bases, start=1):
            by_quote = by_base.get(base)
            if not by_quote:
                continue
            expanded_symbols = tuple(
                by_quote[quote].symbol
                for quote in quotes
                if quote in by_quote
            )
            if not expanded_symbols:
                continue

            ranked_bases.append(
                RankedBase(
                    exchange=exchange,
                    market=market,
                    base=base,
                    rank=rank,
                    symbols=expanded_symbols,
                )
            )

        return ranked_bases[:base_limit]

    async def _fetch_exchange_candidates(
        self,
        *,
        exchange: str,
        market: str,
        quotes: list[str],
    ) -> list[PairCandidate]:
        if exchange == "binance":
            adapter = get_adapter(exchange="binance", market=market)
            instruments = await adapter.list_instruments()
            return [
                PairCandidate(
                    symbol=str(item["symbol"]).upper(),
                    base=str(item["base_asset"]).upper(),
                    quote=str(item["quote_asset"]).upper(),
                )
                for item in instruments
                if item.get("status") == "TRADING"
                and item.get("base_asset")
                and item.get("quote_asset")
                and str(item.get("quote_asset", "")).upper() in quotes
                and (market != "spot" or item.get("is_spot_trading_allowed"))
                and (
                    market != "futures"
                    or str(item.get("contract_type", "")).upper() == "PERPETUAL"
                )
            ]

        if exchange == "bybit":
            adapter = get_adapter(exchange="bybit", market=market)
            instruments = await adapter.list_instruments(market=market)
            return [
                PairCandidate(
                    symbol=str(item["symbol"]).upper(),
                    base=str(item["base_coin"]).upper(),
                    quote=str(item["quote_coin"]).upper(),
                )
                for item in instruments
                if item.get("status") == "Trading"
                and item.get("base_coin")
                and item.get("quote_coin")
                and str(item.get("quote_coin", "")).upper() in quotes
            ]

        if exchange == "okx":
            adapter = get_adapter(exchange="okx", market=market)
            instruments = await adapter.list_instruments(market=market)
            return [
                PairCandidate(
                    symbol=str(item["symbol"]).upper(),
                    base=str(item["base_coin"]).upper(),
                    quote=str(item["quote_coin"]).upper(),
                )
                for item in instruments
                if item.get("status") == "live"
                and item.get("base_coin")
                and item.get("quote_coin")
                and str(item.get("quote_coin", "")).upper() in quotes
            ]

        raise ValueError(f"Unsupported crypto provider: {exchange}")

    async def _fetch_coinmarketcap_top_bases(self) -> list[str]:
        api_key = settings.coinmarketcap_api_key
        if not api_key:
            raise ValueError("COINMARKETCAP_API_KEY is required for popular pair refresh")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{settings.coinmarketcap_api_url}/v1/cryptocurrency/listings/latest",
                    headers={
                        "X-CMC_PRO_API_KEY": api_key,
                        "Accept": "application/json",
                    },
                    params={
                        "start": 1,
                        "limit": settings.coinmarketcap_listings_limit,
                        "convert": "USD",
                    },
                )
                record_http_response(exchange="coinmarketcap", response=response)
            except Exception as exc:
                record_http_error(exchange="coinmarketcap", error=exc)
                raise

        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []

        symbols: list[str] = []
        seen: set[str] = set()
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)

        return symbols

    def _build_diff(
        self,
        *,
        desired_pairs: dict[PairKey, DesiredPair],
        current_by_key: dict[PairKey, TrackedPair],
        mode: str,
    ) -> DiffResult:
        diff = DiffResult()

        for key, desired in desired_pairs.items():
            current = current_by_key.get(key)
            if current is None:
                diff.to_add.append(desired)
            elif current.status == "paused":
                diff.to_resume.append(desired)
            else:
                diff.unchanged.append(desired)

        desired_keys = set(desired_pairs)
        for key, current in current_by_key.items():
            if key in desired_keys:
                continue
            if current.source == "on_demand":
                continue

            stale_item = DesiredPair(
                key=key,
                group=f"{key.exchange}_{key.market}",
                reason="not present in desired set",
            )
            if mode == "delete":
                diff.to_delete.append(stale_item)
            elif current.status == "active":
                diff.to_pause.append(stale_item)
            else:
                diff.unchanged.append(stale_item)

        return diff

    async def _apply_diff(
        self,
        *,
        session: AsyncSession,
        diff: DiffResult,
        current_by_key: dict[PairKey, TrackedPair],
    ) -> ApplyResult:
        failures: list[RefreshPopularPairsItem] = []
        applied_changes = 0

        for item in diff.to_add:
            try:
                async with session.begin_nested():
                    await self.backfill_service.validate_pair(
                        exchange=item.key.exchange,
                        market=item.key.market,
                        symbol=item.key.symbol,
                        interval=item.key.interval,
                    )
                    row = TrackedPair(
                        exchange=item.key.exchange,
                        market=item.key.market,
                        symbol=item.key.symbol,
                        interval=item.key.interval,
                        status="active",
                        source="popular_refresh",
                        priority=100,
                    )
                    session.add(row)
                    await session.flush()
                    await self.backfill_service.backfill_recent_pair(
                        exchange=item.key.exchange,
                        market=item.key.market,
                        symbol=item.key.symbol,
                        interval=item.key.interval,
                        limit=settings.default_backfill_limit,
                    )
                applied_changes += 1
            except Exception as exc:
                failures.append(self._failure_item(item=item, action="add", error=exc))

        for item in diff.to_resume:
            try:
                row = current_by_key[item.key]
                previous_status = row.status
                previous_updated_at = row.updated_at
                await self.backfill_service.repair_missing_for_pair(
                    exchange=item.key.exchange,
                    market=item.key.market,
                    symbol=item.key.symbol,
                    interval=item.key.interval,
                )
                row.status = "active"
                row.updated_at = datetime.utcnow()
                applied_changes += 1
            except Exception as exc:
                row = current_by_key[item.key]
                row.status = previous_status
                row.updated_at = previous_updated_at
                failures.append(self._failure_item(item=item, action="resume", error=exc))

        for item in diff.unchanged:
            row = current_by_key.get(item.key)
            if row is not None and row.source == "on_demand":
                row.source = "popular_refresh"
                row.auto_stop_at = None
                row.updated_at = datetime.utcnow()

        for item in diff.to_pause:
            try:
                row = current_by_key[item.key]
                if row.status != "paused":
                    row.status = "paused"
                    row.updated_at = datetime.utcnow()
                    applied_changes += 1
            except Exception as exc:
                failures.append(self._failure_item(item=item, action="pause", error=exc))

        for item in diff.to_delete:
            try:
                row = current_by_key[item.key]
                await session.execute(delete(TrackedPair).where(TrackedPair.id == row.id))
                applied_changes += 1
            except Exception as exc:
                failures.append(self._failure_item(item=item, action="delete", error=exc))

        await session.commit()
        return ApplyResult(failures=failures, applied_changes=applied_changes)

    def _build_response(
        self,
        *,
        payload: RefreshPopularPairsRequest,
        desired_pairs: dict[PairKey, DesiredPair],
        current_total: int,
        diff: DiffResult,
        failures: list[RefreshPopularPairsItem],
        reload_triggered: bool,
    ) -> RefreshPopularPairsResponse:
        added = [self._action_item(item, "add") for item in diff.to_add]
        resumed = [self._action_item(item, "resume") for item in diff.to_resume]
        paused = [self._action_item(item, "pause") for item in diff.to_pause]
        deleted = [self._action_item(item, "delete") for item in diff.to_delete]
        unchanged = [self._action_item(item, "unchanged") for item in diff.unchanged]

        if failures:
            failed_keys = {
                (item.exchange, item.market, item.symbol, item.interval, item.action)
                for item in failures
            }
            added = [item for item in added if (item.exchange, item.market, item.symbol, item.interval, item.action) not in failed_keys]
            resumed = [item for item in resumed if (item.exchange, item.market, item.symbol, item.interval, item.action) not in failed_keys]
            paused = [item for item in paused if (item.exchange, item.market, item.symbol, item.interval, item.action) not in failed_keys]
            deleted = [item for item in deleted if (item.exchange, item.market, item.symbol, item.interval, item.action) not in failed_keys]

        groups: dict[str, RefreshPopularPairsGroupSummary] = {}
        for desired in desired_pairs.values():
            groups.setdefault(desired.group, RefreshPopularPairsGroupSummary()).desired += 1
        for item in added:
            groups.setdefault(item.group or "other", RefreshPopularPairsGroupSummary()).added += 1
        for item in resumed:
            groups.setdefault(item.group or "other", RefreshPopularPairsGroupSummary()).resumed += 1
        for item in paused:
            groups.setdefault(item.group or "other", RefreshPopularPairsGroupSummary()).paused += 1
        for item in deleted:
            groups.setdefault(item.group or "other", RefreshPopularPairsGroupSummary()).deleted += 1
        for item in unchanged:
            groups.setdefault(item.group or "other", RefreshPopularPairsGroupSummary()).unchanged += 1

        return RefreshPopularPairsResponse(
            ok=not failures,
            dry_run=payload.dry_run,
            mode=payload.mode,
            summary=RefreshPopularPairsSummary(
                desired_total=len(desired_pairs),
                current_total=current_total,
                to_add=len(added),
                to_resume=len(resumed),
                to_pause=len(paused),
                to_delete=len(deleted),
                unchanged=len(unchanged),
                failed=len(failures),
                reload_triggered=reload_triggered,
            ),
            groups=groups,
            added=added,
            resumed=resumed,
            paused=paused,
            deleted=deleted,
            unchanged=unchanged,
            failed_items=failures,
        )

    def _action_item(self, item: DesiredPair, action: str) -> RefreshPopularPairsItem:
        return RefreshPopularPairsItem(
            exchange=item.key.exchange,
            market=item.key.market,
            symbol=item.key.symbol,
            interval=item.key.interval,
            action=action,
            reason=item.reason,
            group=item.group,
        )

    def _failure_item(
        self,
        *,
        item: DesiredPair,
        action: str,
        error: Exception,
    ) -> RefreshPopularPairsItem:
        return RefreshPopularPairsItem(
            exchange=item.key.exchange,
            market=item.key.market,
            symbol=item.key.symbol,
            interval=item.key.interval,
            action=action,
            reason=item.reason,
            error=str(error),
            group=item.group,
            status="failed",
        )

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
