"""
Async exchange execution via ccxt.async_support.

This module is written as a general-purpose, safety-first execution layer.
It supports:
- loading markets
- basic liquidity checks
- market buy/sell
- robust exception handling and retries for transient network/rate-limit issues

NOTE:
Derivative leverage/margin settings vary widely by exchange and market type.
This framework exposes a hook to apply exchange-specific parameters safely.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import ccxt.async_support as ccxt

from config import ExchangeConfig


@dataclass(frozen=True)
class TradeResult:
    symbol: str
    side: str
    amount: float
    avg_price: float | None
    cost: float | None
    fee: float | None
    pnl_quote: float | None
    order_id: str | None


class ExchangeTrader:
    """
    Marc-Antoine / Gemini Engineering Notes
    --------------------------------------
    - Exchange APIs are probabilistic systems: assume partial outages, timeouts, and inconsistent fields.
    - Latency-sensitive behavior must still be bounded by safety controls (allocation caps, stop-losses).
    - This implementation favors correctness and survivability over micro-optimizations.
    """

    def __init__(self, cfg: ExchangeConfig) -> None:
        self._cfg = cfg
        self._exchange = self._build_exchange(cfg)
        self._markets_loaded = False

    @staticmethod
    def _build_exchange(cfg: ExchangeConfig) -> ccxt.Exchange:
        if not hasattr(ccxt, cfg.exchange_id):
            raise RuntimeError(f"Unsupported exchange_id={cfg.exchange_id!r} in ccxt.")

        klass = getattr(ccxt, cfg.exchange_id)
        params: Dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": cfg.timeout_ms,
        }
        if not cfg.dry_run:
            params["apiKey"] = cfg.api_key
            params["secret"] = cfg.api_secret
            if cfg.password:
                params["password"] = cfg.password

        return klass(params)

    async def close(self) -> None:
        try:
            await self._exchange.close()
        except Exception:
            return

    async def load_markets(self) -> None:
        if self._markets_loaded:
            return
        await self._with_retries(self._exchange.load_markets)
        self._markets_loaded = True

    async def _with_retries(self, fn, *args, **kwargs):
        # Tight retry loop for transient conditions.
        backoffs = [0.1, 0.25, 0.5, 1.0]
        last_exc: Exception | None = None
        for b in backoffs:
            try:
                return await fn(*args, **kwargs)
            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection, ccxt.RequestTimeout, ccxt.NetworkError) as e:
                last_exc = e
                await asyncio.sleep(b)
            except Exception as e:
                # Non-transient or unknown; bubble up.
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("Retry wrapper failed unexpectedly.")

    async def get_free_balance(self, currency: str) -> float:
        if self._cfg.dry_run:
            # In dry-run, assume a notional balance.
            return 100.0
        bal = await self._with_retries(self._exchange.fetch_balance)
        free = bal.get("free", {}).get(currency)
        if free is None:
            # Some exchanges return balances in different shapes.
            total = bal.get("total", {}).get(currency, 0.0)
            used = bal.get("used", {}).get(currency, 0.0)
            free = float(total) - float(used)
        return float(free or 0.0)

    async def check_basic_liquidity(self, symbol: str, min_quote_depth: float = 500.0) -> bool:
        """
        Simple orderbook depth check: sum best N bids/asks quote value.
        This is not a guarantee of fill quality; it's a conservative gate.
        """
        await self.load_markets()
        ob = await self._with_retries(self._exchange.fetch_order_book, symbol, 20)
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []

        def quote_depth(levels) -> float:
            acc = 0.0
            for price, amount in levels[:10]:
                acc += float(price) * float(amount)
            return acc

        bid_q = quote_depth(bids)
        ask_q = quote_depth(asks)
        return (bid_q >= min_quote_depth) and (ask_q >= min_quote_depth)

    def _symbol_from_ticker(self, ticker: str) -> str:
        """
        Convert a ticker like DOGE into a ccxt symbol like DOGE/USDT.
        """
        base = ticker.upper().strip().lstrip("$")
        return f"{base}/{self._cfg.quote_currency}"

    async def _maybe_set_leverage(self, symbol: str) -> None:
        """
        Optional leverage hook.

        Many exchanges require:
        - specific market type (swap/futures)
        - separate endpoints/params
        - margin mode selection
        - per-symbol leverage configuration

        This function is best-effort and will no-op safely if unsupported.
        """
        lev = self._cfg.requested_leverage
        if not lev or self._cfg.dry_run:
            return
        # ccxt unified method exists for some exchanges: set_leverage(leverage, symbol, params)
        if getattr(self._exchange, "has", {}).get("setLeverage"):
            try:
                await self._with_retries(self._exchange.set_leverage, lev, symbol, {})
            except Exception:
                # Don’t hard-fail; execution can proceed without leverage change.
                return

    @staticmethod
    def _allocation_from_confidence(confidence_score: float) -> float:
        """
        Confidence -> allocation fraction mapping.

        Requirements:
        - confidence 0.90 => 0.30 of available quote balance
        - confidence 0.98+ => 0.75
        - interpolate linearly between
        """
        c = float(confidence_score)
        if c <= 0.90:
            return 0.30
        if c >= 0.98:
            return 0.75
        # Linear interpolation from (0.90, 0.30) to (0.98, 0.75)
        t = (c - 0.90) / (0.98 - 0.90)
        return 0.30 + t * (0.75 - 0.30)

    async def execute_trade(
        self,
        ticker: str,
        hold_seconds: float,
        confidence_score: float,
        *,
        stop_loss_drawdown: float = 0.025,
        price_poll_interval_s: float = 0.10,
    ) -> Tuple[TradeResult, TradeResult]:
        """
        Executes a buy then sells after a fixed hold period, with a hard stop-loss.

        Safety behavior:
        - allocation fraction is derived from confidence and capped by config max_quote_allocation_fraction
        - dry_run prints simulated fills
        - hard stop-loss: exit early if price drops by stop_loss_drawdown from entry
        """
        await self.load_markets()
        symbol = self._symbol_from_ticker(ticker)

        alloc = self._allocation_from_confidence(confidence_score)
        alloc = min(max(alloc, 0.0), self._cfg.max_quote_allocation_fraction)

        # Do not auto-increase leverage in code; leverage must be explicitly configured in config/env.
        await self._maybe_set_leverage(symbol)

        free_quote = await self.get_free_balance(self._cfg.quote_currency)
        quote_to_spend = free_quote * alloc

        if quote_to_spend <= 1.0:
            raise ccxt.InsufficientFunds(f"Free {self._cfg.quote_currency} too low for allocation.")

        # Prefer using quoteOrderQty if supported; otherwise approximate base amount.
        market = self._exchange.market(symbol)
        precision = market.get("precision", {}) or {}
        amount_precision = precision.get("amount")

        if self._cfg.dry_run:
            buy = TradeResult(
                symbol=symbol,
                side="buy",
                amount=float("nan"),
                avg_price=None,
                cost=quote_to_spend,
                fee=None,
                pnl_quote=None,
                order_id="DRYRUN-BUY",
            )
            await asyncio.sleep(hold_seconds)
            sell = TradeResult(
                symbol=symbol,
                side="sell",
                amount=float("nan"),
                avg_price=None,
                cost=quote_to_spend,
                fee=None,
                pnl_quote=0.0,
                order_id="DRYRUN-SELL",
            )
            return buy, sell

        # Liquidity gate
        ok = await self.check_basic_liquidity(symbol)
        if not ok:
            raise RuntimeError(f"Liquidity gate failed for {symbol}.")

        # Fetch price for sizing if quoteOrderQty not supported.
        ticker_info = await self._with_retries(self._exchange.fetch_ticker, symbol)
        last = float(ticker_info.get("last") or ticker_info.get("close") or 0.0)
        if last <= 0:
            raise RuntimeError(f"Could not fetch valid price for {symbol}.")

        base_amount = quote_to_spend / last
        if amount_precision is not None:
            # Round down to amount precision
            p = int(amount_precision)
            base_amount = math.floor(base_amount * (10**p)) / (10**p)

        if base_amount <= 0:
            raise RuntimeError("Computed order amount <= 0.")

        buy_order = await self._with_retries(self._exchange.create_market_buy_order, symbol, base_amount)
        entry_price = float(buy_order.get("average") or 0.0) or last
        buy = TradeResult(
            symbol=symbol,
            side="buy",
            amount=float(base_amount),
            avg_price=buy_order.get("average"),
            cost=buy_order.get("cost"),
            fee=(buy_order.get("fee") or {}).get("cost"),
            pnl_quote=None,
            order_id=buy_order.get("id"),
        )

        # Hard stop-loss monitoring during the hold window.
        stop_floor = entry_price * (1.0 - float(stop_loss_drawdown))
        deadline = asyncio.get_running_loop().time() + float(hold_seconds)
        exited_early = False
        while True:
            now = asyncio.get_running_loop().time()
            if now >= deadline:
                break
            await asyncio.sleep(float(price_poll_interval_s))
            try:
                t = await self._with_retries(self._exchange.fetch_ticker, symbol)
                px = float(t.get("last") or t.get("close") or 0.0)
                if px > 0 and px <= stop_floor:
                    exited_early = True
                    break
            except Exception:
                # If price polling fails transiently, continue the hold window.
                continue

        sell_order = await self._with_retries(self._exchange.create_market_sell_order, symbol, base_amount)
        buy_cost = float(buy_order.get("cost") or quote_to_spend)
        sell_cost = float(sell_order.get("cost") or 0.0)
        buy_fee = float((buy_order.get("fee") or {}).get("cost") or 0.0)
        sell_fee = float((sell_order.get("fee") or {}).get("cost") or 0.0)
        realized = (sell_cost - sell_fee) - (buy_cost + buy_fee)
        sell = TradeResult(
            symbol=symbol,
            side="sell",
            amount=float(base_amount),
            avg_price=sell_order.get("average"),
            cost=sell_order.get("cost"),
            fee=(sell_order.get("fee") or {}).get("cost"),
            pnl_quote=realized,
            order_id=sell_order.get("id"),
        )
        return buy, sell

    async def market_buy_then_sell(
        self,
        ticker: str,
        hold_seconds: float,
        allocation_fraction: float,
    ) -> Tuple[TradeResult, TradeResult]:
        """
        Backwards-compatible wrapper retained for older callers.
        """
        # Map legacy allocation fraction into a pseudo-confidence in [0.90, 0.98].
        # This keeps behavior stable-ish while callers migrate to execute_trade().
        alloc = min(max(float(allocation_fraction), 0.0), 1.0)
        # Invert the interpolation approximately:
        if alloc <= 0.30:
            c = 0.90
        elif alloc >= 0.75:
            c = 0.98
        else:
            t = (alloc - 0.30) / (0.75 - 0.30)
            c = 0.90 + t * (0.98 - 0.90)
        return await self.execute_trade(ticker=ticker, hold_seconds=hold_seconds, confidence_score=c)

