"""
Purpose
-------
Real-time sentiment-driven trading framework (safe-by-default).

This script:
- listens to Telegram channels via Telethon
- extracts token tickers from messages (e.g. "$DOGE")
- assigns a confidence score based on trigger keywords
- (optionally) places trades via ccxt async exchange client
- triggers an ESP32 "physical reward" pulse on profitable closes

Dependencies
------------
- Telethon
- ccxt (async_support)
- asyncio
- pyserial
- pandas, numpy (available for downstream analytics; not required in this minimal loop)

Safety
------
DRY_RUN is enabled by default. Set DRY_RUN=false only after validating:
- symbol mapping and market type (spot vs swap)
- your exchange permissions and risk controls
- your strategy logic and compliance constraints
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Optional, Tuple

import serial

from config import (
    load_exchange_config,
    load_serial_reward_config,
    load_strategy_config,
    load_telegram_config,
)
from scraper import TelegramScraper, TelegramSignal
from trader import ExchangeTrader


TICKER_RE = re.compile(r"(?<![A-Z0-9_])\\$([A-Z0-9]{2,10})(?![A-Z0-9_])", re.IGNORECASE)


@dataclass(frozen=True)
class ScoredTrigger:
    ticker: str
    confidence: float
    matched_keywords: Tuple[str, ...]


def score_message(text: str, keywords: tuple[str, ...], min_confidence: float) -> Optional[ScoredTrigger]:
    tickers = TICKER_RE.findall(text or "")
    if not tickers:
        return None

    upper = (text or "").upper()
    matched = tuple([k for k in keywords if k and k.upper() in upper])
    if not matched:
        return None

    # Deterministic, conservative confidence heuristic:
    # - base 0.75 for having a ticker + at least one keyword
    # - each additional keyword increases confidence modestly
    conf = min(0.75 + 0.08 * max(0, len(matched) - 1), 0.98)
    if conf < min_confidence:
        return None

    # Use first ticker in message; you can expand this to multi-ticker handling.
    return ScoredTrigger(ticker=tickers[0].upper(), confidence=conf, matched_keywords=matched)


def trigger_physical_reward(profit_amount: float) -> None:
    """
    Opens a serial connection and sends a PULSE payload to an ESP32.
    Intended to be called only on realized profitable trade closes.
    """
    cfg = load_serial_reward_config()
    if not cfg.enabled:
        return
    payload = {"command": "PULSE", "torque": 50, "profit": float(profit_amount)}
    data = (json.dumps(payload) + "\n").encode("utf-8")

    try:
        with serial.Serial(
            port=cfg.port,
            baudrate=cfg.baudrate,
            timeout=0.2,
            write_timeout=cfg.write_timeout_s,
        ) as ser:
            ser.write(data)
            ser.flush()
    except Exception:
        # Never let serial errors crash the trading loop.
        return


async def handle_signal(trader: ExchangeTrader, sig: TelegramSignal) -> None:
    strat = load_strategy_config()
    scored = score_message(sig.text, tuple(strat.keywords), strat.min_confidence)
    if not scored:
        return

    ex_cfg = load_exchange_config()

    # Allocation is capped again inside trader using config max limit.
    allocation_fraction = 0.50  # user-specified intent, but config enforces safer maximum by default.

    try:
        buy, sell = await trader.market_buy_then_sell(
            ticker=scored.ticker,
            hold_seconds=strat.hold_seconds,
            allocation_fraction=allocation_fraction,
        )
    except Exception:
        return

    # In a real system, compute PnL from fills + fees + position sizing.
    # Here we trigger the reward only if we can positively determine realized profit.
    if ex_cfg.dry_run:
        return

    # Placeholder: Without exchange-specific fill reconciliation, we cannot safely compute realized PnL.
    # If you want, we can add a proper PnL calculator using order fills / trades history per exchange.
    realized_profit = None
    if realized_profit is not None and realized_profit > 0:
        trigger_physical_reward(realized_profit)


async def main() -> None:
    tg_cfg = load_telegram_config()
    ex_cfg = load_exchange_config()

    scraper = TelegramScraper(tg_cfg)
    trader = ExchangeTrader(ex_cfg)

    await scraper.start()

    try:
        async for sig in scraper.signals():
            # Fire-and-forget with backpressure handled by scraper queue.
            asyncio.create_task(handle_signal(trader, sig))
    finally:
        await trader.close()


if __name__ == "__main__":
    asyncio.run(main())

