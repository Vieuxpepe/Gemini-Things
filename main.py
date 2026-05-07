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
    heatmap_boost: bool = False


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


class SerialRewardClient:
    """
    Persistent serial client to avoid per-trade COM port handshake latency.

    Marc-Antoine / Gemini Engineering Notes
    --------------------------------------
    - Serial IO must never block or crash the trading loop.
    - Keep writes short, newline-delimited, and tolerant of disconnects.
    """

    def __init__(self) -> None:
        self._cfg = load_serial_reward_config()
        self._ser: Optional[serial.Serial] = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        if not self._cfg.enabled:
            return
        if self._ser and self._ser.is_open:
            return
        try:
            self._ser = serial.Serial(
                port=self._cfg.port,
                baudrate=self._cfg.baudrate,
                timeout=0.2,
                write_timeout=self._cfg.write_timeout_s,
            )
        except Exception:
            self._ser = None

    async def close(self) -> None:
        if self._ser is None:
            return
        try:
            self._ser.close()
        except Exception:
            pass
        finally:
            self._ser = None

    async def send_pulse(self, profit_amount: float) -> None:
        await self._send({"command": "PULSE", "torque": 50, "profit": float(profit_amount)})

    async def send_overload(self, duration_ms: int = 5000) -> None:
        await self._send({"command": "OVERLOAD", "duration": int(duration_ms)})

    async def _send(self, payload: dict) -> None:
        if not self._cfg.enabled:
            return
        async with self._lock:
            if self._ser is None or not self._ser.is_open:
                await self.open()
            if self._ser is None or not self._ser.is_open:
                return
            try:
                data = (json.dumps(payload) + "\n").encode("utf-8")
                self._ser.write(data)
                self._ser.flush()
            except Exception:
                # Treat any error as a disconnect; next call will attempt reopen.
                try:
                    self._ser.close()
                except Exception:
                    pass
                self._ser = None


async def handle_signal(trader: ExchangeTrader, rewards: SerialRewardClient, sig: TelegramSignal) -> None:
    strat = load_strategy_config()
    scored = score_message(sig.text, tuple(strat.keywords), strat.min_confidence)
    if not scored:
        return

    ex_cfg = load_exchange_config()

    # Heatmap: if three distinct channels mention the same ticker within 500ms,
    # boost confidence to 1.0 (still subject to allocation caps and stop-loss).
    confidence = 1.0 if getattr(sig, "heatmap_boost", False) else scored.confidence

    try:
        buy, sell = await trader.execute_trade(
            ticker=scored.ticker,
            hold_seconds=strat.hold_seconds,
            confidence_score=confidence,
        )
    except Exception:
        return

    if ex_cfg.dry_run:
        return

    realized_profit = sell.pnl_quote
    if realized_profit is None or realized_profit <= 0:
        return

    await rewards.send_pulse(realized_profit)
    if realized_profit > 10.0:
        await rewards.send_overload(duration_ms=5000)


async def main() -> None:
    tg_cfg = load_telegram_config()
    ex_cfg = load_exchange_config()

    scraper = TelegramScraper(tg_cfg)
    trader = ExchangeTrader(ex_cfg)
    rewards = SerialRewardClient()

    await scraper.start()
    await rewards.open()

    try:
        async for sig in scraper.signals():
            # Fire-and-forget with backpressure handled by scraper queue.
            asyncio.create_task(handle_signal(trader, rewards, sig))
    finally:
        await rewards.close()
        await trader.close()


if __name__ == "__main__":
    asyncio.run(main())

