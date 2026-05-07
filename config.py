"""
Central configuration for the sentiment-driven trading framework.

SECURITY:
- Keep API keys/secrets out of source control.
- Prefer environment variables or a local, untracked config file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class TelegramConfig:
    api_id: int
    api_hash: str
    session_name: str = "sentiment_bot"

    # Channel identifiers can be usernames ("somechannel") or full t.me links.
    channels: Sequence[str] = ()


@dataclass(frozen=True)
class ExchangeConfig:
    exchange_id: str = "mexc"  # e.g. "mexc" or "bybit"
    api_key: str = ""
    api_secret: str = ""
    password: str | None = None

    # Safety defaults. Set dry_run=False only after you validate behavior end-to-end.
    dry_run: bool = True

    # Basic risk controls
    quote_currency: str = "USDT"
    max_quote_allocation_fraction: float = 0.10  # 10% by default (safer than 50%)

    # Many derivatives endpoints require account/market-specific params; this framework
    # keeps leverage handling optional and exchange-specific.
    requested_leverage: int | None = None

    # Timeouts (ms) for ccxt; keep tight but realistic.
    timeout_ms: int = 10_000


@dataclass(frozen=True)
class SerialRewardConfig:
    enabled: bool = True
    port: str = "COM3"
    baudrate: int = 115200
    write_timeout_s: float = 0.5


@dataclass(frozen=True)
class StrategyConfig:
    # Trigger keywords and minimum score.
    keywords: Sequence[str] = ("PUMP", "MOON", "LONG", "BREAKOUT")
    min_confidence: float = 0.90

    # Hold time is intentionally configurable.
    hold_seconds: float = 3.0


def load_telegram_config() -> TelegramConfig:
    api_id = int(os.environ.get("TG_API_ID", "0"))
    api_hash = os.environ.get("TG_API_HASH", "")
    session_name = os.environ.get("TG_SESSION_NAME", "sentiment_bot")

    channels_raw = os.environ.get("TG_CHANNELS", "")
    channels = tuple([c.strip() for c in channels_raw.split(",") if c.strip()])

    if not api_id or not api_hash:
        raise RuntimeError(
            "Telegram credentials missing. Set TG_API_ID and TG_API_HASH environment variables."
        )

    return TelegramConfig(api_id=api_id, api_hash=api_hash, session_name=session_name, channels=channels)


def load_exchange_config() -> ExchangeConfig:
    exchange_id = os.environ.get("EXCHANGE_ID", "mexc")
    api_key = os.environ.get("EXCHANGE_API_KEY", "")
    api_secret = os.environ.get("EXCHANGE_API_SECRET", "")
    password = os.environ.get("EXCHANGE_PASSWORD") or None

    dry_run = os.environ.get("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y")

    quote_currency = os.environ.get("QUOTE_CCY", "USDT")
    max_alloc = float(os.environ.get("MAX_ALLOC_FRACTION", "0.10"))

    lev_raw = os.environ.get("REQUESTED_LEVERAGE", "").strip()
    requested_leverage = int(lev_raw) if lev_raw else None

    timeout_ms = int(os.environ.get("CCXT_TIMEOUT_MS", "10000"))

    if not dry_run and (not api_key or not api_secret):
        raise RuntimeError(
            "Exchange credentials missing. Set EXCHANGE_API_KEY/EXCHANGE_API_SECRET (or enable DRY_RUN)."
        )

    if not (0.0 < max_alloc <= 1.0):
        raise RuntimeError("MAX_ALLOC_FRACTION must be in (0, 1].")

    return ExchangeConfig(
        exchange_id=exchange_id,
        api_key=api_key,
        api_secret=api_secret,
        password=password,
        dry_run=dry_run,
        quote_currency=quote_currency,
        max_quote_allocation_fraction=max_alloc,
        requested_leverage=requested_leverage,
        timeout_ms=timeout_ms,
    )


def load_serial_reward_config() -> SerialRewardConfig:
    enabled = os.environ.get("SERIAL_REWARD_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y")
    port = os.environ.get("SERIAL_PORT", "COM3")
    baudrate = int(os.environ.get("SERIAL_BAUD", "115200"))
    write_timeout_s = float(os.environ.get("SERIAL_WRITE_TIMEOUT_S", "0.5"))
    return SerialRewardConfig(enabled=enabled, port=port, baudrate=baudrate, write_timeout_s=write_timeout_s)


def load_strategy_config() -> StrategyConfig:
    keywords_raw = os.environ.get("TRIGGER_KEYWORDS", "PUMP,MOON,LONG,BREAKOUT")
    keywords = tuple([k.strip().upper() for k in keywords_raw.split(",") if k.strip()])
    min_confidence = float(os.environ.get("MIN_CONFIDENCE", "0.90"))
    hold_seconds = float(os.environ.get("HOLD_SECONDS", "3.0"))
    return StrategyConfig(keywords=keywords, min_confidence=min_confidence, hold_seconds=hold_seconds)

