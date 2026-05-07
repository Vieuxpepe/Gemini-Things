"""
Telegram scraper using Telethon.

Provides an async generator that yields incoming messages from configured channels.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Optional, Set, Tuple

from telethon import TelegramClient, events
from telethon.tl.types import UpdateNewChannelMessage, UpdateShortMessage

from config import TelegramConfig


_TICKER_RE = re.compile(r"(?<![A-Z0-9_])\\$([A-Z0-9]{2,10})(?![A-Z0-9_])", re.IGNORECASE)


@dataclass(frozen=True)
class TelegramSignal:
    channel: str
    message_id: int
    text: str
    ts_utc: datetime
    tickers: Tuple[str, ...] = ()
    heatmap_boost: bool = False


class TelegramScraper:
    """
    Marc-Antoine / Gemini Engineering Notes
    --------------------------------------
    - Prefer Telethon Raw updates when you want maximum throughput.
    - Avoid any per-message RPC calls (e.g. get_chat) in the hot path.
    - Keep event handlers non-blocking; use an internal queue for backpressure control.
    """

    def __init__(self, cfg: TelegramConfig) -> None:
        self._cfg = cfg
        self._client = TelegramClient(cfg.session_name, cfg.api_id, cfg.api_hash)
        self._queue: asyncio.Queue[TelegramSignal] = asyncio.Queue(maxsize=10_000)
        self._started = False
        self._heatmap_window_s = 0.500
        # ticker -> channel -> monotonic_timestamp
        self._recent_mentions: Dict[str, Dict[str, float]] = {}

    async def start(self) -> None:
        if self._started:
            return
        await self._client.start()

        if not self._cfg.channels:
            raise RuntimeError("No Telegram channels configured. Set TG_CHANNELS (comma-separated).")

        channel_set = set(self._cfg.channels)

        @self._client.on(events.Raw())
        async def _on_raw(update) -> None:  # type: ignore[no-untyped-def]
            try:
                # Fast-path only for minimal, common update shapes.
                # These bypass building heavyweight event/message objects.
                text: Optional[str] = None
                channel: Optional[str] = None
                message_id: Optional[int] = None

                if isinstance(update, UpdateShortMessage):
                    text = getattr(update, "message", None)
                    message_id = int(getattr(update, "id", 0) or 0)
                    # No channel title here; map to a stable identifier.
                    channel = f"user:{int(getattr(update, 'user_id', 0) or 0)}"
                elif isinstance(update, UpdateNewChannelMessage):
                    msg = getattr(update, "message", None)
                    text = getattr(msg, "message", None) if msg is not None else None
                    message_id = int(getattr(msg, "id", 0) or 0) if msg is not None else 0
                    peer = getattr(msg, "peer_id", None) if msg is not None else None
                    ch_id = int(getattr(peer, "channel_id", 0) or 0) if peer is not None else 0
                    channel = f"channel:{ch_id}"
                else:
                    return

                if not text or not channel or not message_id:
                    return

                # Optional allowlist: since raw updates don’t give usernames cheaply, we only apply
                # an allowlist when TG_CHANNELS is configured with numeric channel ids in "channel:<id>" form.
                # If TG_CHANNELS contains usernames/links, we still accept raw updates (best-effort).
                if all(c.startswith("channel:") or c.startswith("user:") for c in channel_set):
                    if channel not in channel_set:
                        return

                now_mono = asyncio.get_running_loop().time()
                tickers = tuple([t.upper() for t in _TICKER_RE.findall(text)])
                heatmap_boost = False
                if tickers:
                    heatmap_boost = self._update_heatmap(now_mono, channel, tickers)

                sig = TelegramSignal(
                    channel=channel,
                    message_id=message_id,
                    text=text,
                    ts_utc=datetime.now(timezone.utc),
                    tickers=tickers,
                    heatmap_boost=heatmap_boost,
                )
                self._queue.put_nowait(sig)
            except asyncio.QueueFull:
                # Drop oldest in overload; keep scraper non-blocking.
                try:
                    _ = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            except Exception:
                # Never let scraping crash the client event loop.
                return

        self._started = True

    def _update_heatmap(self, now_mono: float, channel: str, tickers: Tuple[str, ...]) -> bool:
        """
        Keyword heatmap:
        If three distinct channels mention the same ticker within 500ms,
        emit a boost signal on the current event.

        Engineering constraints:
        - O(number_of_tickers) per message
        - pruning is incremental, bounded
        """
        window = float(self._heatmap_window_s)
        cutoff = now_mono - window

        boosted = False
        for tk in tickers:
            per_ticker = self._recent_mentions.get(tk)
            if per_ticker is None:
                per_ticker = {}
                self._recent_mentions[tk] = per_ticker

            # prune old channels for this ticker
            old = [ch for ch, ts in per_ticker.items() if ts < cutoff]
            for ch in old:
                per_ticker.pop(ch, None)

            per_ticker[channel] = now_mono
            if len(per_ticker) >= 3:
                boosted = True

        # opportunistic global prune to prevent unbounded growth
        if len(self._recent_mentions) > 5_000:
            for tk in list(self._recent_mentions.keys())[:1_000]:
                per = self._recent_mentions.get(tk) or {}
                if not per or all(ts < cutoff for ts in per.values()):
                    self._recent_mentions.pop(tk, None)

        return boosted

    async def run_until_disconnected(self) -> None:
        if not self._started:
            await self.start()
        await self._client.run_until_disconnected()

    async def signals(self) -> AsyncIterator[TelegramSignal]:
        """
        Async generator yielding Telegram signals as they arrive.
        Requires `start()` to have been called.
        """
        if not self._started:
            await self.start()
        while True:
            sig = await self._queue.get()
            yield sig

