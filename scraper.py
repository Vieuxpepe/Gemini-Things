"""
Telegram scraper using Telethon.

Provides an async generator that yields incoming messages from configured channels.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Iterable, Optional

from telethon import TelegramClient, events

from config import TelegramConfig


@dataclass(frozen=True)
class TelegramSignal:
    channel: str
    message_id: int
    text: str
    ts_utc: datetime


class TelegramScraper:
    def __init__(self, cfg: TelegramConfig) -> None:
        self._cfg = cfg
        self._client = TelegramClient(cfg.session_name, cfg.api_id, cfg.api_hash)
        self._queue: asyncio.Queue[TelegramSignal] = asyncio.Queue(maxsize=10_000)
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        await self._client.start()

        if not self._cfg.channels:
            raise RuntimeError("No Telegram channels configured. Set TG_CHANNELS (comma-separated).")

        @self._client.on(events.NewMessage(chats=list(self._cfg.channels)))
        async def _on_new_message(event: events.NewMessage.Event) -> None:  # type: ignore[name-defined]
            try:
                text = event.raw_text or ""
                if not text:
                    return
                chat = await event.get_chat()
                channel_name = getattr(chat, "username", None) or getattr(chat, "title", None) or "unknown"
                sig = TelegramSignal(
                    channel=str(channel_name),
                    message_id=int(event.message.id),
                    text=text,
                    ts_utc=datetime.now(timezone.utc),
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

