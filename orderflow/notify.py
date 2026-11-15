"""Telegram notifications (optional). Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.request

log = logging.getLogger("notify")


class Telegram:
    def __init__(self, token: str | None = None, chat_id: str | None = None):
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def _send(self, text: str) -> None:
        body = json.dumps({"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{self.token}/sendMessage", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        try:
            await asyncio.to_thread(self._send, text)
        except Exception as e:
            log.warning("telegram send failed: %s", e)
