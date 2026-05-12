"""
notifier.py – TelegramNotifier
Lähettää ilmoitukset Telegramiin ostoista, myynneistä ja virheistä.
"""

import os
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger("Scout.Notifier")

TELEGRAM_API = "https://api.telegram.org"


class TelegramNotifier:

    def __init__(self):
        self.token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            log.debug("Telegram ei käytössä — lisää TELEGRAM_BOT_TOKEN ja TELEGRAM_CHAT_ID")

    def send(self, message: str):
        if not self.enabled:
            return
        try:
            r = requests.post(
                f"{TELEGRAM_API}/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5
            )
            if r.status_code != 200:
                log.debug(f"Telegram virhe: {r.text[:100]}")
        except Exception as e:
            log.debug(f"Telegram lähetys epäonnistui: {e}")

    def notify_buy(self, signal: dict, price: float, size: float, status: str):
        emoji = "✅" if status == "matched" else "⏳"
        msg = (
            f"{emoji} <b>OSTO</b>\n"
            f"📊 {signal.get('question','')[:50]}\n"
            f"🎯 Outcome: <b>{signal.get('outcome','')}</b>\n"
            f"💰 {size:.2f} USDC @ {price:.3f}\n"
            f"👥 Tuki: {signal.get('support_count','?')} lompakon | "
            f"{signal.get('total_size_usdc',0):.0f} USDC\n"
            f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )
        self.send(msg)

    def notify_sell(self, question: str, pnl_pct: float, reason: str):
        emoji = "🟢" if pnl_pct > 0 else "🔴"
        msg = (
            f"{emoji} <b>MYYNTI</b>\n"
            f"📊 {question[:50]}\n"
            f"📈 P&L: <b>{pnl_pct:+.1%}</b>\n"
            f"💡 {reason[:80]}\n"
            f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )
        self.send(msg)

    def notify_edge(self, question: str, outcome: str, our_prob: float,
                    poly_price: float, edge: float, confidence: str):
        msg = (
            f"🔍 <b>EDGE LÖYTYI</b>\n"
            f"📊 {question[:50]}\n"
            f"🎯 {outcome}\n"
            f"🧠 Oma: {our_prob:.0%} | Poly: {poly_price:.0%} | "
            f"Edge: <b>{edge:+.0%}</b>\n"
            f"📊 Confidence: {confidence}\n"
            f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )
        self.send(msg)

    def notify_error(self, message: str):
        msg = f"⚠️ <b>VIRHE</b>\n{message[:200]}"
        self.send(msg)

    def notify_daily_summary(self, spent: float, bankroll: float, signals: int):
        msg = (
            f"📋 <b>PÄIVÄYHTEENVETO</b>\n"
            f"💰 Kassa: {bankroll:.2f} USDC\n"
            f"💸 Käytetty: {spent:.2f} USDC\n"
            f"🎯 Signaaleja: {signals}\n"
            f"📅 {datetime.now(timezone.utc).strftime('%d.%m.%Y UTC')}"
        )
        self.send(msg)


# Globaali instanssi — importataan muista tiedostoista
notifier = TelegramNotifier()