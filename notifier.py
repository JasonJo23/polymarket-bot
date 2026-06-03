"""
notifier.py - TelegramNotifier

Lahettaa Telegram-ilmoitukset ostoista, myynneista ja virheista.
Kaikki dynaaminen teksti escapetaan, koska Telegramin HTML-tila hylkaa
viestin helposti jos markkinanimessa on esimerkiksi &, < tai >.
"""

import logging
import os
from datetime import datetime, timezone
from html import escape

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("Scout.Notifier")

TELEGRAM_API = "https://api.telegram.org"


def _utc_time() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M UTC")


def _text(value: object, limit: int = 80) -> str:
    return escape(str(value or "")[:limit])


class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            log.debug("Telegram ei kaytossa - lisaa TELEGRAM_BOT_TOKEN ja TELEGRAM_CHAT_ID")

    def send(self, message: str):
        if not self.enabled:
            return
        try:
            r = requests.post(
                f"{TELEGRAM_API}/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5,
            )
            if r.status_code != 200:
                log.warning(f"Telegram virhe: {r.text[:160]}")
        except Exception as e:
            log.warning(f"Telegram lahetys epaonnistui: {e}")

    def notify_buy(self, signal: dict, price: float, size: float, status: str):
        title = "OSTO" if str(status).lower() == "matched" else "OSTOORDERI AUKI"
        msg = (
            f"<b>{title}</b>\n"
            f"{_text(signal.get('question'), 60)}\n"
            f"Outcome: <b>{_text(signal.get('outcome'), 40)}</b>\n"
            f"{size:.2f} USDC @ {price:.3f}\n"
            f"Status: <b>{_text(str(status).upper(), 20)}</b>\n"
            f"Tuki: {signal.get('support_count', '?')} lompakon | "
            f"{float(signal.get('total_size_usdc', 0) or 0):.0f} USDC\n"
            f"{_utc_time()}"
        )
        self.send(msg)

    def notify_sell(self, question: str, pnl_pct: float, reason: str):
        msg = (
            f"<b>MYYNTI VAHVISTETTU</b>\n"
            f"{_text(question, 60)}\n"
            f"P&L: <b>{pnl_pct:+.1%}</b>\n"
            f"{_text(reason, 100)}\n"
            f"{_utc_time()}"
        )
        self.send(msg)

    def notify_sell_order_open(self, question: str, pnl_pct: float, reason: str, status: str):
        msg = (
            f"<b>MYYNTIORDERI AUKI</b>\n"
            f"{_text(question, 60)}\n"
            f"P&L: <b>{pnl_pct:+.1%}</b>\n"
            f"Status: <b>{_text(str(status).upper(), 30)}</b>\n"
            f"{_text(reason, 100)}\n"
            f"{_utc_time()}"
        )
        self.send(msg)

    def notify_bad_fill(
        self,
        signal: dict,
        requested_price: float,
        actual_price: float,
        slippage_pct: float,
        size: float,
    ):
        msg = (
            f"<b>BAD FILL</b>\n"
            f"{_text(signal.get('question'), 60)}\n"
            f"Outcome: <b>{_text(signal.get('outcome'), 40)}</b>\n"
            f"Pyydetty: {requested_price:.3f} | Toteutui: <b>{actual_price:.3f}</b>\n"
            f"Slippage: <b>{slippage_pct:+.1%}</b> | {size:.2f} USDC\n"
            f"{_utc_time()}"
        )
        self.send(msg)

    def notify_edge(
        self,
        question: str,
        outcome: str,
        our_prob: float,
        poly_price: float,
        edge: float,
        confidence: str,
    ):
        msg = (
            f"<b>EDGE LOYTYI</b>\n"
            f"{_text(question, 60)}\n"
            f"{_text(outcome, 40)}\n"
            f"Oma: {our_prob:.0%} | Poly: {poly_price:.0%} | "
            f"Edge: <b>{edge:+.0%}</b>\n"
            f"Confidence: {_text(confidence, 30)}\n"
            f"{_utc_time()}"
        )
        self.send(msg)

    def notify_error(self, message: str):
        self.send(f"<b>VIRHE</b>\n{_text(message, 240)}")

    def notify_daily_summary(self, spent: float, bankroll: float, signals: int):
        msg = (
            f"<b>PAIVAYHTEENVETO</b>\n"
            f"Kassa: {bankroll:.2f} USDC\n"
            f"Kaytetty: {spent:.2f} USDC\n"
            f"Signaaleja: {signals}\n"
            f"{datetime.now(timezone.utc).strftime('%d.%m.%Y UTC')}"
        )
        self.send(msg)

    def notify_cycle_summary(self, summary: dict):
        hours = float(summary.get("hours", 0.0) or 0.0)
        cycles = int(summary.get("cycles", 0) or 0)
        funnel = summary.get("funnel", {}) or {}
        msg = (
            f"<b>BOTTIYHTEENVETO</b> ({hours:.1f}h)\n"
            f"Syklit: {cycles} | Ostoyritykset: {int(summary.get('order_attempts', 0) or 0)}\n"
            f"Paivaosto: {float(summary.get('daily_spend', 0.0) or 0.0):.2f} USDC | "
            f"Live PnL: {float(summary.get('daily_pnl', 0.0) or 0.0):+.2f} USDC\n"
            f"Kassa: {float(summary.get('bankroll', 0.0) or 0.0):.2f} USDC | "
            f"Avoimet positiot: {int(summary.get('open_positions', 0) or 0)}\n"
            f"Kandidaatit: {int(summary.get('accepted', 0) or 0)} | "
            f"Jatkotark.: {int(summary.get('strong', 0) or 0)} | "
            f"Fresh: {int(funnel.get('fresh_spike_candidates', 0) or 0)}\n"
            f"Hylatty: hinta {int(funnel.get('price_extreme', 0) or 0)} | "
            f"wallet/size {int(funnel.get('wallet_quality_or_size', 0) or 0)} | "
            f"myoha/vola {int(funnel.get('late_or_volatile', 0) or 0)}\n"
            f"Avg sykli: {float(summary.get('avg_cycle_seconds', 0.0) or 0.0):.1f}s | "
            f"{_utc_time()}"
        )
        self.send(msg)


notifier = TelegramNotifier()
