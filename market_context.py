"""
=============================================================================
market_context.py – MarketContextFetcher  (v1.0)
=============================================================================
Hakee reaaliaikaisen kontekstin suoraan Polymarket API:sta:

  1. Gamma API → markkinan kuvaus, vastustaja, turnaus
  2. CLOB API  → token hinnat, order book laatu
  3. Crypto    → BTC/ETH live-hinta crypto-markkinoille

Nämä domainit toimivat Hetzner-serverillä koska botti jo käyttää niitä.

Konteksti välitetään probability_engine.py:lle → Claude saa oikean datan
analysointiin eikä arvaile.
=============================================================================
"""

import os
import re
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime, timezone

log = logging.getLogger("Scout.MarketContext")

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

# Sessio jaetaan — ei luoda uutta joka kutsulle
_session = requests.Session()
_session.headers.update({"User-Agent": "PolymarketScout/6.0", "Accept": "application/json"})

# Cache — ei haeta samaa markkinaa uudelleen per sykli
_context_cache: Dict[str, Dict] = {}


def get_market_context(
    question:     str,
    condition_id: str = "",
    token_id:     str = "",
    token_price:  float = 0.5,
) -> Dict[str, Any]:
    """
    Hakee markkinan kontekstin Polymarket API:sta.

    Returns:
        {
            "description":   str,   # Markkinan kuvaus
            "opponents":     str,   # "Team A vs Team B" jos löytyy
            "tournament":    str,   # Turnauksen nimi
            "crypto_price":  float, # BTC/ETH hinta jos relevantti
            "recent_prices": list,  # Viimeisimmät hinnat (price history)
            "context_text":  str,   # Valmis teksti Claudelle
            "data_quality":  float, # 0-1
        }
    """
    cache_key = condition_id or question[:50]
    if cache_key in _context_cache:
        return _context_cache[cache_key]

    result = {
        "description":   "",
        "opponents":     "",
        "tournament":    "",
        "crypto_price":  0.0,
        "recent_prices": [],
        "context_text":  "",
        "data_quality":  0.0,
    }

    quality = 0.0

    # 1. Gamma API — markkinan kuvaus ja metatiedot
    gamma_data = _fetch_gamma_market(condition_id, question)
    if gamma_data:
        result["description"] = gamma_data.get("description", "")[:300]
        result["tournament"]  = _extract_tournament(gamma_data)
        result["opponents"]   = _extract_opponents(question, gamma_data)
        quality += 0.4

    # 2. Crypto-hinta jos relevantti
    crypto_price = _fetch_crypto_price(question)
    if crypto_price > 0:
        result["crypto_price"] = crypto_price
        quality += 0.3

    # 3. Price history CLOB:sta
    if token_id:
        recent = _fetch_price_history(token_id, token_price)
        if recent:
            result["recent_prices"] = recent
            quality += 0.3

    # 4. Muodosta kontekstitekstiä Claudelle
    result["context_text"] = _build_context_text(result, question)
    result["data_quality"]  = round(min(1.0, quality), 2)

    _context_cache[cache_key] = result
    log.info(
        f"Konteksti: '{question[:40]}' | "
        f"quality={result['data_quality']:.1f} | "
        f"opponents='{result['opponents']}' | "
        f"crypto={result['crypto_price']:.0f}"
    )
    return result


def clear_context_cache():
    """Tyhjentää cachen syklin alussa."""
    _context_cache.clear()


# ===========================================================================
# Gamma API
# ===========================================================================

def _fetch_gamma_market(condition_id: str, question: str) -> Optional[Dict]:
    """Hakee markkinan tiedot Gamma API:sta."""
    try:
        # Hae condition_id:llä
        if condition_id:
            r = _session.get(
                f"{GAMMA_BASE}/markets",
                params={"condition_id": condition_id},
                timeout=5
            )
            if r.status_code == 200:
                data = r.json()
                markets = data if isinstance(data, list) else data.get("markets", [])
                if markets:
                    match = _best_question_match(question, markets, min_score=0.35)
                    if match:
                        return match
                    log.debug(f"Gamma condition_id hylätty: ei vastaa kysymystä '{question[:80]}'")

        # Fallback: hae kysymyksen perusteella
        r = _session.get(
            f"{GAMMA_BASE}/markets",
            params={"question": question[:80], "limit": 5, "active": "true"},
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            match = _best_question_match(question, markets)
            if match:
                return match
            if markets:
                log.debug(f"Gamma fallback hylätty: ei riittävää osumaa kysymykseen '{question[:80]}'")

    except Exception as e:
        log.debug(f"Gamma API haku epäonnistui: {e}")
    return None


def _important_tokens(text: str) -> set:
    """Palauttaa vertailuun kelpaavat sanat markkinakysymyksestä."""
    stop_words = {
        "will", "the", "and", "for", "with", "from", "this", "that",
        "market", "before", "after", "over", "under", "winner", "game",
        "match", "series", "round", "may", "jun", "jul", "aug", "sep",
        "oct", "nov", "dec", "jan", "feb", "mar", "apr", "2026",
    }
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
    return {
        token
        for token in normalized.split()
        if len(token) >= 3 and token not in stop_words and not token.isdigit()
    }


def _question_match_score(source_question: str, candidate_question: str) -> float:
    """Laskee kuinka hyvin Gamma-tulos vastaa alkuperäistä markkinaa."""
    source_tokens = _important_tokens(source_question)
    candidate_tokens = _important_tokens(candidate_question)
    if not source_tokens or not candidate_tokens:
        return 0.0
    return len(source_tokens & candidate_tokens) / max(len(source_tokens), 1)


def _best_question_match(question: str, markets: list, min_score: float = None) -> Optional[Dict]:
    """Valitsee parhaan Gamma-tuloksen vain jos osuma on riittävän vahva."""
    best = None
    best_score = 0.0
    for market in markets:
        candidate_text = " ".join([
            str(market.get("question", "")),
            str(market.get("title", "")),
            str(market.get("slug", "")),
        ])
        score = _question_match_score(question, candidate_text)
        if score > best_score:
            best = market
            best_score = score

    if min_score is None:
        min_score = float(os.getenv("GAMMA_FALLBACK_MIN_MATCH", "0.55"))
    if best and best_score >= min_score:
        return best

    return None


def _extract_opponents(question: str, gamma_data: Dict) -> str:
    """Erottaa vastustajat kysymyksestä tai Gamma-datasta."""
    # Suora vs-muoto kysymyksessä
    vs_match = re.search(r'(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\|]|$)', question, re.IGNORECASE)
    if vs_match:
        home = re.sub(r'^[A-Z]+[0-9]*:\s*', '', vs_match.group(1)).strip()
        away = vs_match.group(2).strip()
        return f"{home} vs {away}"

    # Gamma description saattaa sisältää vastustajan
    desc = gamma_data.get("description", "")
    vs_match2 = re.search(r'(.+?)\s+(?:vs\.?|against|facing)\s+(.+?)[\.\,\n]', desc, re.IGNORECASE)
    if vs_match2:
        return f"{vs_match2.group(1).strip()} vs {vs_match2.group(2).strip()}"

    return ""


def _extract_tournament(gamma_data: Dict) -> str:
    """Erottaa turnauksen nimen."""
    tags = gamma_data.get("tags", [])
    if tags:
        if isinstance(tags[0], dict):
            return tags[0].get("label", "")
        return str(tags[0])

    # Yritetään erottaa kysymyksestä sulkeiden sisältä
    question = gamma_data.get("question", "")
    paren_match = re.search(r'\(([^)]+)\)', question)
    if paren_match:
        return paren_match.group(1)

    return ""


# ===========================================================================
# Crypto-hinnat
# ===========================================================================

def _fetch_crypto_price(question: str) -> float:
    """
    Hakee kryptovaluutan live-hinnan jos markkina on relevantti.
    Käyttää data-api.polymarket.com:ia joka on sallittu.
    """
    q = question.lower()

    # Tunnista mikä crypto on kyseessä
    symbol = ""
    if "bitcoin" in q or "btc" in q:
        symbol = "BTC"
    elif "ethereum" in q or "eth" in q:
        symbol = "ETH"
    elif "solana" in q or "sol" in q:
        symbol = "SOL"
    else:
        return 0.0

    # Hae hinta Polymarket CLOB:n kautta — ne tietävät BTC:n hinnan
    # koska niillä on crypto-markkinoita
    try:
        # Hae aktiiviset BTC-markkinat josta voi päätellä hintatason
        r = _session.get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit":     5,
                "active":    "true",
                "closed":    "false",
                "order":     "volume24hr",
                "ascending": "false",
            },
            timeout=5
        )
        if r.status_code == 200:
            markets = r.json() if isinstance(r.json(), list) else []
            for m in markets:
                mq = m.get("question", "").lower()
                if symbol.lower() in mq and "above" in mq:
                    # Etsi hintaraja kysymyksestä: "Will BTC be above $78,000"
                    price_match = re.search(r'\$([0-9,]+)', m.get("question", ""))
                    if price_match:
                        threshold = float(price_match.group(1).replace(",", ""))
                        # Outcome-hinta kertoo markkinoiden arvion
                        outcomes = m.get("outcomePrices", "[]")
                        if isinstance(outcomes, str):
                            import json
                            try:
                                outcomes = json.loads(outcomes)
                            except Exception:
                                outcomes = []
                        if outcomes and len(outcomes) >= 2:
                            yes_price = float(outcomes[0]) if outcomes else 0.5
                            # Jos YES @ 0.85 → markkinat uskovat BTC yli rajan
                            # Jos YES @ 0.15 → BTC todennäköisesti alle rajan
                            log.debug(f"{symbol} threshold=${threshold:,.0f} yes_price={yes_price:.2f}")
                            return threshold  # Palauta kynnysarvo kontekstiksi
    except Exception as e:
        log.debug(f"Crypto-hinta haku epäonnistui: {e}")

    return 0.0


# ===========================================================================
# Price history
# ===========================================================================

def _fetch_price_history(token_id: str, current_price: float) -> list:
    """Hakee token-hinnan historian CLOB:sta."""
    try:
        r = _session.get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": "1d", "fidelity": 5},
            timeout=5
        )
        if r.status_code == 200:
            history = r.json().get("history", [])
            if len(history) >= 2:
                prices = [float(h.get("p", current_price)) for h in history[-5:]]
                trend = "nouseva" if prices[-1] > prices[0] else "laskeva"
                return {
                    "oldest": round(prices[0], 3),
                    "newest": round(prices[-1], 3),
                    "trend":  trend,
                    "change": round(prices[-1] - prices[0], 3),
                }
    except Exception as e:
        log.debug(f"Price history haku epäonnistui: {e}")
    return []


# ===========================================================================
# Kontekstitekstin muodostus
# ===========================================================================

def _build_context_text(result: Dict, question: str) -> str:
    """Muodostaa selkeän kontekstitekstin Claudelle."""
    parts = []

    if result["opponents"]:
        parts.append(f"Ottelu: {result['opponents']}")

    if result["tournament"]:
        parts.append(f"Turnaus: {result['tournament']}")

    if result["description"]:
        parts.append(f"Kuvaus: {result['description'][:200]}")

    if result["crypto_price"] > 0:
        parts.append(f"Markkinoiden kynnysarvo: ${result['crypto_price']:,.0f}")

    if result["recent_prices"] and isinstance(result["recent_prices"], dict):
        p = result["recent_prices"]
        parts.append(
            f"Hintahistoria: {p['oldest']:.3f} → {p['newest']:.3f} "
            f"({p['trend']}, muutos {p['change']:+.3f})"
        )

    if not parts:
        return "Ei lisäkontekstia saatavilla Polymarket API:sta."

    return "\n".join(parts)
