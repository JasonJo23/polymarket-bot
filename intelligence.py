"""
=============================================================================
intelligence.py – MarketIntelligence  (v1.0 – Likviditeetti + Momentum)
=============================================================================
Vaihe 1: Kaksi mitattavaa signaalia ennen ostopäätöstä

SIGNAALIT:
  1. Markkinan laatu (CLOB order book)
     - Bid-ask spread
     - Likviditeettisyvyys
     → market_quality_score (0–1)

  2. Hintamomentum
     - Hinnan muutos viimeisen tunnin aikana
     - Liikkuuko hinta signaalin suuntaan vai vastaan?
     → momentum_score (0–1)

  3. Kategoria-suodatus
     - Urheilu → korkeampi kynnys (enemmän tukea vaaditaan)
     - Politiikka/crypto/talous → normaali kynnys
     → category_multiplier (0.5–1.0)

CONFIDENCE SCORE (0–100):
  confidence = (0.5 * market_quality + 0.5 * momentum) * 100 * category_multiplier

P�ÄTÖS:
  - confidence >= MIN_CONFIDENCE → sallitaan osto
  - Heikko likviditeetti → EI KAUPPAA riippumatta muusta
  - Hinta liikkuu vastaan → EI KAUPPAA

MISSÄ TÄMÄ VOI EPÄONNISTUA:
  1. Momentum on lyhytaikainen — 1h muutos ei ennusta tulosta
  2. Urheilu-tunnistus perustuu avainsanoihin, voi missata joitain
  3. Order book syvyys ei kerro manipulaatiosta

=============================================================================
"""

import logging
import requests
from typing import Dict, Any, Optional, Tuple

log = logging.getLogger("Scout.Intelligence")

CLOB_BASE = "https://clob.polymarket.com"

# Urheilumarkkinoiden tunnistusavainsanat
SPORTS_KEYWORDS = [
    "vs.", "vs ", "game 1", "game 2", "game 3", "bo3", "bo5",
    "nba", "nfl", "nhl", "mlb", "wnba", "epl", "lol:", "dota",
    "csgo", "valorant", "winner", "match", "series",
    "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
    "thunder", "pistons", "magic", "rockets", "spurs"
]

# Politiikka/makro avainsanat — parempi edge
HIGH_QUALITY_KEYWORDS = [
    "trump", "biden", "election", "fed", "bitcoin", "ethereum",
    "btc", "eth", "crypto", "gdp", "inflation", "iran", "ceasefire",
    "war", "congress", "senate", "president", "will", "rate"
]


def _detect_category(question: str) -> Tuple[str, float]:
    """
    Tunnistaa markkinan kategorian kysymystekstin perusteella.
    
    Palauttaa:
        category: "sports" | "politics" | "crypto" | "general"
        multiplier: kerroin confidence scorelle (sports = 0.7, muut = 1.0)
    """
    q = question.lower()
    
    for kw in SPORTS_KEYWORDS:
        if kw in q:
            return "sports", 0.7  # Urheilussa vaaditaan 30% korkeampi signaali
    
    for kw in HIGH_QUALITY_KEYWORDS:
        if kw in q:
            return "politics/macro", 1.0
    
    return "general", 0.9


def _get_order_book_quality(token_id: str) -> float:
    """
    Analysoi markkinan laadun CLOB order bookin perusteella.
    
    Mittaa:
    - Bid-ask spread (pienempi = parempi)
    - Likviditeettisyvyys (suurempi = parempi)
    
    Palauttaa: market_quality_score (0.0–1.0)
    """
    try:
        r = requests.get(
            f"{CLOB_BASE}/book",
            params={"token_id": token_id},
            timeout=5
        )
        if r.status_code != 200:
            log.debug(f"Order book haku epäonnistui: {r.status_code}")
            return 0.5  # Neutraali jos ei dataa

        book = r.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])

        if not bids or not asks:
            log.debug("Tyhjä order book — heikko likviditeetti")
            return 0.2  # Heikko likviditeetti

        # Paras bid ja ask hinta
        best_bid = float(bids[0].get("price", 0)) if bids else 0
        best_ask = float(asks[0].get("price", 1)) if asks else 1

        # Spread (0 = täydellinen, 0.1+ = huono)
        spread = best_ask - best_bid
        spread_score = max(0, 1 - (spread / 0.05))  # 5% spread = score 0

        # Syvyys — laske top 5 tason yhteisvolyymi
        bid_depth = sum(float(b.get("size", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("size", 0)) for a in asks[:5])
        total_depth = bid_depth + ask_depth
        depth_score = min(1.0, total_depth / 1000)  # 1000 tokenia = täysi syvyys

        quality = (0.6 * spread_score) + (0.4 * depth_score)
        log.debug(f"Order book: spread={spread:.3f} depth={total_depth:.0f} → quality={quality:.2f}")
        return round(quality, 3)

    except Exception as e:
        log.debug(f"Order book analyysi epäonnistui: {e}")
        return 0.5


def _get_price_momentum(token_id: str, current_price: float, signal_outcome: str) -> float:
    """
    Analysoi hinnan suunnan viimeisimmistä kaupoista.
    
    Logiikka:
    - Jos hinta nousee → signaali on vahvistumassa → parempi osta nyt
    - Jos hinta laskee → signaali heikkenee → odota tai ohita
    - Tasainen hinta → neutraali
    
    Palauttaa: momentum_score (0.0–1.0)
      0.0 = hinta liikkuu vahvasti vastaan
      0.5 = neutraali
      1.0 = hinta liikkuu vahvasti signaalia tukien
    """
    try:
        r = requests.get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": "1h", "fidelity": 10},
            timeout=5
        )
        if r.status_code != 200:
            return 0.5  # Neutraali jos ei historiaa

        data = r.json()
        history = data.get("history", [])

        if len(history) < 2:
            return 0.5  # Liian vähän dataa

        # Hae ensimmäinen ja viimeinen hinta tunnin ajalta
        oldest_price = float(history[0].get("p", current_price))
        price_change = current_price - oldest_price

        # Muunna muutos scoreksi
        # +0.05 muutos = score 0.9 (hinta nousee = vahvistuu)
        # -0.05 muutos = score 0.1 (hinta laskee = heikkenee)
        normalized = price_change / 0.10  # Normalisoi ±10% muutokseen
        score = max(0.0, min(1.0, 0.5 + normalized))

        log.debug(f"Momentum: {oldest_price:.3f} → {current_price:.3f} (Δ{price_change:+.3f}) → score={score:.2f}")
        return round(score, 3)

    except Exception as e:
        log.debug(f"Momentum analyysi epäonnistui: {e}")
        return 0.5


def analyze_signal(signal: Dict[str, Any], token_id: str, token_price: float) -> Dict[str, Any]:
    """
    Pääfunktio — analysoi signaali ennen ostopäätöstä.
    
    Input:
        signal: tracker.py:n tuottama signaali
        token_id: CLOB token ID
        token_price: nykyinen hinta
    
    Output:
        {
            "approved": bool,           # Sallitaanko osto
            "confidence": float,        # 0–100
            "category": str,            # Markkinakategoria
            "market_quality": float,    # 0–1
            "momentum": float,          # 0–1
            "reason": str               # Selitys päätökselle
        }
    """
    import os
    min_confidence = float(os.getenv("MIN_CONFIDENCE", 55))
    min_liquidity  = float(os.getenv("MIN_LIQUIDITY", 0.3))

    question = signal.get("question", "")
    outcome  = signal.get("outcome", "")

    # 1. Kategoria-tunnistus
    category, cat_multiplier = _detect_category(question)

    # 2. Markkinan laatu
    market_quality = _get_order_book_quality(token_id)

    # 3. Hintamomentum
    momentum = _get_price_momentum(token_id, token_price, outcome)

    # 4. Confidence score
    raw_confidence = (0.5 * market_quality + 0.5 * momentum) * 100
    confidence = round(raw_confidence * cat_multiplier, 1)

    # 5. Päätöslogiikka
    reason = ""
    approved = True

    if market_quality < min_liquidity:
        approved = False
        reason = f"Heikko likviditeetti ({market_quality:.2f} < {min_liquidity})"
    elif momentum < 0.25:
        approved = False
        reason = f"Hinta liikkuu vahvasti vastaan (momentum={momentum:.2f})"
    elif confidence < min_confidence:
        approved = False
        reason = f"Confidence liian matala ({confidence:.1f} < {min_confidence})"
    else:
        reason = f"OK — confidence={confidence:.1f} quality={market_quality:.2f} momentum={momentum:.2f} [{category}]"

    result = {
        "approved":       approved,
        "confidence":     confidence,
        "category":       category,
        "market_quality": market_quality,
        "momentum":       momentum,
        "cat_multiplier": cat_multiplier,
        "reason":         reason
    }

    if approved:
        log.info(f"✅ Intelligence: {question[:40]} → {reason}")
    else:
        log.warning(f"❌ Intelligence hylkäsi: {question[:40]} → {reason}")

    return result