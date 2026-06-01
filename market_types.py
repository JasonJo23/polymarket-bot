"""
Shared market classification and risk bounds.

Keep domain heuristics here so signal building, intelligence, edge detection,
scoring, and position management classify the same market the same way.
"""

from __future__ import annotations

import os
from typing import Tuple

MARKET_MACRO = "macro"
MARKET_SPORTS = "sports"
MARKET_ESPORTS_MATCH = "esports_match"
MARKET_ESPORTS_MAP = "esports_map"
MARKET_GENERAL = "general"

ESPORTS_KEYWORDS = [
    "lol:", "league of legends", "dota", "cs2", "csgo", "counter-strike",
    "valorant", "esports", "lck", "lec", "lpl", "lcs", "vct", "iem",
    "pgl", "blast", "dreamleague", "esl",
]

ESPORTS_MAP_KEYWORDS = ["game ", "map "]

SPORTS_KEYWORDS = [
    "vs.", "vs ", "winner", "match", "series", "spread", "o/u",
    "nba", "nfl", "nhl", "mlb", "wnba", "atp", "wta", "ufc", "mma",
    "fight night", "boxing", "fc ", "epl", "bundesliga", "serie a",
    "la liga", "premier league", "champions league", "innings",
    "lakers", "celtics", "knicks", "thunder", "spurs", "76ers",
    "cavaliers", "pistons", "timberwolves", "warriors", "mavericks",
    "nuggets", "pacers", "bucks", "heat", "magic", "raptors",
    "yankees", "dodgers", "red sox", "orioles", "oilers", "panthers",
    "rangers", "barcelona", "madrid", "arsenal", "chelsea", "liverpool",
    "manchester", "juventus", "milan", "inter", "napoli", "bayern",
]

MACRO_KEYWORDS = [
    "trump", "biden", "election", "fed", "powell", "rate", "tariff",
    "tariffs", "iran", "ceasefire", "war", "congress", "senate",
    "president", "policy", "agreement", "deal", "treaty", "sanctions",
    "nuclear", "btc", "eth", "bitcoin", "ethereum", "crypto", "gdp",
    "inflation", "treasury", "recession", "default", "elon", "musk",
    "apple", "nvidia", "tesla", "nasdaq", "uranium", "peace deal",
]


def classify_market(question: str) -> str:
    q = (question or "").lower()
    if any(keyword in q for keyword in ESPORTS_KEYWORDS):
        if any(keyword in q for keyword in ESPORTS_MAP_KEYWORDS):
            return MARKET_ESPORTS_MAP
        return MARKET_ESPORTS_MATCH
    if any(keyword in q for keyword in SPORTS_KEYWORDS):
        return MARKET_SPORTS
    if any(keyword in q for keyword in MACRO_KEYWORDS):
        return MARKET_MACRO
    return MARKET_GENERAL


def is_sports(question: str) -> bool:
    return classify_market(question) == MARKET_SPORTS


def is_esports(question: str) -> bool:
    return classify_market(question) in (MARKET_ESPORTS_MATCH, MARKET_ESPORTS_MAP)


def is_esports_map(question: str) -> bool:
    return classify_market(question) == MARKET_ESPORTS_MAP


def price_bounds(market_type: str, *, relaxed: bool = False) -> Tuple[float, float]:
    bounds = {
        MARKET_MACRO: (
            float(os.getenv("MACRO_MIN_TOKEN_PRICE", 0.20)),
            float(os.getenv("MACRO_MAX_TOKEN_PRICE", 0.85)),
        ),
        MARKET_SPORTS: (
            float(os.getenv("SPORTS_MIN_TOKEN_PRICE", 0.25)),
            float(os.getenv("SPORTS_MAX_TOKEN_PRICE", 0.85)),
        ),
        MARKET_ESPORTS_MATCH: (
            float(os.getenv("ESPORTS_MATCH_MIN_TOKEN_PRICE", 0.30)),
            float(os.getenv("ESPORTS_MATCH_MAX_TOKEN_PRICE", 0.78)),
        ),
        MARKET_ESPORTS_MAP: (
            float(os.getenv("ESPORTS_MAP_MIN_TOKEN_PRICE", 0.35)),
            float(os.getenv("ESPORTS_MAP_MAX_TOKEN_PRICE", 0.70)),
        ),
        MARKET_GENERAL: (
            float(os.getenv("GENERAL_MIN_TOKEN_PRICE", 0.25)),
            float(os.getenv("GENERAL_MAX_TOKEN_PRICE", 0.80)),
        ),
    }
    low, high = bounds.get(market_type, bounds[MARKET_GENERAL])
    if relaxed:
        buffer = float(os.getenv("CANDIDATE_PRICE_BUFFER", 0.02))
        return max(0.01, low - buffer), min(0.99, high + buffer)
    return low, high


def order_cap(market_type: str, default_cap: float) -> float:
    caps = {
        MARKET_MACRO: float(os.getenv("MACRO_MAX_ORDER_SIZE_USDC", default_cap)),
        MARKET_SPORTS: float(os.getenv("SPORTS_MAX_ORDER_SIZE_USDC", default_cap)),
        MARKET_ESPORTS_MATCH: float(os.getenv("ESPORTS_MATCH_MAX_ORDER_SIZE_USDC", 25)),
        MARKET_ESPORTS_MAP: float(os.getenv("ESPORTS_MAP_MAX_ORDER_SIZE_USDC", 15)),
        MARKET_GENERAL: float(os.getenv("GENERAL_MAX_ORDER_SIZE_USDC", 20)),
    }
    return caps.get(market_type, caps[MARKET_GENERAL])


def confidence_multiplier(market_type: str) -> float:
    if market_type == MARKET_MACRO:
        return 1.0
    if market_type in (MARKET_SPORTS, MARKET_ESPORTS_MATCH, MARKET_ESPORTS_MAP):
        return 0.7
    return 0.9
