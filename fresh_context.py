"""
FreshContextFetcher

Adds current sports/esports context for Claude without changing trading rules.
All sources are best-effort: missing API keys or failed requests simply produce
empty context so the bot can continue normally.
"""

import os
import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import requests
try:
    import feedparser
except Exception:
    feedparser = None

log = logging.getLogger("Scout.FreshContext")

PANDASCORE_BASE = "https://api.pandascore.co"
MYSPORTS_BASE = "https://api.mysportsfeeds.com/v2.1/pull"

ESPN_FEEDS = {
    "nba": "https://www.espn.com/espn/rss/nba/news",
    "nhl": "https://www.espn.com/espn/rss/nhl/news",
    "mlb": "https://www.espn.com/espn/rss/mlb/news",
    "nfl": "https://www.espn.com/espn/rss/nfl/news",
    "soccer": "https://www.espn.com/espn/rss/soccer/news",
    "esports": "https://www.espn.com/espn/rss/esports/news",
    "general": "https://www.espn.com/espn/rss/news",
}


class FreshContextFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "PolymarketScout/7.0",
        })
        self._cache: Dict[str, Dict] = {}

    def get_context(self, question: str, market_type: str = "") -> Dict[str, Any]:
        if os.getenv("FRESH_CONTEXT_ENABLED", "true").lower() != "true":
            return self._empty()

        cache_key = f"{market_type}|{question}".lower()
        if cache_key in self._cache:
            return self._cache[cache_key]

        category = self._detect_category(question, market_type)
        if category not in ("sports", "esports"):
            return self._empty()

        context = self._empty()
        context["category"] = category
        context["source"] = []
        teams = self._extract_teams(question)
        context["teams"] = teams

        if category == "esports":
            esports = self._fetch_pandascore(question, teams)
            self._merge(context, esports)
            rss = self._fetch_espn_news(question, "esports")
            self._merge(context, rss)
        else:
            sports = self._fetch_mysportsfeeds(question, teams)
            self._merge(context, sports)
            rss = self._fetch_espn_news(question, self._detect_sport_feed(question))
            self._merge(context, rss)

        context["context_text"] = self._build_text(context)
        context["data_quality"] = self._quality(context)
        context["fetched_at"] = datetime.now(timezone.utc).isoformat()
        self._cache[cache_key] = context

        if context["context_text"]:
            log.info(
                f"Fresh data: '{question[:40]}' | quality={context['data_quality']:.1f} | "
                f"sources={','.join(context['source'])}"
            )
        else:
            log.debug(f"Fresh data ei löytynyt: {question[:40]}")

        return context

    def clear_cache(self):
        self._cache.clear()

    def _empty(self) -> Dict[str, Any]:
        return {
            "category": "",
            "source": [],
            "teams": {},
            "matches": [],
            "injuries": [],
            "news": [],
            "context_text": "",
            "data_quality": 0.0,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    def _merge(self, target: Dict[str, Any], update: Dict[str, Any]):
        if not update:
            return
        for source in update.get("source", []):
            if source not in target["source"]:
                target["source"].append(source)
        for key in ("matches", "injuries", "news"):
            target[key].extend(update.get(key, []))

    def _detect_category(self, question: str, market_type: str) -> str:
        if market_type in ("esports_match", "esports_map"):
            return "esports"
        if market_type == "sports":
            return "sports"
        q = question.lower()
        if any(k in q for k in ["lol:", "dota", "cs2", "csgo", "valorant", "lck", "lec", "lpl"]):
            return "esports"
        if any(k in q for k in ["vs.", "vs ", "nba", "nhl", "mlb", "nfl", "fc ", "o/u", "spread"]):
            return "sports"
        return ""

    def _detect_sport_feed(self, question: str) -> str:
        q = question.lower()
        if any(k in q for k in ["nba", "lakers", "knicks", "thunder", "spurs", "celtics", "cavaliers"]):
            return "nba"
        if any(k in q for k in ["nhl", "canadiens", "hurricanes", "oilers", "panthers"]):
            return "nhl"
        if any(k in q for k in ["mlb", "yankees", "dodgers", "pirates", "cardinals"]):
            return "mlb"
        if any(k in q for k in ["nfl", "chiefs", "eagles", "cowboys"]):
            return "nfl"
        if any(k in q for k in ["fc ", "premier league", "la liga", "juventus", "barcelona"]):
            return "soccer"
        return "general"

    def _extract_teams(self, question: str) -> Dict[str, str]:
        q = re.sub(r"\s+", " ", question).strip()
        match = re.search(r"(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\-:|]|$)", q, re.IGNORECASE)
        if match:
            home = re.sub(r"^[A-Za-z0-9 ]+:\s*", "", match.group(1)).strip()
            away = match.group(2).strip()
            return {"home": home, "away": away}
        will_match = re.search(r"Will\s+(.+?)\s+(?:win|beat|score)", q, re.IGNORECASE)
        if will_match:
            return {"home": will_match.group(1).strip()}
        return {}

    def _fetch_pandascore(self, question: str, teams: Dict[str, str]) -> Dict[str, Any]:
        token = os.getenv("PANDASCORE_API_KEY", "")
        if not token:
            return {}

        game = self._detect_esport_game(question)
        endpoints = ["/v2/matches/upcoming"]
        if game:
            endpoints.insert(0, f"/v2/{game}/matches/upcoming")

        headers = {"Authorization": f"Bearer {token}"}
        params = {"per_page": int(os.getenv("PANDASCORE_MATCH_LIMIT", 20))}

        for endpoint in endpoints:
            try:
                r = self.session.get(
                    f"{PANDASCORE_BASE}{endpoint}",
                    headers=headers,
                    params=params,
                    timeout=8,
                )
                if r.status_code != 200:
                    log.debug(f"PandaScore {r.status_code}: {r.text[:120]}")
                    continue
                matches = self._select_pandascore_matches(r.json(), question, teams)
                if matches:
                    return {"source": ["PandaScore"], "matches": matches}
            except Exception as e:
                log.debug(f"PandaScore haku epäonnistui: {e}")
        return {}

    def _detect_esport_game(self, question: str) -> str:
        q = question.lower()
        if "lol:" in q or "league of legends" in q or "lck" in q or "lec" in q or "lpl" in q:
            return "lol"
        if "dota" in q:
            return "dota2"
        if "valorant" in q:
            return "valorant"
        if "cs2" in q or "csgo" in q or "counter-strike" in q:
            return "csgo"
        return ""

    def _select_pandascore_matches(self, data: Any, question: str, teams: Dict[str, str]) -> List[Dict]:
        if not isinstance(data, list):
            return []
        wanted = self._tokens(" ".join(teams.values()) or question)
        selected = []
        for match in data:
            name = str(match.get("name") or match.get("slug") or "")
            opponents = []
            for item in match.get("opponents", []) or []:
                opp = item.get("opponent") or {}
                if opp.get("name"):
                    opponents.append(str(opp["name"]))
            haystack = " ".join([name] + opponents)
            if wanted and not (wanted & self._tokens(haystack)):
                continue
            selected.append({
                "name": name,
                "opponents": opponents,
                "league": (match.get("league") or {}).get("name", ""),
                "serie": (match.get("serie") or {}).get("full_name", ""),
                "tournament": (match.get("tournament") or {}).get("name", ""),
                "begin_at": match.get("begin_at", ""),
                "status": match.get("status", ""),
                "number_of_games": match.get("number_of_games", ""),
            })
            if len(selected) >= 3:
                break
        return selected

    def _fetch_mysportsfeeds(self, question: str, teams: Dict[str, str]) -> Dict[str, Any]:
        api_key = os.getenv("MYSPORTSFEEDS_API_KEY", "")
        if not api_key:
            return {}

        league = self._detect_mysports_league(question)
        if not league:
            return {}

        password = os.getenv("MYSPORTSFEEDS_PASSWORD", "MYSPORTSFEEDS")
        auth = (api_key, password)
        updates = {"source": ["MySportsFeeds"], "matches": [], "injuries": []}

        for feed in ("daily_game_schedule", "injuries"):
            try:
                url = f"{MYSPORTS_BASE}/{league}/current/{feed}.json"
                r = self.session.get(url, auth=auth, timeout=8)
                if r.status_code != 200:
                    log.debug(f"MySportsFeeds {feed} {r.status_code}: {r.text[:120]}")
                    continue
                data = r.json()
                if feed == "daily_game_schedule":
                    updates["matches"].extend(self._parse_mysports_games(data, question, teams))
                else:
                    updates["injuries"].extend(self._parse_mysports_injuries(data, question, teams))
            except Exception as e:
                log.debug(f"MySportsFeeds {feed} haku epäonnistui: {e}")

        return updates if updates["matches"] or updates["injuries"] else {}

    def _detect_mysports_league(self, question: str) -> str:
        feed = self._detect_sport_feed(question)
        return feed if feed in ("nba", "nhl", "mlb", "nfl") else ""

    def _parse_mysports_games(self, data: Dict, question: str, teams: Dict[str, str]) -> List[Dict]:
        games = data.get("games") or data.get("schedule", {}).get("games", []) or []
        wanted = self._tokens(" ".join(teams.values()) or question)
        result = []
        for item in games:
            game = item.get("game", item)
            text = jsonish_text(game)
            if wanted and not (wanted & self._tokens(text)):
                continue
            result.append({
                "name": _pick(game, ["awayTeam.abbreviation", "awayTeam.name"], "") + " @ " +
                        _pick(game, ["homeTeam.abbreviation", "homeTeam.name"], ""),
                "begin_at": _pick(game, ["startTime", "dateTime", "schedule.startTime"], ""),
                "status": _pick(game, ["playedStatus", "status"], ""),
            })
            if len(result) >= 3:
                break
        return result

    def _parse_mysports_injuries(self, data: Dict, question: str, teams: Dict[str, str]) -> List[Dict]:
        injuries = data.get("injuries") or data.get("playerInjuries") or []
        wanted = self._tokens(" ".join(teams.values()) or question)
        result = []
        for item in injuries:
            text = jsonish_text(item)
            if wanted and not (wanted & self._tokens(text)):
                continue
            player = item.get("player", item)
            result.append({
                "player": _pick(player, ["fullName", "name", "firstName"], "?"),
                "team": _pick(item, ["team.abbreviation", "team.name"], ""),
                "status": _pick(item, ["playingProbability", "status", "injuryStatus"], ""),
                "detail": _pick(item, ["injury", "bodyPart", "notes"], ""),
            })
            if len(result) >= 8:
                break
        return result

    def _fetch_espn_news(self, question: str, feed_key: str) -> Dict[str, Any]:
        if feedparser is None:
            log.debug("feedparser puuttuu — ESPN RSS ohitetaan")
            return {}
        url = ESPN_FEEDS.get(feed_key, ESPN_FEEDS["general"])
        try:
            feed = feedparser.parse(url)
            entries = feed.get("entries", [])
        except Exception as e:
            log.debug(f"ESPN RSS haku epäonnistui: {e}")
            return {}

        q_tokens = self._tokens(question)
        news = []
        for entry in entries[:25]:
            title = str(entry.get("title", ""))
            summary = str(entry.get("summary", ""))
            score = len(q_tokens & self._tokens(f"{title} {summary}"))
            if score <= 0:
                continue
            news.append({
                "title": title,
                "published": entry.get("published", ""),
                "summary": summary[:180],
                "relevance": score,
            })
        news.sort(key=lambda item: item["relevance"], reverse=True)
        return {"source": ["ESPN RSS"], "news": news[:5]} if news else {}

    def _build_text(self, context: Dict[str, Any]) -> str:
        parts = []
        if context["matches"]:
            lines = []
            for match in context["matches"][:3]:
                detail = " | ".join(str(v) for v in [
                    match.get("name", ""),
                    match.get("league", ""),
                    match.get("tournament", ""),
                    match.get("begin_at", ""),
                    f"status={match.get('status', '')}" if match.get("status") else "",
                    f"BO{match.get('number_of_games')}" if match.get("number_of_games") else "",
                ] if v)
                if detail:
                    lines.append(f"- {detail}")
            if lines:
                parts.append("Tuoreet ottelutiedot:\n" + "\n".join(lines))

        if context["injuries"]:
            lines = []
            for injury in context["injuries"][:8]:
                lines.append(
                    f"- {injury.get('player', '?')} {injury.get('team', '')}: "
                    f"{injury.get('status', '')} {injury.get('detail', '')}".strip()
                )
            parts.append("Tuoreet loukkaantumiset/kokoonpanot:\n" + "\n".join(lines))

        if context["news"]:
            lines = []
            for item in context["news"][:5]:
                published = str(item.get("published", ""))[:22]
                lines.append(f"- {item.get('title', '')} ({published})")
            parts.append("Tuoreet uutisotsikot:\n" + "\n".join(lines))

        return "\n\n".join(parts)

    def _quality(self, context: Dict[str, Any]) -> float:
        score = 0.0
        if context["matches"]:
            score += 0.35
        if context["injuries"]:
            score += 0.35
        if context["news"]:
            score += 0.20
        if len(context["source"]) >= 2:
            score += 0.10
        return round(min(1.0, score), 2)

    def _tokens(self, text: str) -> set:
        stop = {"the", "and", "for", "with", "game", "match", "winner", "will", "vs", "bo3", "bo5"}
        return {
            token
            for token in re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).split()
            if len(token) >= 3 and token not in stop
        }


def jsonish_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(jsonish_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(jsonish_text(v) for v in value)
    return str(value or "")


def _pick(data: Dict, paths: List[str], default: str = "") -> str:
    for path in paths:
        current = data
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current:
            return str(current)
    return default


_instance: Optional[FreshContextFetcher] = None


def get_fresh_context(question: str, market_type: str = "") -> Dict[str, Any]:
    global _instance
    if _instance is None:
        _instance = FreshContextFetcher()
    return _instance.get_context(question, market_type)


def clear_fresh_context_cache():
    if _instance is not None:
        _instance.clear_cache()
