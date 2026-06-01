"""
FreshContextFetcher

Adds current sports/esports context for Claude without changing trading rules.
All sources are best-effort: missing API keys or failed requests simply produce
empty context so the bot can continue normally.
"""

import os
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

import requests
from market_types import classify_market
try:
    import feedparser
except Exception:
    feedparser = None

log = logging.getLogger("Scout.FreshContext")

PANDASCORE_BASE = "https://api.pandascore.co"
MYSPORTS_BASE = "https://api.mysportsfeeds.com/v2.1/pull"
ESPN_SCOREBOARD_BASE = "https://site.api.espn.com/apis/site/v2/sports"

ESPN_FEEDS = {
    "nba": "https://www.espn.com/espn/rss/nba/news",
    "nhl": "https://www.espn.com/espn/rss/nhl/news",
    "mlb": "https://www.espn.com/espn/rss/mlb/news",
    "nfl": "https://www.espn.com/espn/rss/nfl/news",
    "soccer": "https://www.espn.com/espn/rss/soccer/news",
    "esports": "https://www.espn.com/espn/rss/esports/news",
    "general": "https://www.espn.com/espn/rss/news",
}

ESPN_SCOREBOARD_PATHS = {
    "nba": "basketball/nba",
    "mlb": "baseball/mlb",
    "nhl": "hockey/nhl",
    "nfl": "football/nfl",
}

ESPN_SOCCER_SCOREBOARD_PATHS = [
    "soccer/uefa.champions",
    "soccer/eng.1",
    "soccer/fra.1",
    "soccer/esp.1",
    "soccer/ita.1",
    "soccer/ger.1",
    "soccer/uefa.europa",
]


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
            scoreboard = self._fetch_espn_scoreboard(question, teams)
            self._merge(context, scoreboard)
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
        canonical_type = market_type or classify_market(question)
        if canonical_type in ("esports_match", "esports_map"):
            return "esports"
        if canonical_type == "sports":
            return "sports"
        return ""

    def _detect_sport_feed(self, question: str) -> str:
        q = question.lower()
        if any(k in q for k in [
            "nba", "lakers", "knicks", "thunder", "spurs", "celtics", "cavaliers",
            "warriors", "mavericks", "nuggets", "wolves", "timberwolves", "pacers",
            "bucks", "heat", "magic", "raptors", "bulls", "suns", "clippers",
        ]):
            return "nba"
        if any(k in q for k in [
            "nhl", "canadiens", "hurricanes", "oilers", "panthers", "rangers",
            "bruins", "leafs", "maple leafs", "stars", "avalanche", "golden knights",
            "jets", "kings", "devils", "lightning",
        ]):
            return "nhl"
        if any(k in q for k in [
            "mlb", "yankees", "dodgers", "pirates", "cardinals", "phillies",
            "padres", "mets", "braves", "red sox", "blue jays", "orioles",
            "cubs", "brewers", "giants", "mariners", "astros", "rangers",
            "diamondbacks", "guardians", "twins", "tigers", "royals",
        ]):
            return "mlb"
        if any(k in q for k in ["nfl", "chiefs", "eagles", "cowboys"]):
            return "nfl"
        if any(k in q for k in [
            "fc ", "psg", "paris saint-germain", "paris saint germain",
            "arsenal", "premier league", "la liga", "juventus", "barcelona",
            "champions league",
        ]):
            return "soccer"
        return "general"

    def _extract_teams(self, question: str) -> Dict[str, str]:
        q = re.sub(r"\s+", " ", question).strip()
        q = re.sub(r"^(?:game|map)\s+handicap:\s*", "", q, flags=re.IGNORECASE)
        match = re.search(r"(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\-:|]|$)", q, re.IGNORECASE)
        if match:
            home = self._clean_team_name(re.sub(r"^[A-Za-z0-9 ]+:\s*", "", match.group(1)))
            away = self._clean_team_name(match.group(2))
            return {"home": home, "away": away}
        will_match = re.search(r"Will\s+(.+?)\s+(?:win|beat|score)", q, re.IGNORECASE)
        if will_match:
            return {"home": self._clean_team_name(will_match.group(1))}
        return {}

    def _clean_team_name(self, name: str) -> str:
        cleaned = re.sub(r"\s+", " ", name or "").strip()
        cleaned = re.sub(r"\s*\([+-]?\d+(?:\.\d+)?\)\s*$", "", cleaned)
        return cleaned.strip(" -|:")

    def _fetch_pandascore(self, question: str, teams: Dict[str, str]) -> Dict[str, Any]:
        token = os.getenv("PANDASCORE_API_KEY", "")
        if not token:
            return {}

        game = self._detect_esport_game(question)
        endpoints = self._pandascore_endpoints(game)

        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "per_page": int(os.getenv("PANDASCORE_MATCH_LIMIT", 50)),
            "sort": "begin_at",
        }
        debug = os.getenv("FRESH_CONTEXT_DEBUG", "false").lower() == "true"

        for endpoint in endpoints:
            try:
                r = self.session.get(
                    f"{PANDASCORE_BASE}{endpoint}",
                    headers=headers,
                    params=params,
                    timeout=8,
                )
                if r.status_code != 200:
                    log.debug(f"PandaScore {endpoint} {r.status_code}: {r.text[:120]}")
                    continue
                matches = self._select_pandascore_matches(r.json(), question, teams)
                if matches:
                    log.info(f"PandaScore osuma: {endpoint} -> {matches[0].get('name', '')[:60]}")
                    return {"source": ["PandaScore"], "matches": matches}
                if debug:
                    log.info(f"PandaScore ei osumaa: {endpoint} | sample={self._sample_pandascore_names(r.json())}")
            except Exception as e:
                log.debug(f"PandaScore haku epaonnistui: {e}")
        return {}
    def _pandascore_endpoints(self, game: str) -> List[str]:
        endpoints = []
        if game:
            endpoints.extend([
                f"/{game}/matches/running",
                f"/{game}/matches/upcoming",
                f"/{game}/matches/past",
                f"/{game}/matches",
            ])
        endpoints.extend([
            "/matches/running",
            "/matches/upcoming",
            "/matches/past",
            "/matches",
        ])
        return endpoints

    def _detect_esport_game(self, question: str) -> str:
        q = question.lower()
        if "lol:" in q or "league of legends" in q or "lck" in q or "lec" in q or "lpl" in q:
            return "lol"
        if any(alias in q for alias in [
            "giantx", "karmine corp", "kcorp", "kc ", "fnatic", "mad lions",
            "g2", "bds", "sk gaming", "team heretics", "rogue",
            "anyone's legend", "team we", "bilibili", "blg", "top esports",
            "tes", "jdg", "wbg", "weibo", "t1", "gen.g", "geng",
            "dplus", "dk", "hanwha", "hle", "kt rolster", "drx",
        ]):
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
        wanted = self._team_tokens(teams, question)
        selected = []
        for match in data:
            name = str(match.get("name") or match.get("slug") or "")
            opponents = []
            for item in match.get("opponents", []) or []:
                opp = item.get("opponent") or {}
                if opp.get("name"):
                    opponents.append(str(opp["name"]))
            haystack = " ".join([name] + opponents)
            score = self._match_score(wanted, haystack)
            if wanted and score <= 0:
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
                "live_score": self._extract_pandascore_score(match),
                "_match_score": score,
            })
        selected.sort(key=lambda item: (item.get("_match_score", 0), item.get("begin_at", "")), reverse=True)
        return selected[:3]

    def _sample_pandascore_names(self, data: Any) -> List[str]:
        if not isinstance(data, list):
            return []
        return [
            str(match.get("name") or match.get("slug") or "")[:60]
            for match in data[:5]
        ]

    def _extract_pandascore_score(self, match: Dict[str, Any]) -> str:
        parts = []
        for result in match.get("results", []) or []:
            team_id = result.get("team_id")
            score = result.get("score")
            if score is not None:
                parts.append(f"team_id {team_id}: {score}")

        games = match.get("games", []) or []
        game_parts = []
        for game in games[:7]:
            status = game.get("status", "")
            winner = game.get("winner") or {}
            winner_name = winner.get("name") if isinstance(winner, dict) else ""
            position = game.get("position") or game.get("number") or ""
            detail = []
            if position:
                detail.append(f"game {position}")
            if status:
                detail.append(str(status))
            if winner_name:
                detail.append(f"winner={winner_name}")
            if detail:
                game_parts.append(" ".join(detail))

        if game_parts:
            parts.append("; ".join(game_parts))
        return " | ".join(parts)

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

    def _fetch_espn_scoreboard(self, question: str, teams: Dict[str, str]) -> Dict[str, Any]:
        league = self._detect_sport_feed(question)
        paths = ESPN_SOCCER_SCOREBOARD_PATHS if league == "soccer" else [ESPN_SCOREBOARD_PATHS.get(league)]
        paths = [path for path in paths if path]
        if not paths:
            return {}

        wanted = self._team_tokens(teams, question)
        if not wanted:
            return {}

        selected = []
        now = datetime.now(timezone.utc)
        lookback = int(os.getenv("ESPN_SCOREBOARD_LOOKBACK_DAYS", 1))
        lookahead = int(os.getenv("ESPN_SCOREBOARD_LOOKAHEAD_DAYS", 7))
        for offset in range(-lookback, lookahead + 1):
            day = now + timedelta(days=offset)
            for path in paths:
                try:
                    r = self.session.get(
                        f"{ESPN_SCOREBOARD_BASE}/{path}/scoreboard",
                        params={
                            "dates": day.strftime("%Y%m%d"),
                            "limit": int(os.getenv("ESPN_SCOREBOARD_LIMIT", 100)),
                        },
                        timeout=8,
                    )
                    if r.status_code != 200:
                        log.debug(f"ESPN scoreboard {league}/{path} {r.status_code}: {r.text[:120]}")
                        continue
                    selected.extend(self._select_espn_events(r.json(), league, wanted, now))
                    if selected:
                        break
                except Exception as e:
                    log.debug(f"ESPN scoreboard haku epaonnistui: {e}")
            if selected:
                break

        selected.sort(key=lambda item: (item.get("_match_score", 0), item.get("begin_at", "")), reverse=True)
        if selected:
            log.info(f"ESPN scoreboard osuma: {league} -> {selected[0].get('name', '')[:60]}")
            return {"source": ["ESPN Scoreboard"], "matches": selected[:3]}
        return {}
    def _select_espn_events(
        self,
        data: Dict[str, Any],
        league: str,
        wanted: set,
        now: datetime,
    ) -> List[Dict[str, Any]]:
        events = data.get("events", []) if isinstance(data, dict) else []
        selected = []
        for event in events:
            competitions = event.get("competitions", []) or []
            competition = competitions[0] if competitions else {}
            competitors = competition.get("competitors", []) or []
            teams = []
            scores = []
            for comp in competitors:
                team = comp.get("team", {}) or {}
                name = team.get("displayName") or team.get("shortDisplayName") or team.get("name") or ""
                abbr = team.get("abbreviation") or ""
                if name:
                    teams.append(str(name))
                if comp.get("score") not in (None, ""):
                    scores.append(f"{abbr or name}: {comp.get('score')}")

            name = event.get("name") or " vs ".join(teams)
            haystack = " ".join([name] + teams)
            score = self._match_score(wanted, haystack)
            if score <= 0:
                continue

            status = competition.get("status", {}).get("type", {}) if competition else {}
            venue = competition.get("venue", {}) if competition else {}
            begin_at = event.get("date", "")
            status_text = status.get("description") or status.get("name") or ""
            relation = self._espn_match_relation(begin_at, status_text, now)
            selected.append({
                "name": name,
                "league": league.upper(),
                "tournament": event.get("season", {}).get("type", ""),
                "begin_at": begin_at,
                "status": status_text,
                "relation": relation,
                "live_score": " | ".join(scores),
                "venue": venue.get("fullName", ""),
                "_match_score": score,
            })
        return selected

    def _espn_match_relation(self, begin_at: str, status: str, now: datetime) -> str:
        status_l = (status or "").lower()
        if any(k in status_l for k in ["in progress", "halftime", "period", "quarter", "inning", "live"]):
            return "current_live"
        try:
            event_dt = datetime.fromisoformat(str(begin_at).replace("Z", "+00:00"))
        except Exception:
            event_dt = None

        if event_dt is not None:
            hours = (event_dt - now).total_seconds() / 3600
            if hours >= -2 and not any(k in status_l for k in ["final", "postponed", "canceled", "cancelled"]):
                return "upcoming_or_current"
            if hours < -2:
                return "previous_h2h"

        if any(k in status_l for k in ["final", "postponed", "canceled", "cancelled"]):
            return "previous_h2h"
        if any(k in status_l for k in ["scheduled", "pre-game", "pre game"]):
            return "upcoming_or_current"
        return "unknown"

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
                    f"relation={match.get('relation')}" if match.get("relation") else "",
                    match.get("name", ""),
                    match.get("league", ""),
                    match.get("tournament", ""),
                    match.get("begin_at", ""),
                    f"status={match.get('status', '')}" if match.get("status") else "",
                    f"BO{match.get('number_of_games')}" if match.get("number_of_games") else "",
                    f"live_score={match.get('live_score')}" if match.get("live_score") else "",
                    f"venue={match.get('venue')}" if match.get("venue") else "",
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
        stop = {"the", "and", "for", "with", "game", "match", "winner", "will", "vs", "bo3", "bo5", "fc"}
        return {
            token
            for token in re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).split()
            if len(token) >= 2 and token not in stop
        }

    def _team_tokens(self, teams: Dict[str, str], question: str) -> set:
        tokens = self._tokens(" ".join(teams.values()) or question)
        aliases = {
            "dplus": {"dplus", "kia", "dk"},
            "kia": {"dplus", "kia", "dk"},
            "dk": {"dplus", "kia", "dk"},
            "kc": {"kc", "karmine", "corp", "kcorp"},
            "karmine": {"kc", "karmine", "corp", "kcorp"},
            "kcorp": {"kc", "karmine", "corp", "kcorp"},
            "giantx": {"giantx", "giants"},
            "t1": {"t1"},
            "gen": {"gen", "geng"},
            "geng": {"gen", "geng"},
            "psg": {"psg", "paris", "saint", "germain"},
        }
        expanded = set(tokens)
        for token in list(tokens):
            expanded.update(aliases.get(token, set()))
        return expanded

    def _match_score(self, wanted: set, haystack: str) -> int:
        hay_tokens = self._tokens(haystack)
        score = len(wanted & hay_tokens)
        hay_lower = haystack.lower()
        for token in wanted:
            if token in hay_lower:
                score += 1
        return score


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
