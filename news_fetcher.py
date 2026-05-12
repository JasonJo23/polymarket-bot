"""
=============================================================================
news_fetcher.py – SportDataFetcher  (v1.0)
=============================================================================
STRATEGIA:
  Hakee reaaliaikaista dataa urheilutapahtumista juuri ennen peliä.
  Edge syntyy kun tiedät kokoonpanon tai loukkaantumisen ENNEN kuin
  Polymarketin hinnat ehtivät reagoida.

LÄHTEET (ilmaiset, ei API-avainta):
  NBA:      balldontlie.io  — injuries, stats, standings
  Esports:  liquipedia.net  — rosters, results, upcoming matches
  Football: football-data.org — lineups, injuries, form
  General:  ESPN RSS-feedit  — breaking news kaikista lajeista

MAKSULLISET (tulossa kun edge todistettu):
  - mysportsfeeds.com  (~20€/kk) — kattava NBA/NHL/MLB
  - pandascore.co      (~30€/kk) — esports API
  - api-football.com   (~10€/kk) — jalkapallo

KÄYTTÖ:
  fetcher = SportDataFetcher()
  context = fetcher.get_context_for_market("Lakers vs. Thunder")
  # → {"home_team": "Lakers", "away_team": "Thunder",
  #    "injuries": [...], "recent_form": [...], "h2h": [...]}
=============================================================================
"""

import os
import re
import time
import logging
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from functools import lru_cache

log = logging.getLogger("Scout.NewsFetcher")

# API base URLit
NBA_API      = "https://api.balldontlie.io/v1"
FOOTBALL_API = "https://api.football-data.org/v4"
LIQUIPEDIA   = "https://liquipedia.net"

# ESPN RSS-feedit lajeittain
ESPN_FEEDS = {
    "nba":      "https://www.espn.com/espn/rss/nba/news",
    "nfl":      "https://www.espn.com/espn/rss/nfl/news",
    "nhl":      "https://www.espn.com/espn/rss/nhl/news",
    "mlb":      "https://www.espn.com/espn/rss/mlb/news",
    "soccer":   "https://www.espn.com/espn/rss/soccer/news",
    "esports":  "https://www.espn.com/espn/rss/esports/news",
    "general":  "https://www.espn.com/espn/rss/news",
}

# NBA-joukkueiden nimikartta (Polymarket → balldontlie ID)
NBA_TEAMS = {
    "lakers": 14, "celtics": 2, "knicks": 20, "hawks": 1,
    "bulls": 4, "heat": 15, "thunder": 25, "pistons": 8,
    "magic": 17, "rockets": 10, "spurs": 24, "raptors": 28,
    "cavaliers": 5, "76ers": 23, "nuggets": 7, "timberwolves": 18,
    "pacers": 11, "bucks": 16, "clippers": 12, "warriors": 9,
    "suns": 22, "jazz": 29, "mavericks": 6, "grizzlies": 29,
    "trail blazers": 22, "kings": 26, "hornets": 3,
}

# Esports-pelien tunnistus
ESPORTS_GAMES = {
    "lol": "leagueoflegends",
    "league of legends": "leagueoflegends",
    "cs2": "counterstrike",
    "csgo": "counterstrike",
    "counter-strike": "counterstrike",
    "valorant": "valorant",
    "dota": "dota2",
}


class SportDataFetcher:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "PolymarketScout/1.0",
        })
        # API-avaimet .env:stä (valinnaisia)
        self.football_api_key = os.getenv("FOOTBALL_DATA_API_KEY", "")
        self.nba_api_key      = os.getenv("NBA_API_KEY", "")

        # Kevyt in-memory cache — vältetään turhat API-kutsut
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = 300  # 5 min

    # ===========================================================================
    # Päämetodi: hae konteksti markkinakysymykselle
    # ===========================================================================

    def get_context_for_market(self, question: str) -> Dict[str, Any]:
        """
        Päärajapinta: ottaa Polymarket-kysymyksen ja palauttaa
        kaiken saatavilla olevan kontekstin todennäköisyyslaskentaa varten.

        Args:
            question: Polymarket-markkinan kysymys
                      esim. "Lakers vs. Thunder" tai "Will Arsenal FC win?"

        Returns:
            {
                "sport":        str,      # "nba", "esports", "football", "unknown"
                "home_team":    str,
                "away_team":    str,
                "injuries":     list,     # [{player, team, status, detail}]
                "recent_form":  list,     # Viimeiset 5 tulosta per joukkue
                "h2h":          list,     # Head-to-head historia
                "news":         list,     # Tuoreet uutiset (ESPN RSS)
                "lineup_notes": list,     # Kokoonpanomuutokset
                "data_quality": float,    # 0-1, kuinka paljon dataa löytyi
                "fetched_at":   str,
            }
        """
        cache_key = question.lower().strip()
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        sport = self._detect_sport(question)
        teams = self._extract_teams(question)

        log.info(f"Haetaan konteksti: '{question[:50]}' | sport={sport} | teams={teams}")

        context = {
            "sport":        sport,
            "home_team":    teams.get("home", ""),
            "away_team":    teams.get("away", ""),
            "injuries":     [],
            "recent_form":  [],
            "h2h":          [],
            "news":         [],
            "lineup_notes": [],
            "data_quality": 0.0,
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
        }

        # Hae sport-spesifinen data
        if sport == "nba":
            self._enrich_nba(context, teams)
        elif sport == "esports":
            self._enrich_esports(context, teams, question)
        elif sport == "football":
            self._enrich_football(context, teams, question)

        # Hae uutiset aina
        context["news"] = self._fetch_news(question, sport)

        # Laske data quality score
        context["data_quality"] = self._calculate_data_quality(context)

        log.info(
            f"Konteksti valmis: injuries={len(context['injuries'])} "
            f"news={len(context['news'])} quality={context['data_quality']:.2f}"
        )

        self._set_cached(cache_key, context)
        return context

    # ===========================================================================
    # NBA
    # ===========================================================================

    def _enrich_nba(self, context: Dict, teams: Dict):
        """Hakee NBA-dataa balldontlie.io:sta."""
        home = teams.get("home", "").lower()
        away = teams.get("away", "").lower()

        home_id = self._find_nba_team_id(home)
        away_id = self._find_nba_team_id(away)

        if home_id:
            context["injuries"].extend(self._fetch_nba_injuries(home_id))
            context["recent_form"].extend(self._fetch_nba_recent_form(home_id, home))

        if away_id:
            context["injuries"].extend(self._fetch_nba_injuries(away_id))
            context["recent_form"].extend(self._fetch_nba_recent_form(away_id, away))

        if home_id and away_id:
            context["h2h"] = self._fetch_nba_h2h(home_id, away_id)

    def _find_nba_team_id(self, team_name: str) -> Optional[int]:
        """Löytää NBA-joukkueen ID:n nimen perusteella."""
        for key, team_id in NBA_TEAMS.items():
            if key in team_name or team_name in key:
                return team_id
        return None

    def _fetch_nba_injuries(self, team_id: int) -> List[Dict]:
        """Hakee NBA-joukkueen loukkaantuneet pelaajat."""
        try:
            headers = {}
            if self.nba_api_key:
                headers["Authorization"] = self.nba_api_key

            r = self.session.get(
                f"{NBA_API}/injuries",
                params={"team_ids[]": team_id},
                headers=headers,
                timeout=8
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                injuries = []
                for injury in data:
                    injuries.append({
                        "player": injury.get("player", {}).get("display_name", "?"),
                        "status": injury.get("status", "?"),
                        "detail": injury.get("description", ""),
                        "team_id": team_id,
                    })
                log.debug(f"NBA injuries team {team_id}: {len(injuries)} pelaajaa")
                return injuries
            elif r.status_code == 401:
                log.debug("NBA API: ei API-avainta, injuries ei saatavilla")
            return []
        except Exception as e:
            log.debug(f"NBA injuries haku epäonnistui: {e}")
            return []

    def _fetch_nba_recent_form(self, team_id: int, team_name: str) -> List[Dict]:
        """Hakee joukkueen viimeiset 5 peliä."""
        try:
            headers = {}
            if self.nba_api_key:
                headers["Authorization"] = self.nba_api_key

            r = self.session.get(
                f"{NBA_API}/games",
                params={
                    "team_ids[]": team_id,
                    "per_page":   5,
                    "sort":       "date",
                    "order":      "desc",
                },
                headers=headers,
                timeout=8
            )
            if r.status_code == 200:
                games = r.json().get("data", [])
                form = []
                for g in games:
                    home_team = g.get("home_team", {}).get("abbreviation", "")
                    away_team = g.get("visitor_team", {}).get("abbreviation", "")
                    home_score = g.get("home_team_score", 0)
                    away_score = g.get("visitor_team_score", 0)
                    is_home = str(team_id) == str(g.get("home_team", {}).get("id"))
                    won = (home_score > away_score) if is_home else (away_score > home_score)
                    form.append({
                        "team":   team_name,
                        "date":   g.get("date", "")[:10],
                        "result": f"{home_team} {home_score}-{away_score} {away_team}",
                        "won":    won,
                    })
                return form
        except Exception as e:
            log.debug(f"NBA form haku epäonnistui: {e}")
        return []

    def _fetch_nba_h2h(self, home_id: int, away_id: int) -> List[Dict]:
        """Hakee kahden joukkueen keskinäistä historiaa."""
        try:
            headers = {}
            if self.nba_api_key:
                headers["Authorization"] = self.nba_api_key

            r = self.session.get(
                f"{NBA_API}/games",
                params={
                    "team_ids[]": [home_id, away_id],
                    "per_page":   10,
                    "sort":       "date",
                    "order":      "desc",
                },
                headers=headers,
                timeout=8
            )
            if r.status_code == 200:
                games = r.json().get("data", [])
                # Suodata vain pelit joissa molemmat joukkueet
                h2h = []
                for g in games:
                    h_id = g.get("home_team", {}).get("id")
                    a_id = g.get("visitor_team", {}).get("id")
                    if {h_id, a_id} == {home_id, away_id}:
                        h2h.append({
                            "date":       g.get("date", "")[:10],
                            "home":       g.get("home_team", {}).get("abbreviation"),
                            "away":       g.get("visitor_team", {}).get("abbreviation"),
                            "home_score": g.get("home_team_score"),
                            "away_score": g.get("visitor_team_score"),
                        })
                return h2h[:5]
        except Exception as e:
            log.debug(f"NBA h2h haku epäonnistui: {e}")
        return []

    # ===========================================================================
    # Esports (Liquipedia)
    # ===========================================================================

    def _enrich_esports(self, context: Dict, teams: Dict, question: str):
        """Hakee esports-dataa Liquipediasta."""
        game = self._detect_esports_game(question)
        if not game:
            return

        home = teams.get("home", "")
        away = teams.get("away", "")

        if home:
            roster = self._fetch_liquipedia_roster(game, home)
            if roster:
                context["lineup_notes"].append({
                    "team": home,
                    "roster": roster,
                })

        if away:
            roster = self._fetch_liquipedia_roster(game, away)
            if roster:
                context["lineup_notes"].append({
                    "team": away,
                    "roster": roster,
                })

        # Hae viimeisimmät tulokset
        if home:
            results = self._fetch_liquipedia_results(game, home)
            context["recent_form"].extend(results)

    def _detect_esports_game(self, question: str) -> Optional[str]:
        q = question.lower()
        for keyword, game in ESPORTS_GAMES.items():
            if keyword in q:
                return game
        return None

    def _fetch_liquipedia_roster(self, game: str, team_name: str) -> List[str]:
        """Hakee joukkueen nykyisen kokoonpanon Liquipediasta."""
        try:
            # Liquipedia API (parse endpoint)
            r = self.session.get(
                f"{LIQUIPEDIA}/{game}/api.php",
                params={
                    "action": "parse",
                    "page":   team_name,
                    "format": "json",
                    "prop":   "wikitext",
                },
                timeout=10
            )
            if r.status_code == 200:
                wikitext = r.json().get("parse", {}).get("wikitext", {}).get("*", "")
                # Yksinkertainen parsinta — etsi pelaajanimet
                players = re.findall(r'\|player=([^\|]+)', wikitext)
                return [p.strip() for p in players if p.strip()][:6]
        except Exception as e:
            log.debug(f"Liquipedia roster haku epäonnistui ({team_name}): {e}")
        return []

    def _fetch_liquipedia_results(self, game: str, team_name: str) -> List[Dict]:
        """Hakee joukkueen viimeisimmät tulokset."""
        try:
            r = self.session.get(
                f"{LIQUIPEDIA}/{game}/api.php",
                params={
                    "action":     "parse",
                    "page":       f"{team_name}/Results",
                    "format":     "json",
                    "prop":       "wikitext",
                },
                timeout=10
            )
            if r.status_code == 200:
                wikitext = r.json().get("parse", {}).get("wikitext", {}).get("*", "")
                # Yksinkertainen parsinta — etsi W/L tulokset
                results = re.findall(r'\|(W|L)\|', wikitext)
                return [{"team": team_name, "won": r == "W"} for r in results[:5]]
        except Exception as e:
            log.debug(f"Liquipedia results haku epäonnistui ({team_name}): {e}")
        return []

    # ===========================================================================
    # Jalkapallo (football-data.org)
    # ===========================================================================

    def _enrich_football(self, context: Dict, teams: Dict, question: str):
        """Hakee jalkapalodataa football-data.org:sta."""
        if not self.football_api_key:
            log.debug("Football API key puuttuu — ohitetaan jalkapallo-data")
            return

        home = teams.get("home", "")
        away = teams.get("away", "")

        # Yksinkertainen toteutus — hakee viimeisimmät 5 peliä
        for team in [home, away]:
            if team:
                form = self._fetch_football_form(team)
                context["recent_form"].extend(form)

    def _fetch_football_form(self, team_name: str) -> List[Dict]:
        """Hakee jalkapallojoukkueen viimeisimmät tulokset."""
        try:
            r = self.session.get(
                f"{FOOTBALL_API}/teams",
                params={"name": team_name},
                headers={"X-Auth-Token": self.football_api_key},
                timeout=8
            )
            if r.status_code == 200:
                teams = r.json().get("teams", [])
                if not teams:
                    return []
                team_id = teams[0]["id"]

                # Hae viimeiset pelit
                r2 = self.session.get(
                    f"{FOOTBALL_API}/teams/{team_id}/matches",
                    params={"status": "FINISHED", "limit": 5},
                    headers={"X-Auth-Token": self.football_api_key},
                    timeout=8
                )
                if r2.status_code == 200:
                    matches = r2.json().get("matches", [])
                    form = []
                    for m in matches:
                        home_team  = m.get("homeTeam", {}).get("shortName", "")
                        away_team  = m.get("awayTeam", {}).get("shortName", "")
                        home_score = m.get("score", {}).get("fullTime", {}).get("home", 0)
                        away_score = m.get("score", {}).get("fullTime", {}).get("away", 0)
                        is_home    = home_team.lower() in team_name.lower()
                        won = (home_score > away_score) if is_home else (away_score > home_score)
                        form.append({
                            "team":   team_name,
                            "date":   m.get("utcDate", "")[:10],
                            "result": f"{home_team} {home_score}-{away_score} {away_team}",
                            "won":    won,
                        })
                    return form
        except Exception as e:
            log.debug(f"Football form haku epäonnistui ({team_name}): {e}")
        return []

    # ===========================================================================
    # ESPN RSS — uutiset kaikille lajeille
    # ===========================================================================

    def _fetch_news(self, question: str, sport: str) -> List[Dict]:
        """
        Hakee tuoreet uutiset ESPN RSS-feedistä.
        Suodattaa relevanteimmat markkinan kysymyksen perusteella.
        """
        feed_url = ESPN_FEEDS.get(sport, ESPN_FEEDS["general"])

        try:
            feed = feedparser.parse(feed_url)
            entries = feed.get("entries", [])

            # Etsi joukkueet kysymyksestä
            teams = self._extract_teams(question)
            keywords = set()
            for team in teams.values():
                keywords.update(team.lower().split())

            relevant = []
            for entry in entries[:20]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                text    = f"{title} {summary}".lower()

                # Tarkista onko uutinen relevantti
                score = sum(1 for kw in keywords if kw in text and len(kw) > 3)
                if score > 0:
                    relevant.append({
                        "title":     title,
                        "summary":   summary[:200],
                        "published": entry.get("published", ""),
                        "relevance": score,
                    })

            # Järjestä relevanssin mukaan
            relevant.sort(key=lambda x: x["relevance"], reverse=True)
            return relevant[:5]

        except Exception as e:
            log.debug(f"ESPN RSS haku epäonnistui ({sport}): {e}")
        return []

    # ===========================================================================
    # Apumetodit
    # ===========================================================================

    def _detect_sport(self, question: str) -> str:
        """Tunnistaa lajin kysymyksen perusteella."""
        q = question.lower()

        nba_keywords = [
            "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
            "thunder", "pistons", "magic", "rockets", "spurs", "raptors",
            "cavaliers", "76ers", "nuggets", "timberwolves", "nba",
            "pacers", "bucks", "warriors", "suns", "mavericks",
        ]
        esports_keywords = [
            "lol:", "dota", "csgo", "cs2", "valorant", "counter-strike",
            "esports", "lck", "lec", "lcs", "lpl", "blast", "pgl",
        ]
        football_keywords = [
            "fc ", "arsenal", "chelsea", "liverpool", "manchester",
            "barcelona", "madrid", "premier league", "bundesliga",
            "serie a", "la liga", "champions league", "win on",
        ]
        nhl_keywords = [
            "bruins", "sabres", "lightning", "oilers", "avalanche",
            "kings", "canadiens", "flyers", "nhl",
        ]
        mlb_keywords = [
            "red sox", "yankees", "dodgers", "cubs", "mlb",
            "innings", "o/u", "mets", "angels", "royals",
        ]

        if any(kw in q for kw in esports_keywords):
            return "esports"
        if any(kw in q for kw in nba_keywords):
            return "nba"
        if any(kw in q for kw in football_keywords):
            return "football"
        if any(kw in q for kw in nhl_keywords):
            return "nhl"
        if any(kw in q for kw in mlb_keywords):
            return "mlb"
        return "general"

    def _extract_teams(self, question: str) -> Dict[str, str]:
        """
        Erottaa joukkuenimet kysymyksestä.
        Tukee formaatteja:
          - "Lakers vs. Thunder"
          - "Will Arsenal FC win on 2026-05-05?"
          - "LoL: T1 vs HANJIN BRION (BO3)"
        """
        teams = {}

        # Formaatti: "Team1 vs. Team2" tai "Team1 vs Team2"
        vs_match = re.search(
            r'^(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\|]|$)',
            question, re.IGNORECASE
        )
        if vs_match:
            home = vs_match.group(1).strip()
            away = vs_match.group(2).strip()
            # Poista etuliitteet kuten "LoL: "
            home = re.sub(r'^[A-Z]+:\s*', '', home).strip()
            teams["home"] = home
            teams["away"] = away
            return teams

        # Formaatti: "Will Arsenal FC win?"
        will_match = re.search(
            r'Will\s+(.+?)\s+(?:win|beat|score)',
            question, re.IGNORECASE
        )
        if will_match:
            teams["home"] = will_match.group(1).strip()

        return teams

    def _calculate_data_quality(self, context: Dict) -> float:
        """
        Laskee datalaadun pisteytykseksi 0-1.
        Kertoo probability enginelle kuinka luotettava konteksti on.
        """
        score = 0.0
        max_score = 5.0

        if context["injuries"]:
            score += 1.5   # Injuries = kriittisin tieto
        if context["recent_form"]:
            score += 1.5   # Recent form = tärkeä
        if context["h2h"]:
            score += 0.5   # H2H = hyödyllinen
        if context["news"]:
            score += 1.0   # Uutiset = konteksti
        if context["lineup_notes"]:
            score += 0.5   # Kokoonpano = hyödyllinen

        return round(min(1.0, score / max_score), 2)

    def _get_cached(self, key: str) -> Optional[Dict]:
        if key in self._cache:
            entry = self._cache[key]
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                entry["fetched_at"]
            )).total_seconds()
            if age < self._cache_ttl:
                log.debug(f"Cache hit: {key[:30]}")
                return entry
            del self._cache[key]
        return None

    def _set_cached(self, key: str, context: Dict):
        self._cache[key] = context