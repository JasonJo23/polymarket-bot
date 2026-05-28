"""
=============================================================================
probability_engine.py – ProbabilityEngine  (v1.0)
=============================================================================
STRATEGIA:
  Käyttää Claude API:a laskemaan oman todennäköisyysarvion markkinalle
  kontekstuaalisen datan perusteella (injuries, form, h2h, news).

  Ei arvata — jos dataa ei ole tarpeeksi (quality < MIN_DATA_QUALITY),
  palautetaan None ja jätetään ostos tekemättä.

KALIBROINTI:
  Claude ei ole automaattisesti parempi kuin markkinat.
  Luotettavuus rakentuu vähitellen kun verrataan ennusteita tuloksiin.
  Tarvitaan ~100 ennustetta ennen kuin tiedetään onko mallilla edgeä.

RAKENNE:
  1. Muodosta prompt kontekstidatasta
  2. Kutsu Claude API
  3. Parsoi todennäköisyys JSON-vastauksesta
  4. Tallenna ennuste myöhempää kalibrointia varten
=============================================================================
"""

import os
import json
import logging
import requests
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional

log = logging.getLogger("Scout.ProbabilityEngine")

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
PREDICTIONS_FILE = "predictions_log.json"


class ProbabilityEngine:

    def __init__(self):
        self.api_key         = os.getenv("ANTHROPIC_API_KEY", "")
        self.model           = os.getenv("PROBABILITY_MODEL", "claude-sonnet-4-20250514")
        self.min_data_quality = float(os.getenv("MIN_DATA_QUALITY", 0.3))
        self.min_edge        = float(os.getenv("MIN_EDGE_THRESHOLD", 0.05))

        if not self.api_key:
            log.warning("ANTHROPIC_API_KEY puuttuu — probability engine ei toimi")

    # ===========================================================================
    # Päämetodi
    # ===========================================================================

    def calculate_edge(
        self,
        question:        str,
        outcome:         str,
        polymarket_price: float,
        context:         Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Laskee edgen vertaamalla omaa todennäköisyyttä Polymarket-hintaan.

        Args:
            question:         Markkinan kysymys
            outcome:          Outcome jolle lasketaan (esim. "THUNDER")
            polymarket_price: Nykyinen Polymarket-hinta (0-1)
            context:          SportDataFetcher.get_context_for_market() tulos

        Returns:
            {
                "our_probability": float,   # Oma arvio 0-1
                "polymarket_price": float,  # Polymarketin hinta
                "edge":           float,   # Ero (positiivinen = osta)
                "confidence":     str,     # "high"/"medium"/"low"
                "reasoning":      str,     # Selitys
                "should_bet":     bool,    # Suositus
                "data_quality":   float,   # Kontekstin laatu
            }
        """
        default = self._no_edge_result(polymarket_price, "Ei tarpeeksi dataa")

        if not self.api_key:
            return default

        data_quality = context.get("data_quality", 0.0)
        if data_quality < self.min_data_quality:
            log.info(
                f"Data quality liian heikko ({data_quality:.2f} < {self.min_data_quality}) "
                f"— ohitetaan {question[:40]}"
            )
            return default

        # Muodosta prompt
        prompt = self._build_prompt(question, outcome, polymarket_price, context)

        # Kutsu Claude API
        response = self._call_claude(prompt)
        if not response:
            return default

        # Parsoi vastaus
        result = self._parse_response(response, polymarket_price)
        if not result:
            return default

        # Tallenna ennuste kalibrointia varten
        self._log_prediction(question, outcome, polymarket_price, result, context)

        return result

    # ===========================================================================
    # Prompt-rakenne
    # ===========================================================================

    def _build_prompt(
        self,
        question:        str,
        outcome:         str,
        polymarket_price: float,
        context:         Dict
    ) -> str:
        """
        Rakentaa promptin Claudelle kontekstuaalisen datan perusteella.
        Selkeä rakenne: faktat ensin, kysymys lopussa.
        """
        sport      = context.get("sport", "unknown")
        home       = context.get("home_team", "")
        away       = context.get("away_team", "")
        injuries   = context.get("injuries", [])
        form       = context.get("recent_form", [])
        h2h        = context.get("h2h", [])
        news       = context.get("news", [])
        lineups    = context.get("lineup_notes", [])

        # Muodosta konteksti-osiot
        injury_text = ""
        if injuries:
            lines = []
            for inj in injuries[:8]:
                lines.append(f"  - {inj['player']}: {inj['status']} ({inj.get('detail','')[:60]})")
            injury_text = "LOUKKAANTUMISET:\n" + "\n".join(lines)

        form_text = ""
        if form:
            lines = []
            for f in form[:10]:
                result = "W" if f.get("won") else "L"
                lines.append(f"  {f.get('team','')} [{result}] {f.get('result','')}")
            form_text = "VIIMEISIMMÄT TULOKSET:\n" + "\n".join(lines)

        h2h_text = ""
        if h2h:
            lines = []
            for g in h2h[:5]:
                lines.append(
                    f"  {g.get('date','')} | "
                    f"{g.get('home','')} {g.get('home_score','')}-{g.get('away_score','')} {g.get('away','')}"
                )
            h2h_text = "KESKINÄINEN HISTORIA:\n" + "\n".join(lines)

        news_text = ""
        if news:
            lines = []
            for n in news[:3]:
                lines.append(f"  - {n.get('title','')} ({n.get('published','')[:16]})")
            news_text = "TUOREET UUTISET:\n" + "\n".join(lines)

        lineup_text = ""
        if lineups:
            lines = []
            for ln in lineups:
                roster = ", ".join(ln.get("roster", []))
                lines.append(f"  {ln['team']}: {roster}")
            lineup_text = "KOKOONPANOT:\n" + "\n".join(lines)

        # Yhdistä kaikki saatavilla oleva data
        context_sections = [s for s in [injury_text, form_text, h2h_text, news_text, lineup_text] if s]
        context_str = "\n\n".join(context_sections) if context_sections else "Ei lisädataa saatavilla."

        # Hae Polymarket-konteksti jos saatavilla
        polymarket_context = context.get("context_text", "")
        fresh_context = context.get("fresh_context_text", "")
        fresh_sources = context.get("fresh_sources", [])
        opponents    = context.get("opponents", "")
        tournament   = context.get("tournament", "")
        crypto_price = context.get("crypto_price", 0.0)
        time_context = self._build_time_context(question, context)

        # Rakenna kontekstiosio promptiin
        context_blocks = []
        if polymarket_context and polymarket_context != "Ei lisäkontekstia saatavilla Polymarket API:sta.":
            context_blocks.append("POLYMARKET-KONTEKSTI:\n" + polymarket_context)
        if fresh_context:
            source_text = f" (lähteet: {', '.join(fresh_sources)})" if fresh_sources else ""
            context_blocks.append("TUORE DATA" + source_text + ":\n" + fresh_context)
        if context_str and context_str != "Ei lisädataa saatavilla.":
            context_blocks.append("MUU SAATAVILLA OLEVA DATA:\n" + context_str)

        context_section = "\n\n".join(context_blocks)
        if context_section:
            context_section += "\n"
        else:
            context_section = "Ei ulkoista dataa — käytä yleistietoa ja Polymarket-hintaa lähtökohtana.\n"

        # Crypto-spesifinen konteksti
        crypto_section = ""
        if crypto_price > 0:
            crypto_section = "HUOMIO: Markkinan kynnysarvo on $" + f"{crypto_price:,.0f}" + ". Arvioi onko tämä realistinen.\n"

        prompt = f"""Olet prediction market -analyytikko. Arvioi todennäköisyys markkinalle.

MARKKINA: {question}
ARVIOITAVA OUTCOME: {outcome}
POLYMARKET-HINTA: {polymarket_price:.3f} ({polymarket_price*100:.1f}%)
LAJI: {sport}
{time_context}
{f"OTTELU: {opponents}" if opponents else ""}
{f"TURNAUS: {tournament}" if tournament else ""}

{context_section}{crypto_section}
OHJE:
- Arvioi onko Polymarket-hinta oikein vai väärin käyttäen kaikkea saatavilla olevaa tietoa
- Jos vastustaja on tiedossa: analysoi tasoero, muoto, kotietua
- Esports: joukkueiden taso, turnausmuoto (BO1/BO3/BO5), meta
- NBA/urheilu: joukkueiden vire, playoff-tilanne, loukkaantumiset
- Jos TUORE DATA on mukana, painota sitä enemmän kuin yleistä historiatietoa
- Politiikka/makro: historiallinen todennäköisyys, nykytilanne
- Crypto: ÄLÄ KOSKAAN arvaile krypton nykyistä hintaa — koulutustietosi on vanhentunut. Polymarket-hinta on ainoa luotettava tieto. Jos YES @ 0.52 → markkinat sanovat 52% todennäköisyys. Käytä tätä.
- Jos et tiedä tarpeeksi → conf "low", our_probability lähelle Polymarket-hintaa
- Edge syntyy VAIN kun tiedät jotain mitä hinta ei heijasta

Vastaa VAIN JSON, ei muuta tekstiä:
{{
  "our_probability": <0.0-1.0>,
  "confidence": "<high|medium|low>",
  "reasoning": "<max 120 merkkiä, konkreettinen>",
  "key_factors": ["<tekijä 1>", "<tekijä 2>"]
}}"""

        return prompt

    def _build_time_context(self, question: str, context: Dict[str, Any]) -> str:
        now = datetime.now(timezone.utc)
        lines = [f"AIKAKONTEKSTI: nykyhetki UTC {now.date().isoformat()}"]

        end_date = str(context.get("market_end_date", "") or "").strip()
        if end_date and end_date != "?":
            lines.append(f"MARKKINAN END_DATE: {end_date}")
            days = self._days_until(end_date, now)
            if days is not None:
                lines.append(f"END_DATE ON NYKYHETKESTA: {days:+d} paivaa")

        title_date = self._extract_title_date(question)
        if title_date:
            lines.append(f"OTSIKON PAIVAMAARA: {title_date}")
            days = self._days_until(title_date, now)
            if days is not None:
                lines.append(f"OTSIKON PAIVAMAARA ON NYKYHETKESTA: {days:+d} paivaa")

        return "\n".join(lines)

    def _extract_title_date(self, question: str) -> str:
        match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", question or "")
        return match.group(1) if match else ""

    def _days_until(self, date_text: str, now: datetime) -> Optional[int]:
        try:
            normalized = str(date_text).replace("Z", "+00:00")
            target = datetime.fromisoformat(normalized)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            return (target.date() - now.date()).days
        except Exception:
            try:
                target = datetime.strptime(str(date_text)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return (target.date() - now.date()).days
            except Exception:
                return None

    # ===========================================================================
    # Claude API -kutsu
    # ===========================================================================

    def _call_claude(self, prompt: str) -> Optional[str]:
        """Kutsuu Claude API:a ja palauttaa tekstivastauksen."""
        try:
            payload = {
                "model":      self.model,
                "max_tokens": 500,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
            }
            headers = {
                "Content-Type":      "application/json",
                "x-api-key":         self.api_key,
                "anthropic-version": "2023-06-01",
            }

            r = requests.post(ANTHROPIC_API, json=payload, headers=headers, timeout=15)

            if r.status_code == 200:
                content = r.json().get("content", [])
                if content and content[0].get("type") == "text":
                    return content[0]["text"]
            else:
                log.warning(f"Claude API virhe: {r.status_code} — {r.text[:100]}")
        except Exception as e:
            log.warning(f"Claude API kutsu epäonnistui: {e}")
        return None

    # ===========================================================================
    # Vastauksen parsinta
    # ===========================================================================

    def _parse_response(self, response: str, polymarket_price: float) -> Optional[Dict]:
        """Parsoi Clauden JSON-vastauksen."""
        try:
            # Poista mahdolliset markdown-koodiblokki-merkit
            clean = response.strip()
            clean = clean.replace("```json", "").replace("```", "").strip()

            data = json.loads(clean)

            our_prob   = float(data.get("our_probability", 0.5))
            confidence = str(data.get("confidence", "low"))
            reasoning  = str(data.get("reasoning", ""))
            key_factors = data.get("key_factors", [])

            # Järkevyystarkistus
            if not (0.01 <= our_prob <= 0.99):
                log.warning(f"Epärealistinen todennäköisyys: {our_prob}")
                return None

            edge = our_prob - polymarket_price

            # Suositellaan ostoa vain jos:
            # 1. Edge on riittävän suuri
            # 2. Confidence ei ole "low"
            # 3. Oma todennäköisyys on realistinen (ei alle 0.10 tai yli 0.90)
            should_bet = (
                edge >= self.min_edge and
                confidence != "low" and
                0.15 <= our_prob <= 0.88
            )

            log.info(
                f"Probability: oma={our_prob:.2f} poly={polymarket_price:.2f} "
                f"edge={edge:+.2f} conf={confidence} bet={should_bet}"
            )

            return {
                "our_probability": round(our_prob, 3),
                "polymarket_price": polymarket_price,
                "edge":            round(edge, 3),
                "confidence":      confidence,
                "reasoning":       reasoning,
                "key_factors":     key_factors,
                "should_bet":      should_bet,
                "data_quality":    0.0,  # Täytetään kutsuvassa koodissa
            }

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            log.warning(f"Vastauksen parsinta epäonnistui: {e} | Response: {response[:100]}")
        return None

    # ===========================================================================
    # Kalibrointi-logi
    # ===========================================================================

    def _log_prediction(
        self,
        question:        str,
        outcome:         str,
        polymarket_price: float,
        result:          Dict,
        context:         Dict
    ):
        """
        Tallentaa ennusteen predictions_log.json:iin myöhempää kalibrointia varten.
        Tulos (oikea/väärä) lisätään manuaalisesti tai automaattisesti myöhemmin.
        """
        try:
            try:
                with open(PREDICTIONS_FILE, "r") as f:
                    log_data = json.load(f)
            except FileNotFoundError:
                log_data = {"predictions": []}

            entry = {
                "timestamp":       datetime.now(timezone.utc).isoformat(),
                "question":        question[:80],
                "outcome":         outcome,
                "polymarket_price": polymarket_price,
                "our_probability": result["our_probability"],
                "edge":            result["edge"],
                "confidence":      result["confidence"],
                "reasoning":       result["reasoning"],
                "sport":           context.get("sport", "unknown"),
                "data_quality":    context.get("data_quality", 0.0),
                "should_bet":      result["should_bet"],
                "actual_result":   None,  # Täytetään myöhemmin
            }

            log_data["predictions"].append(entry)

            with open(PREDICTIONS_FILE, "w") as f:
                json.dump(log_data, f, indent=2)

        except Exception as e:
            log.debug(f"Prediction loki epäonnistui: {e}")

    def _no_edge_result(self, polymarket_price: float, reason: str) -> Dict:
        """Palauttaa tyhjän tuloksen kun edgeä ei löydy."""
        return {
            "our_probability": polymarket_price,  # Neutraali — sama kuin markkinat
            "polymarket_price": polymarket_price,
            "edge":            0.0,
            "confidence":      "low",
            "reasoning":       reason,
            "key_factors":     [],
            "should_bet":      False,
            "data_quality":    0.0,
        }

    # ===========================================================================
    # Kalibrointi-analyysi
    # ===========================================================================

    def analyze_calibration(self) -> Dict:
        """
        Analysoi ennusteiden tarkkuuden.
        Aja manuaalisesti kun olet lisännyt actual_result-kentät.
        """
        try:
            with open(PREDICTIONS_FILE, "r") as f:
                data = json.load(f)
            predictions = data.get("predictions", [])
        except FileNotFoundError:
            return {"error": "Ei ennusteita vielä"}

        resolved = [p for p in predictions if p.get("actual_result") is not None]
        if len(resolved) < 10:
            return {
                "total":    len(predictions),
                "resolved": len(resolved),
                "message":  f"Liian vähän dataa ({len(resolved)}/10 min)"
            }

        correct   = sum(1 for p in resolved if p["actual_result"] == True)
        win_rate  = correct / len(resolved)

        # Laske expected value per konfidenssiluokka
        by_confidence = {}
        for p in resolved:
            conf = p.get("confidence", "low")
            if conf not in by_confidence:
                by_confidence[conf] = {"correct": 0, "total": 0, "avg_edge": 0}
            by_confidence[conf]["total"] += 1
            by_confidence[conf]["avg_edge"] += p.get("edge", 0)
            if p["actual_result"]:
                by_confidence[conf]["correct"] += 1

        for conf, stats in by_confidence.items():
            stats["win_rate"] = stats["correct"] / stats["total"]
            stats["avg_edge"] = stats["avg_edge"] / stats["total"]

        return {
            "total":          len(predictions),
            "resolved":       len(resolved),
            "win_rate":       round(win_rate, 3),
            "by_confidence":  by_confidence,
            "has_edge":       win_rate > 0.55,
        }
