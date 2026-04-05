"""
SportsAPISimulator — realistic mock of the TheSportsDB REST API.

Returns deterministic, richly-detailed sports data for Premier League and NBA
so tests never need a live network connection.  Data is structured identically
to the real API so the ingester can be exercised with zero code changes.

Usage in tests
--------------
    simulator = SportsAPISimulator()
    session   = simulator.as_session()   # drop-in requests.Session replacement
    ingester  = SportsIngester(http_session=session, s3_client=mock_s3)
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Rich seed data — real teams, realistic player rosters
# ---------------------------------------------------------------------------

PREMIER_LEAGUE_TEAMS = [
    ("133604", "Manchester City",   "Erling Haaland", "Pep Guardiola",   "Etihad Stadium"),
    ("133612", "Arsenal",           "Bukayo Saka",    "Mikel Arteta",    "Emirates Stadium"),
    ("133616", "Liverpool",         "Mohamed Salah",  "Jürgen Klopp",    "Anfield"),
    ("133613", "Manchester United", "Marcus Rashford","Erik ten Hag",    "Old Trafford"),
    ("133600", "Chelsea",           "Cole Palmer",    "Mauricio Pochettino","Stamford Bridge"),
    ("133601", "Tottenham Hotspur", "Son Heung-min",  "Ange Postecoglou","Tottenham Hotspur Stadium"),
    ("133619", "Newcastle United",  "Alexander Isak", "Eddie Howe",      "St James' Park"),
    ("133615", "Aston Villa",       "Ollie Watkins",  "Unai Emery",      "Villa Park"),
    ("133609", "Brighton",          "Evan Ferguson",  "Roberto De Zerbi","Amex Stadium"),
    ("133614", "West Ham United",   "Jarrod Bowen",   "David Moyes",     "London Stadium"),
    ("133614", "Fulham",            "Aleksandar Mitrovic","Marco Silva", "Craven Cottage"),
    ("133610", "Wolves",            "Matheus Cunha",  "Gary O'Neil",     "Molineux Stadium"),
    ("133618", "Everton",           "Dominic Calvert-Lewin","Sean Dyche","Goodison Park"),
    ("133607", "Crystal Palace",    "Eberechi Eze",   "Oliver Glasner",  "Selhurst Park"),
    ("133606", "Brentford",         "Ivan Toney",     "Thomas Frank",    "GTech Community Stadium"),
    ("133620", "Nottingham Forest", "Morgan Gibbs-White","Nuno Espírito Santo","City Ground"),
    ("133602", "Luton Town",        "Carlton Morris",  "Rob Edwards",    "Kenilworth Road"),
    ("133608", "Burnley",           "Lyle Foster",    "Vincent Kompany", "Turf Moor"),
    ("133605", "Sheffield United",  "Oliver McBurnie","Chris Wilder",    "Bramall Lane"),
    ("133617", "Bournemouth",       "Dominic Solanke","Andoni Iraola",   "Vitality Stadium"),
]

NBA_TEAMS = [
    ("134880", "Boston Celtics",        "Jayson Tatum",    "Joe Mazzulla",     "TD Garden"),
    ("134881", "Golden State Warriors", "Stephen Curry",   "Steve Kerr",       "Chase Center"),
    ("134882", "Los Angeles Lakers",    "LeBron James",    "Darvin Ham",       "Crypto.com Arena"),
    ("134883", "Milwaukee Bucks",       "Giannis Antetokounmpo","Doc Rivers","Fiserv Forum"),
    ("134884", "Denver Nuggets",        "Nikola Jokić",    "Michael Malone",   "Ball Arena"),
    ("134885", "Miami Heat",            "Jimmy Butler",    "Erik Spoelstra",   "Kaseya Center"),
    ("134886", "Phoenix Suns",          "Kevin Durant",    "Frank Vogel",      "Footprint Center"),
    ("134887", "Philadelphia 76ers",    "Joel Embiid",     "Nick Nurse",       "Wells Fargo Center"),
    ("134888", "New York Knicks",       "Jalen Brunson",   "Tom Thibodeau",    "Madison Square Garden"),
    ("134889", "Oklahoma City Thunder", "Shai Gilgeous-Alexander","Mark Daigneault","Paycom Center"),
]

EPL_LEAGUE_ID = "4328"
NBA_LEAGUE_ID = "4387"
CURRENT_SEASON = "2023-2024"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rng(seed: int) -> random.Random:
    return random.Random(seed)


def _make_match_id(home_idx: int, away_idx: int, week: int) -> str:
    return f"epl-{home_idx}-{away_idx}-w{week:02d}"


def _kickoff(week: int, day_offset: int = 0) -> datetime:
    base = datetime(2024, 1, 6)  # Gameweek 1 base date
    return base + timedelta(weeks=week - 1, days=day_offset, hours=15)


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _build_epl_season_events(season: str = CURRENT_SEASON) -> list[dict]:
    """Generate a full 38-gameweek EPL season: fixtures + results."""
    events = []
    teams = PREMIER_LEAGUE_TEAMS
    rng = _rng(42)

    # 38 gameweeks × 10 matches each
    for week in range(1, 39):
        indices = list(range(len(teams)))
        rng.shuffle(indices)
        pairs = [(indices[i], indices[i + 1]) for i in range(0, len(indices) - 1, 2)]

        for day, (hi, ai) in enumerate(pairs):
            home_id, home_name, home_star, _, venue = teams[hi]
            away_id, away_name, away_star, _, _ = teams[ai]
            match_id = _make_match_id(hi, ai, week)
            kickoff = _kickoff(week, day % 3)

            event: dict[str, Any] = {
                "idEvent": match_id,
                "strHomeTeam": home_name,
                "strAwayTeam": away_name,
                "idHomeTeam": home_id,
                "idAwayTeam": away_id,
                "strLeague": "English Premier League",
                "strSeason": season,
                "dateEvent": kickoff.strftime("%Y-%m-%d"),
                "strTime": kickoff.strftime("%H:%M:%S"),
                "strVenue": venue,
                "intRound": str(week),
            }

            # Past weeks have scores; future weeks don't
            if week <= 28:
                hs = rng.choices([0, 1, 2, 3, 4], weights=[15, 30, 30, 15, 10])[0]
                as_ = rng.choices([0, 1, 2, 3, 4], weights=[15, 30, 30, 15, 10])[0]
                event["intHomeScore"] = str(hs)
                event["intAwayScore"] = str(as_)
                # Goal events
                events_list = []
                for g in range(hs):
                    minute = rng.randint(1, 90)
                    events_list.append({
                        "strEvent": "Goal",
                        "strPlayer": home_star if rng.random() > 0.4 else f"Player_{rng.randint(1,11)}",
                        "intEventTime": str(minute),
                        "strTeam": home_name,
                    })
                for g in range(as_):
                    minute = rng.randint(1, 90)
                    events_list.append({
                        "strEvent": "Goal",
                        "strPlayer": away_star if rng.random() > 0.4 else f"Player_{rng.randint(1,11)}",
                        "intEventTime": str(minute),
                        "strTeam": away_name,
                    })
                event["strEvents"] = json.dumps(events_list)

            events.append(event)

    return events


def _build_nba_season_events(season: str = CURRENT_SEASON) -> list[dict]:
    """Generate an NBA regular season schedule (82 games per team, subset shown)."""
    events = []
    teams = NBA_TEAMS
    rng = _rng(99)

    for week in range(1, 25):
        indices = list(range(len(teams)))
        rng.shuffle(indices)
        pairs = [(indices[i], indices[i + 1]) for i in range(0, len(indices) - 1, 2)]

        for day, (hi, ai) in enumerate(pairs):
            home_id, home_name, home_star, _, venue = teams[hi]
            away_id, away_name, away_star, _, _ = teams[ai]
            match_id = f"nba-{hi}-{ai}-w{week:02d}"
            kickoff = datetime(2023, 10, 18) + timedelta(weeks=week - 1, days=day % 4)

            event: dict[str, Any] = {
                "idEvent": match_id,
                "strHomeTeam": home_name,
                "strAwayTeam": away_name,
                "idHomeTeam": home_id,
                "idAwayTeam": away_id,
                "strLeague": "NBA",
                "strSeason": season,
                "dateEvent": kickoff.strftime("%Y-%m-%d"),
                "strTime": "19:30:00",
                "strVenue": venue,
                "intRound": str(week),
            }

            if week <= 20:
                hs = rng.randint(95, 135)
                as_ = rng.randint(95, 135)
                event["intHomeScore"] = str(hs)
                event["intAwayScore"] = str(as_)

            events.append(event)

    return events


def _build_player_roster(team_id: str) -> list[dict]:
    """Generate a realistic 25-player squad for a team."""
    positions_epl = ["GK", "DEF", "DEF", "DEF", "DEF", "MID", "MID", "MID", "MID", "FWD", "FWD"]
    nationalities = ["English", "French", "Brazilian", "Spanish", "German", "Portuguese", "Dutch"]
    rng = _rng(int(team_id[-4:]) if team_id[-4:].isdigit() else 1234)
    players = []
    for i in range(11):
        players.append({
            "idPlayer": f"{team_id}-P{i+1:02d}",
            "strPlayer": f"Player {i+1}",
            "strPosition": positions_epl[i],
            "strNationality": rng.choice(nationalities),
            "strTeam": team_id,
            "intSoccerXMLID": str(rng.randint(100000, 999999)),
            "dateBorn": f"{rng.randint(1994, 2003)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
        })
    return players


# ---------------------------------------------------------------------------
# Main simulator class
# ---------------------------------------------------------------------------

class SportsAPISimulator:
    """
    Drop-in replacement for ``requests.Session`` that returns realistic sports
    data without any network calls.

    The data is deterministically generated from fixed seeds, so every test
    run returns exactly the same fixtures and results.
    """

    def __init__(self, network_failure_rate: float = 0.0, latency_ms: int = 0) -> None:
        self._failure_rate = network_failure_rate
        self._latency_ms = latency_ms
        self._call_log: list[dict] = []

        # Pre-build data once
        self._epl_events = _build_epl_season_events()
        self._nba_events = _build_nba_season_events()

    # ------------------------------------------------------------------
    # requests.Session interface
    # ------------------------------------------------------------------

    def get(self, url: str, params: dict | None = None, timeout: int = 10) -> MagicMock:
        params = params or {}
        self._call_log.append({"url": url, "params": params})

        if self._failure_rate > 0 and random.random() < self._failure_rate:
            raise ConnectionError(f"Simulated network failure for {url}")

        payload = self._route(url, params)
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        return response

    # ------------------------------------------------------------------
    # Session-compatible helpers (used by requests)
    # ------------------------------------------------------------------

    def as_session(self) -> "SportsAPISimulator":
        return self

    # ------------------------------------------------------------------
    # Router
    # ------------------------------------------------------------------

    def _route(self, url: str, params: dict) -> dict:
        if "eventsseason" in url:
            league_id = params.get("id", EPL_LEAGUE_ID)
            season = params.get("s", CURRENT_SEASON)
            return self._events_season(league_id, season)
        elif "lookup_all_players" in url:
            team_id = params.get("id", "133604")
            return {"player": _build_player_roster(team_id)}
        elif "search_all_teams" in url:
            league = params.get("l", "English Premier League")
            return self._search_teams(league)
        elif "lookupleague" in url:
            return self._league_detail(params.get("id", EPL_LEAGUE_ID))
        else:
            return {}

    def _events_season(self, league_id: str, season: str) -> dict:
        if league_id == NBA_LEAGUE_ID:
            events = self._nba_events
        else:
            events = self._epl_events
        # Filter by season if needed
        filtered = [e for e in events if e.get("strSeason", CURRENT_SEASON) == season]
        return {"events": filtered or events}

    def _search_teams(self, league: str) -> dict:
        if "NBA" in league.upper():
            teams_data = NBA_TEAMS
        else:
            teams_data = PREMIER_LEAGUE_TEAMS
        return {
            "teams": [
                {
                    "idTeam": t[0],
                    "strTeam": t[1],
                    "strStadium": t[4],
                    "strLeague": league,
                }
                for t in teams_data
            ]
        }

    def _league_detail(self, league_id: str) -> dict:
        name = "English Premier League" if league_id == EPL_LEAGUE_ID else "NBA"
        return {
            "leagues": [{
                "idLeague": league_id,
                "strLeague": name,
                "strSport": "Soccer" if league_id == EPL_LEAGUE_ID else "Basketball",
                "strCurrentSeason": CURRENT_SEASON,
            }]
        }

    # ------------------------------------------------------------------
    # Inspection helpers (used in tests)
    # ------------------------------------------------------------------

    @property
    def call_count(self) -> int:
        return len(self._call_log)

    @property
    def calls(self) -> list[dict]:
        return list(self._call_log)

    def reset(self) -> None:
        self._call_log.clear()

    def get_results_for_league(self, league_id: str = EPL_LEAGUE_ID) -> list[dict]:
        """Return only completed match results (have a score)."""
        events = self._epl_events if league_id != NBA_LEAGUE_ID else self._nba_events
        return [e for e in events if e.get("intHomeScore") is not None]

    def get_fixtures_for_league(self, league_id: str = EPL_LEAGUE_ID) -> list[dict]:
        """Return only upcoming fixtures (no score yet)."""
        events = self._epl_events if league_id != NBA_LEAGUE_ID else self._nba_events
        return [e for e in events if e.get("intHomeScore") is None]
