"""
SportsIngester — fetches match fixtures, results, and player events
from the Sports API and lands raw JSON onto S3.

The class is designed so tests can inject a mock HTTP session and a mock
S3 client, keeping the logic fully testable without live infrastructure.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

import boto3
import requests

from src.config import cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

@dataclass
class MatchFixture:
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff_utc: datetime
    venue: str
    season: str


@dataclass
class MatchResult:
    match_id: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    league: str
    season: str
    played_at: datetime
    events: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class IngestionReport:
    fixtures_fetched: int = 0
    results_fetched: int = 0
    bytes_written: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Protocols — allow mock injection in tests
# ---------------------------------------------------------------------------

class HttpSession(Protocol):
    def get(self, url: str, params: dict | None = None, timeout: int = 10) -> Any: ...


class S3Client(Protocol):
    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> Any: ...


# ---------------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------------

class SportsIngester:
    """
    Fetches data from the Sports API and stores raw JSON blobs on S3.

    Parameters
    ----------
    http_session : HttpSession
        ``requests.Session()`` in production; ``MockHttpSession`` in tests.
    s3_client : S3Client | None
        Boto3 S3 client.  Defaults to a client pointed at LocalStack.
    bucket : str
        Override the destination bucket (tests inject a per-run bucket).
    """

    def __init__(
        self,
        http_session: HttpSession | None = None,
        s3_client: S3Client | None = None,
        bucket: str | None = None,
    ) -> None:
        self._http = http_session or requests.Session()
        self._s3 = s3_client or boto3.client(
            "s3",
            region_name=cfg.aws_region,
            endpoint_url=cfg.localstack_endpoint,
        )
        self._bucket = bucket or cfg.raw_data_bucket
        self._base_url = cfg.sports_api_base_url

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest_league_fixtures(self, league_id: str, season: str) -> IngestionReport:
        """Fetch upcoming fixtures for a league season and persist to S3."""
        report = IngestionReport()
        url = f"{self._base_url}/eventsseason.php"
        try:
            resp = self._http.get(url, params={"id": league_id, "s": season}, timeout=cfg.sports_api_timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.error("Failed to fetch fixtures for league %s: %s", league_id, exc)
            report.errors.append(str(exc))
            return report

        events = payload.get("events") or []
        fixtures = [self._parse_fixture(e) for e in events if e.get("intHomeScore") is None]
        report.fixtures_fetched = len(fixtures)

        key = f"fixtures/{league_id}/{season}/{date.today().isoformat()}.json"
        body = json.dumps([self._fixture_to_dict(f) for f in fixtures]).encode()
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        report.bytes_written += len(body)
        logger.info("Ingested %d fixtures for league %s → s3://%s/%s", len(fixtures), league_id, self._bucket, key)
        return report

    def ingest_league_results(self, league_id: str, season: str) -> IngestionReport:
        """Fetch completed match results for a league season and persist to S3."""
        report = IngestionReport()
        url = f"{self._base_url}/eventsseason.php"
        try:
            resp = self._http.get(url, params={"id": league_id, "s": season}, timeout=cfg.sports_api_timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.error("Failed to fetch results for league %s: %s", league_id, exc)
            report.errors.append(str(exc))
            return report

        events = payload.get("events") or []
        results = [self._parse_result(e) for e in events if e.get("intHomeScore") is not None]
        report.results_fetched = len(results)

        key = f"results/{league_id}/{season}/{date.today().isoformat()}.json"
        body = json.dumps([self._result_to_dict(r) for r in results]).encode()
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        report.bytes_written += len(body)
        logger.info("Ingested %d results for league %s → s3://%s/%s", len(results), league_id, self._bucket, key)
        return report

    def ingest_player_stats(self, team_id: str, season: str) -> IngestionReport:
        """Fetch player stats for a team in a given season."""
        report = IngestionReport()
        url = f"{self._base_url}/lookup_all_players.php"
        try:
            resp = self._http.get(url, params={"id": team_id}, timeout=cfg.sports_api_timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.error("Failed to fetch players for team %s: %s", team_id, exc)
            report.errors.append(str(exc))
            return report

        players = payload.get("player") or []
        key = f"players/{team_id}/{season}.json"
        body = json.dumps(players).encode()
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        report.bytes_written += len(body)
        report.fixtures_fetched = len(players)
        return report

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_fixture(event: dict) -> MatchFixture:
        dt_str = f"{event.get('dateEvent', '')} {event.get('strTime', '00:00:00')}"
        try:
            kickoff = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            kickoff = datetime.utcnow()
        return MatchFixture(
            match_id=str(event.get("idEvent", "")),
            home_team=event.get("strHomeTeam", ""),
            away_team=event.get("strAwayTeam", ""),
            league=event.get("strLeague", ""),
            kickoff_utc=kickoff,
            venue=event.get("strVenue", ""),
            season=event.get("strSeason", ""),
        )

    @staticmethod
    def _parse_result(event: dict) -> MatchResult:
        dt_str = f"{event.get('dateEvent', '')} {event.get('strTime', '00:00:00')}"
        try:
            played_at = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            played_at = datetime.utcnow()
        return MatchResult(
            match_id=str(event.get("idEvent", "")),
            home_team=event.get("strHomeTeam", ""),
            away_team=event.get("strAwayTeam", ""),
            home_score=int(event.get("intHomeScore") or 0),
            away_score=int(event.get("intAwayScore") or 0),
            league=event.get("strLeague", ""),
            season=event.get("strSeason", ""),
            played_at=played_at,
        )

    @staticmethod
    def _fixture_to_dict(f: MatchFixture) -> dict:
        return {
            "match_id": f.match_id,
            "home_team": f.home_team,
            "away_team": f.away_team,
            "league": f.league,
            "kickoff_utc": f.kickoff_utc.isoformat(),
            "venue": f.venue,
            "season": f.season,
        }

    @staticmethod
    def _result_to_dict(r: MatchResult) -> dict:
        return {
            "match_id": r.match_id,
            "home_team": r.home_team,
            "away_team": r.away_team,
            "home_score": r.home_score,
            "away_score": r.away_score,
            "league": r.league,
            "season": r.season,
            "played_at": r.played_at.isoformat(),
            "events": r.events,
        }
