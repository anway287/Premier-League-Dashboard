"""
StatsProcessor — reads raw match results from S3, computes league standings
and player stat aggregations, then writes processed records to DynamoDB.

DynamoDB key schema
───────────────────
  pk = "TEAM#<team_name>"           sk = "SEASON#<season>#STANDINGS"
  pk = "MATCH#<match_id>"           sk = "RESULT"
  pk = "PLAYER#<player_name>"       sk = "SEASON#<season>#STATS"
  pk = "LEAGUE#<league_id>"         sk = "SEASON#<season>#SUMMARY"
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

import boto3

from src.config import cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

@dataclass
class TeamStanding:
    team: str
    league: str
    season: str
    played: int = 0
    won: int = 0
    drawn: int = 0
    lost: int = 0
    goals_for: int = 0
    goals_against: int = 0

    @property
    def points(self) -> int:
        return self.won * 3 + self.drawn

    @property
    def goal_difference(self) -> int:
        return self.goals_for - self.goals_against

    def to_dynamo_item(self) -> dict:
        return {
            "pk": {"S": f"TEAM#{self.team}"},
            "sk": {"S": f"SEASON#{self.season}#STANDINGS"},
            "league": {"S": self.league},
            "played": {"N": str(self.played)},
            "won": {"N": str(self.won)},
            "drawn": {"N": str(self.drawn)},
            "lost": {"N": str(self.lost)},
            "goals_for": {"N": str(self.goals_for)},
            "goals_against": {"N": str(self.goals_against)},
            "points": {"N": str(self.points)},
            "goal_difference": {"N": str(self.goal_difference)},
        }


@dataclass
class LeagueSummary:
    league_id: str
    league_name: str
    season: str
    total_matches: int = 0
    total_goals: int = 0
    avg_goals_per_match: float = 0.0
    top_scorer_team: str = ""
    standings: list[TeamStanding] = field(default_factory=list)

    def to_dynamo_item(self) -> dict:
        return {
            "pk": {"S": f"LEAGUE#{self.league_id}"},
            "sk": {"S": f"SEASON#{self.season}#SUMMARY"},
            "league_name": {"S": self.league_name},
            "total_matches": {"N": str(self.total_matches)},
            "total_goals": {"N": str(self.total_goals)},
            "avg_goals_per_match": {"N": str(round(self.avg_goals_per_match, 2))},
            "top_scorer_team": {"S": self.top_scorer_team},
        }


@dataclass
class ProcessingReport:
    matches_processed: int = 0
    standings_written: int = 0
    summaries_written: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------

class S3Client(Protocol):
    def get_object(self, Bucket: str, Key: str) -> Any: ...
    def list_objects_v2(self, Bucket: str, Prefix: str) -> Any: ...


class DynamoClient(Protocol):
    def put_item(self, TableName: str, Item: dict) -> Any: ...
    def batch_write_item(self, RequestItems: dict) -> Any: ...


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class StatsProcessor:
    """
    Reads raw results JSON files from S3 and computes standings + summaries,
    writing them into DynamoDB.

    Designed for hermetic testing: inject mock clients, control exact data.
    """

    def __init__(
        self,
        s3_client: S3Client | None = None,
        dynamo_client: DynamoClient | None = None,
        bucket: str | None = None,
        table: str | None = None,
    ) -> None:
        self._s3 = s3_client or boto3.client(
            "s3", region_name=cfg.aws_region, endpoint_url=cfg.localstack_endpoint
        )
        self._dynamo = dynamo_client or boto3.client(
            "dynamodb", region_name=cfg.aws_region, endpoint_url=cfg.localstack_endpoint
        )
        self._bucket = bucket or cfg.raw_data_bucket
        self._table = table or cfg.stats_table

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_league_season(self, league_id: str, season: str) -> ProcessingReport:
        """
        Load all result files for league/season from S3, compute standings,
        write to DynamoDB.
        """
        report = ProcessingReport()
        results = self._load_results(league_id, season)
        if not results:
            logger.warning("No results found for league %s season %s", league_id, season)
            return report

        standings_map: dict[str, TeamStanding] = {}
        total_goals = 0

        for r in results:
            report.matches_processed += 1
            home = r["home_team"]
            away = r["away_team"]
            hs = int(r["home_score"])
            as_ = int(r["away_score"])
            total_goals += hs + as_

            for team in (home, away):
                if team not in standings_map:
                    standings_map[team] = TeamStanding(
                        team=team, league=r.get("league", ""), season=season
                    )

            h_standing = standings_map[home]
            a_standing = standings_map[away]

            h_standing.played += 1
            a_standing.played += 1
            h_standing.goals_for += hs
            h_standing.goals_against += as_
            a_standing.goals_for += as_
            a_standing.goals_against += hs

            if hs > as_:
                h_standing.won += 1
                a_standing.lost += 1
            elif as_ > hs:
                a_standing.won += 1
                h_standing.lost += 1
            else:
                h_standing.drawn += 1
                a_standing.drawn += 1

            # Persist individual match result
            self._put_match_result(r)

        # Write standings to DynamoDB (batch in groups of 25)
        standings = sorted(standings_map.values(), key=lambda s: (-s.points, -s.goal_difference))
        self._batch_write_standings(standings)
        report.standings_written = len(standings)

        # Write league summary
        top_team = standings[0].team if standings else ""
        avg = total_goals / report.matches_processed if report.matches_processed else 0.0
        summary = LeagueSummary(
            league_id=league_id,
            league_name=results[0].get("league", "") if results else "",
            season=season,
            total_matches=report.matches_processed,
            total_goals=total_goals,
            avg_goals_per_match=avg,
            top_scorer_team=top_team,
            standings=standings,
        )
        self._dynamo.put_item(TableName=self._table, Item=summary.to_dynamo_item())
        report.summaries_written = 1
        logger.info(
            "Processed league %s season %s: %d matches, %d teams",
            league_id, season, report.matches_processed, report.standings_written,
        )
        return report

    def get_standings(self, league_id: str, season: str) -> list[TeamStanding]:
        """Query DynamoDB for the computed standings of a league season."""
        from boto3.dynamodb.conditions import Key as DKey
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=cfg.aws_region,
            endpoint_url=cfg.localstack_endpoint,
        )
        table = dynamodb.Table(self._table)
        resp = table.query(
            IndexName="sk-pk-index",
            KeyConditionExpression=DKey("sk").eq(f"SEASON#{season}#STANDINGS"),
        )
        standings = []
        for item in resp.get("Items", []):
            team_name = item["pk"].replace("TEAM#", "")
            s = TeamStanding(
                team=team_name,
                league=item.get("league", ""),
                season=season,
                played=int(item.get("played", 0)),
                won=int(item.get("won", 0)),
                drawn=int(item.get("drawn", 0)),
                lost=int(item.get("lost", 0)),
                goals_for=int(item.get("goals_for", 0)),
                goals_against=int(item.get("goals_against", 0)),
            )
            standings.append(s)
        return sorted(standings, key=lambda x: (-x.points, -x.goal_difference))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_results(self, league_id: str, season: str) -> list[dict]:
        prefix = f"results/{league_id}/{season}/"
        resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        results: list[dict] = []
        for obj in resp.get("Contents", []):
            body = self._s3.get_object(Bucket=self._bucket, Key=obj["Key"])["Body"].read()
            results.extend(json.loads(body))
        return results

    def _put_match_result(self, r: dict) -> None:
        item = {
            "pk": {"S": f"MATCH#{r['match_id']}"},
            "sk": {"S": "RESULT"},
            "home_team": {"S": r["home_team"]},
            "away_team": {"S": r["away_team"]},
            "home_score": {"N": str(r["home_score"])},
            "away_score": {"N": str(r["away_score"])},
            "league": {"S": r.get("league", "")},
            "season": {"S": r.get("season", "")},
            "played_at": {"S": r.get("played_at", "")},
        }
        self._dynamo.put_item(TableName=self._table, Item=item)

    def _batch_write_standings(self, standings: list[TeamStanding]) -> None:
        batch_size = 25
        for i in range(0, len(standings), batch_size):
            chunk = standings[i : i + batch_size]
            request_items = {
                self._table: [
                    {"PutRequest": {"Item": s.to_dynamo_item()}} for s in chunk
                ]
            }
            self._dynamo.batch_write_item(RequestItems=request_items)
