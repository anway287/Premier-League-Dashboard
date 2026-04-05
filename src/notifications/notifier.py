"""
Notifier — publishes game events (GoalScored, MatchStarted, MatchEnded,
InjuryAlert) to an SNS topic so downstream subscribers (fan apps, dashboards,
data consumers) can react in near-real-time.

In tests the SNS client is replaced by a ``MockNotificationClient`` which
records every published message for assertion without network calls.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

import boto3

from src.config import cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    MATCH_STARTED = "MatchStarted"
    MATCH_ENDED = "MatchEnded"
    GOAL_SCORED = "GoalScored"
    PENALTY_AWARDED = "PenaltyAwarded"
    RED_CARD = "RedCard"
    YELLOW_CARD = "YellowCard"
    INJURY_ALERT = "InjuryAlert"
    VAR_REVIEW = "VARReview"
    SUBSTITUTION = "Substitution"


@dataclass
class GameEvent:
    event_type: EventType
    match_id: str
    league: str
    home_team: str
    away_team: str
    minute: int | None = None
    player: str | None = None
    detail: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_message(self) -> str:
        body: dict[str, Any] = {
            "event_type": self.event_type.value,
            "match_id": self.match_id,
            "league": self.league,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "detail": self.detail,
        }
        if self.minute is not None:
            body["minute"] = self.minute
        if self.player:
            body["player"] = self.player
        body.update(self.extra)
        return json.dumps(body)

    def to_subject(self) -> str:
        return f"[{self.league}] {self.event_type.value} — {self.home_team} vs {self.away_team}"


@dataclass
class NotificationResult:
    message_id: str
    event: GameEvent


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class SnsClient(Protocol):
    def publish(self, TopicArn: str, Message: str, Subject: str, MessageAttributes: dict) -> dict: ...


# ---------------------------------------------------------------------------
# Notifier
# ---------------------------------------------------------------------------

class Notifier:
    """
    Publishes ``GameEvent`` objects to an SNS topic.

    Parameters
    ----------
    sns_client : SnsClient | None
        Real boto3 SNS client (production) or MockNotificationClient (tests).
    topic_arn : str | None
        Override the topic ARN — tests inject a per-run topic.
    """

    def __init__(
        self,
        sns_client: SnsClient | None = None,
        topic_arn: str | None = None,
    ) -> None:
        self._sns = sns_client or boto3.client(
            "sns", region_name=cfg.aws_region, endpoint_url=cfg.localstack_endpoint
        )
        self._topic_arn = topic_arn or cfg.alerts_topic_arn

    def publish(self, event: GameEvent) -> NotificationResult:
        resp = self._sns.publish(
            TopicArn=self._topic_arn,
            Message=event.to_message(),
            Subject=event.to_subject(),
            MessageAttributes={
                "EventType": {"DataType": "String", "StringValue": event.event_type.value},
                "League": {"DataType": "String", "StringValue": event.league},
                "MatchId": {"DataType": "String", "StringValue": event.match_id},
            },
        )
        message_id = resp["MessageId"]
        logger.info("Published %s (msg_id=%s)", event.event_type.value, message_id)
        return NotificationResult(message_id=message_id, event=event)

    def publish_match_start(self, match_id: str, league: str, home: str, away: str) -> NotificationResult:
        return self.publish(GameEvent(
            event_type=EventType.MATCH_STARTED,
            match_id=match_id, league=league, home_team=home, away_team=away,
            detail=f"{home} vs {away} kicks off",
        ))

    def publish_goal(
        self, match_id: str, league: str, home: str, away: str,
        scorer: str, minute: int, home_score: int, away_score: int,
    ) -> NotificationResult:
        return self.publish(GameEvent(
            event_type=EventType.GOAL_SCORED,
            match_id=match_id, league=league, home_team=home, away_team=away,
            minute=minute, player=scorer,
            detail=f"GOAL! {scorer} scores in the {minute}' ({home} {home_score}–{away_score} {away})",
            extra={"home_score": home_score, "away_score": away_score},
        ))

    def publish_match_end(
        self, match_id: str, league: str, home: str, away: str,
        home_score: int, away_score: int,
    ) -> NotificationResult:
        result_str = "WIN" if home_score > away_score else ("DRAW" if home_score == away_score else "LOSS")
        return self.publish(GameEvent(
            event_type=EventType.MATCH_ENDED,
            match_id=match_id, league=league, home_team=home, away_team=away,
            detail=f"Full time: {home} {home_score}–{away_score} {away} ({result_str})",
            extra={"home_score": home_score, "away_score": away_score, "result": result_str},
        ))

    def publish_red_card(
        self, match_id: str, league: str, home: str, away: str, player: str, minute: int
    ) -> NotificationResult:
        return self.publish(GameEvent(
            event_type=EventType.RED_CARD,
            match_id=match_id, league=league, home_team=home, away_team=away,
            minute=minute, player=player,
            detail=f"RED CARD: {player} sent off in the {minute}'",
        ))
