"""
test_notifications.py — Integration tests for the Notifier / GameEvent system.

Validates that the right SNS messages are published for each event type,
with correct fields, subjects, and message attributes.  The MockNotificationClient
captures every publish call so tests never touch a live SNS topic.
"""
from __future__ import annotations

import json

import pytest

from src.notifications.notifier import EventType, GameEvent, Notifier
from tests.conftest import HermeticEnv

MATCH_ID = "epl-match-001"
LEAGUE = "English Premier League"
HOME = "Manchester City"
AWAY = "Arsenal"


# ---------------------------------------------------------------------------
# Match lifecycle events
# ---------------------------------------------------------------------------

class TestMatchLifecycleEvents:

    def test_match_started_event_published(self, hermetic: HermeticEnv):
        result = hermetic.notifier.publish_match_start(MATCH_ID, LEAGUE, HOME, AWAY)

        assert result.message_id is not None
        hermetic.sns.assert_published("MatchStarted", count=1)

    def test_match_started_body_fields(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_start(MATCH_ID, LEAGUE, HOME, AWAY)

        msg = hermetic.sns.latest
        assert msg["event_type"] == "MatchStarted"
        assert msg["match_id"] == MATCH_ID
        assert msg["home_team"] == HOME
        assert msg["away_team"] == AWAY
        assert msg["league"] == LEAGUE

    def test_match_ended_with_correct_result_label(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_end(MATCH_ID, LEAGUE, HOME, AWAY, 2, 1)

        msg = hermetic.sns.latest
        assert msg["event_type"] == "MatchEnded"
        assert msg["result"] == "WIN"
        assert msg["home_score"] == 2
        assert msg["away_score"] == 1

    def test_draw_result_label(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_end(MATCH_ID, LEAGUE, HOME, AWAY, 1, 1)
        assert hermetic.sns.latest["result"] == "DRAW"

    def test_away_win_result_label(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_end(MATCH_ID, LEAGUE, HOME, AWAY, 0, 3)
        assert hermetic.sns.latest["result"] == "LOSS"

    def test_full_match_lifecycle_produces_two_events(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_start(MATCH_ID, LEAGUE, HOME, AWAY)
        hermetic.notifier.publish_match_end(MATCH_ID, LEAGUE, HOME, AWAY, 3, 1)

        assert hermetic.sns.count == 2
        types = [m.body["event_type"] for m in hermetic.sns.messages]
        assert types == ["MatchStarted", "MatchEnded"]


# ---------------------------------------------------------------------------
# Goal events
# ---------------------------------------------------------------------------

class TestGoalEvents:

    def test_goal_event_published(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_goal(
            MATCH_ID, LEAGUE, HOME, AWAY, "Erling Haaland", 23, 1, 0
        )
        hermetic.sns.assert_published("GoalScored", count=1)

    def test_goal_event_has_scorer_and_minute(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_goal(
            MATCH_ID, LEAGUE, HOME, AWAY, "Erling Haaland", 23, 1, 0
        )
        msg = hermetic.sns.latest
        assert msg["player"] == "Erling Haaland"
        assert msg["minute"] == 23

    def test_goal_detail_contains_scoreline(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_goal(
            MATCH_ID, LEAGUE, HOME, AWAY, "Bukayo Saka", 67, 0, 2
        )
        msg = hermetic.sns.latest
        assert "67'" in msg["detail"] or "67" in msg["detail"]
        assert "Bukayo Saka" in msg["detail"]

    def test_multiple_goals_accumulate(self, hermetic: HermeticEnv):
        goals = [
            ("Erling Haaland", 15, 1, 0),
            ("Phil Foden", 33, 2, 0),
            ("Bukayo Saka", 45, 2, 1),
            ("Erling Haaland", 71, 3, 1),
        ]
        for scorer, minute, hs, as_ in goals:
            hermetic.notifier.publish_goal(MATCH_ID, LEAGUE, HOME, AWAY, scorer, minute, hs, as_)

        hermetic.sns.assert_published("GoalScored", count=4)
        # Haaland scored twice
        haaland_goals = [
            m for m in hermetic.sns.messages_of_type("GoalScored")
            if m.get("player") == "Erling Haaland"
        ]
        assert len(haaland_goals) == 2

    def test_away_goal_correct_team_reference(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_goal(
            MATCH_ID, LEAGUE, HOME, AWAY, "Bukayo Saka", 55, 0, 1
        )
        msg = hermetic.sns.latest
        # score updated: City 0-1 Arsenal
        assert msg["home_score"] == 0
        assert msg["away_score"] == 1


# ---------------------------------------------------------------------------
# Disciplinary events
# ---------------------------------------------------------------------------

class TestDisciplinaryEvents:

    def test_red_card_event_published(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_red_card(MATCH_ID, LEAGUE, HOME, AWAY, "Gabriel", 55)
        hermetic.sns.assert_published("RedCard", count=1)

    def test_red_card_body_has_player_and_minute(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_red_card(MATCH_ID, LEAGUE, HOME, AWAY, "Gabriel", 55)
        msg = hermetic.sns.latest
        assert msg["player"] == "Gabriel"
        assert msg["minute"] == 55
        assert "55'" in msg["detail"]

    def test_custom_event_type_published(self, hermetic: HermeticEnv):
        event = GameEvent(
            event_type=EventType.VAR_REVIEW,
            match_id=MATCH_ID,
            league=LEAGUE,
            home_team=HOME,
            away_team=AWAY,
            detail="VAR reviewing potential handball",
        )
        hermetic.notifier.publish(event)
        hermetic.sns.assert_published("VARReview", count=1)


# ---------------------------------------------------------------------------
# Message structure
# ---------------------------------------------------------------------------

class TestMessageStructure:

    def test_subject_contains_league_and_event_type(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_start(MATCH_ID, LEAGUE, HOME, AWAY)
        subject = hermetic.sns.messages[0].subject
        assert LEAGUE in subject
        assert "MatchStarted" in subject

    def test_message_attributes_set_correctly(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_goal(
            MATCH_ID, LEAGUE, HOME, AWAY, "Erling Haaland", 9, 1, 0
        )
        attrs = hermetic.sns.messages[0].attributes
        assert attrs["EventType"] == "GoalScored"
        assert attrs["League"] == LEAGUE
        assert attrs["MatchId"] == MATCH_ID

    def test_message_body_is_valid_json(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_start(MATCH_ID, LEAGUE, HOME, AWAY)
        raw = hermetic.sns.messages[0].raw_message
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)

    def test_no_notifications_published_before_first_event(self, hermetic: HermeticEnv):
        assert hermetic.sns.count == 0
        hermetic.sns.assert_not_published("GoalScored")
        hermetic.sns.assert_not_published("MatchStarted")


# ---------------------------------------------------------------------------
# Isolation between tests
# ---------------------------------------------------------------------------

class TestNotificationIsolation:

    def test_each_test_starts_with_empty_sink(self, hermetic: HermeticEnv):
        """Verify the fixture gives a fresh sink — previous test's messages are gone."""
        assert hermetic.sns.count == 0

    def test_publish_and_verify_no_cross_contamination(self, hermetic: HermeticEnv):
        hermetic.notifier.publish_match_start("isolate-match", LEAGUE, HOME, AWAY)
        assert hermetic.sns.count == 1
        # If this is the only event, count should be exactly 1 — no leakage
        assert len(hermetic.sns.messages_of_type("MatchEnded")) == 0
