"""
test_processing.py — Integration tests for StatsProcessor.

Tests validate standings computation, DynamoDB write patterns, and
correctness of derived stats (points, goal difference, ranking).
"""
from __future__ import annotations

import json

import pytest

from mocks.sports_api_mock import EPL_LEAGUE_ID, CURRENT_SEASON
from tests.conftest import HermeticEnv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_results(hermetic: HermeticEnv, results: list[dict]) -> str:
    """Write a results JSON file to mock S3 and return the key."""
    key = f"results/{hermetic.epl_league_id}/{hermetic.season}/test-seed.json"
    hermetic.s3.put_object(
        Bucket=hermetic.bucket,
        Key=key,
        Body=json.dumps(results).encode(),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# Standings computation
# ---------------------------------------------------------------------------

class TestStandingsComputation:

    def test_points_calculated_correctly(self, hermetic: HermeticEnv):
        """Win = 3pts, Draw = 1pt, Loss = 0pts."""
        results = [
            {"match_id": "m1", "home_team": "Man City", "away_team": "Arsenal",
             "home_score": 3, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
            {"match_id": "m2", "home_team": "Liverpool", "away_team": "Man City",
             "home_score": 2, "away_score": 2, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-13T15:00:00"},
        ]
        _seed_results(hermetic, results)

        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)
        assert report.matches_processed == 2

        # Man City: 1W 1D = 4pts
        city_item = hermetic.dynamo.get_item_plain(
            f"TEAM#Man City", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert city_item is not None
        assert int(city_item["points"]) == 4
        assert int(city_item["won"]) == 1
        assert int(city_item["drawn"]) == 1
        assert int(city_item["lost"]) == 0

    def test_goal_difference_computed(self, hermetic: HermeticEnv):
        results = [
            {"match_id": "m1", "home_team": "Tottenham", "away_team": "Chelsea",
             "home_score": 4, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        spurs_item = hermetic.dynamo.get_item_plain(
            "TEAM#Tottenham", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert int(spurs_item["goals_for"]) == 4
        assert int(spurs_item["goals_against"]) == 1
        assert int(spurs_item["goal_difference"]) == 3

        chelsea_item = hermetic.dynamo.get_item_plain(
            "TEAM#Chelsea", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert int(chelsea_item["goal_difference"]) == -3

    def test_draw_gives_one_point_each(self, hermetic: HermeticEnv):
        results = [
            {"match_id": "m1", "home_team": "Everton", "away_team": "Fulham",
             "home_score": 1, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        for team in ("Everton", "Fulham"):
            item = hermetic.dynamo.get_item_plain(
                f"TEAM#{team}", f"SEASON#{CURRENT_SEASON}#STANDINGS"
            )
            assert int(item["points"]) == 1
            assert int(item["drawn"]) == 1
            assert int(item["won"]) == 0
            assert int(item["lost"]) == 0

    def test_loss_gives_zero_points(self, hermetic: HermeticEnv):
        results = [
            {"match_id": "m1", "home_team": "Sheffield United", "away_team": "Burnley",
             "home_score": 0, "away_score": 3, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        loser = hermetic.dynamo.get_item_plain(
            "TEAM#Sheffield United", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert int(loser["points"]) == 0
        assert int(loser["lost"]) == 1

    @pytest.mark.parametrize("home_score,away_score,expected_home_pts,expected_away_pts", [
        (3, 0, 3, 0),
        (0, 2, 0, 3),
        (1, 1, 1, 1),
        (0, 0, 1, 1),
        (5, 3, 3, 0),
    ])
    def test_points_parametrized(
        self, hermetic: HermeticEnv,
        home_score, away_score, expected_home_pts, expected_away_pts
    ):
        results = [
            {"match_id": "m1", "home_team": "HomeFC", "away_team": "AwayFC",
             "home_score": home_score, "away_score": away_score, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        home_item = hermetic.dynamo.get_item_plain(
            "TEAM#HomeFC", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        away_item = hermetic.dynamo.get_item_plain(
            "TEAM#AwayFC", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert int(home_item["points"]) == expected_home_pts
        assert int(away_item["points"]) == expected_away_pts


# ---------------------------------------------------------------------------
# Multi-match season processing
# ---------------------------------------------------------------------------

class TestMultiMatchProcessing:

    def test_accumulates_stats_across_matches(self, hermetic: HermeticEnv):
        """A team playing 3 matches should have played=3 in its standing."""
        team = "Newcastle United"
        results = [
            {"match_id": f"m{i}", "home_team": team, "away_team": f"Opponent{i}",
             "home_score": 2, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": f"2024-01-{6+i*7:02d}T15:00:00"}
            for i in range(3)
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        item = hermetic.dynamo.get_item_plain(
            f"TEAM#{team}", f"SEASON#{CURRENT_SEASON}#STANDINGS"
        )
        assert int(item["played"]) == 3
        assert int(item["won"]) == 3
        assert int(item["goals_for"]) == 6
        assert int(item["goals_against"]) == 3

    def test_individual_match_results_persisted(self, hermetic: HermeticEnv):
        """Each match should be persisted as a separate MATCH# item."""
        results = [
            {"match_id": "m-abc", "home_team": "Brighton", "away_team": "Wolves",
             "home_score": 2, "away_score": 0, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        match_item = hermetic.dynamo.get_item_plain("MATCH#m-abc", "RESULT")
        assert match_item is not None
        assert match_item["home_team"] == "Brighton"
        assert match_item["away_team"] == "Wolves"
        assert int(match_item["home_score"]) == 2
        assert int(match_item["away_score"]) == 0


# ---------------------------------------------------------------------------
# League summary
# ---------------------------------------------------------------------------

class TestLeagueSummary:

    def test_league_summary_written_to_dynamo(self, hermetic: HermeticEnv):
        results = [
            {"match_id": "m1", "home_team": "A", "away_team": "B",
             "home_score": 3, "away_score": 2, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
        ]
        _seed_results(hermetic, results)
        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)
        assert report.summaries_written == 1

        summary = hermetic.dynamo.get_item_plain(
            f"LEAGUE#{hermetic.epl_league_id}",
            f"SEASON#{CURRENT_SEASON}#SUMMARY",
        )
        assert summary is not None
        assert int(summary["total_matches"]) == 1
        assert int(summary["total_goals"]) == 5

    def test_avg_goals_per_match(self, hermetic: HermeticEnv):
        results = [
            {"match_id": "m1", "home_team": "A", "away_team": "B",
             "home_score": 2, "away_score": 2, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
            {"match_id": "m2", "home_team": "C", "away_team": "D",
             "home_score": 3, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-13T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        summary = hermetic.dynamo.get_item_plain(
            f"LEAGUE#{hermetic.epl_league_id}",
            f"SEASON#{CURRENT_SEASON}#SUMMARY",
        )
        # Total goals = 4+4 = 8, matches = 2, avg = 4.0
        assert float(summary["avg_goals_per_match"]) == 4.0

    def test_top_scorer_team_is_highest_goals_for(self, hermetic: HermeticEnv):
        """Top scorer team should be the one with the most points (first in standings)."""
        results = [
            {"match_id": "m1", "home_team": "Liverpool", "away_team": "Burnley",
             "home_score": 5, "away_score": 0, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-06T15:00:00"},
            {"match_id": "m2", "home_team": "Liverpool", "away_team": "Luton",
             "home_score": 4, "away_score": 1, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-13T15:00:00"},
            {"match_id": "m3", "home_team": "Arsenal", "away_team": "Burnley",
             "home_score": 1, "away_score": 0, "league": "EPL",
             "season": CURRENT_SEASON, "played_at": "2024-01-20T15:00:00"},
        ]
        _seed_results(hermetic, results)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        summary = hermetic.dynamo.get_item_plain(
            f"LEAGUE#{hermetic.epl_league_id}",
            f"SEASON#{CURRENT_SEASON}#SUMMARY",
        )
        assert summary["top_scorer_team"] == "Liverpool"


# ---------------------------------------------------------------------------
# Full season simulation (uses simulator data directly)
# ---------------------------------------------------------------------------

@pytest.mark.epl
@pytest.mark.slow
class TestFullSeasonProcessing:

    def test_full_epl_season_produces_20_teams(self, hermetic: HermeticEnv):
        """Processing a full EPL season should yield 20 teams in the table."""
        # First ingest all results
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        # Then process
        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        assert report.matches_processed > 0
        # 20 teams in EPL (our simulator has 20)
        standings_items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        assert len(standings_items) == 20

    def test_full_season_total_points_correct(self, hermetic: HermeticEnv):
        """
        In EPL: each match distributes exactly 3 pts (win) or 2 pts (draw).
        Total points across all 20 teams must be between 2×n and 3×n matches.
        """
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        standings_items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        total_points = sum(int(item["points"]["N"]) for item in standings_items)
        n_matches = report.matches_processed

        assert 2 * n_matches <= total_points <= 3 * n_matches, (
            f"Total points {total_points} outside valid range "
            f"[{2*n_matches}, {3*n_matches}] for {n_matches} matches"
        )

    def test_full_season_goals_for_equals_goals_against_league_wide(self, hermetic: HermeticEnv):
        """
        League-wide: sum(goals_for) must equal sum(goals_against).
        Every goal scored by home is a goal conceded by away.
        """
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        standings_items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        total_gf = sum(int(item["goals_for"]["N"]) for item in standings_items)
        total_ga = sum(int(item["goals_against"]["N"]) for item in standings_items)
        assert total_gf == total_ga, (
            f"League-wide GF ({total_gf}) ≠ GA ({total_ga}) — data integrity violation"
        )
