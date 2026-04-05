"""
Data integrity tests — verify invariants that must hold across the full dataset.
These catch subtle bugs that unit tests miss (off-by-one, sign errors, etc).
"""
from __future__ import annotations

import pytest

from mocks.sports_api_mock import EPL_LEAGUE_ID, CURRENT_SEASON
from tests.conftest import HermeticEnv


@pytest.mark.epl
@pytest.mark.slow
class TestLeagueInvariants:

    @pytest.fixture(autouse=True)
    def _run_pipeline(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

    def test_played_equals_won_plus_drawn_plus_lost(self, hermetic: HermeticEnv):
        for item in hermetic.dynamo.items_with_sk_suffix("STANDINGS"):
            p = int(item["played"]["N"])
            w = int(item["won"]["N"])
            d = int(item["drawn"]["N"])
            l = int(item["lost"]["N"])
            team = item["pk"]["S"]
            assert p == w + d + l, f"{team}: played({p}) != W({w})+D({d})+L({l})"

    def test_goal_difference_equals_gf_minus_ga(self, hermetic: HermeticEnv):
        for item in hermetic.dynamo.items_with_sk_suffix("STANDINGS"):
            gf = int(item["goals_for"]["N"])
            ga = int(item["goals_against"]["N"])
            gd = int(item["goal_difference"]["N"])
            team = item["pk"]["S"]
            assert gd == gf - ga, f"{team}: GD({gd}) != GF({gf})-GA({ga})"

    def test_points_equals_3w_plus_d(self, hermetic: HermeticEnv):
        for item in hermetic.dynamo.items_with_sk_suffix("STANDINGS"):
            pts = int(item["points"]["N"])
            w = int(item["won"]["N"])
            d = int(item["drawn"]["N"])
            team = item["pk"]["S"]
            assert pts == 3 * w + d, f"{team}: points({pts}) != 3*W({w})+D({d})"

    def test_league_wide_gf_equals_ga(self, hermetic: HermeticEnv):
        items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        total_gf = sum(int(i["goals_for"]["N"]) for i in items)
        total_ga = sum(int(i["goals_against"]["N"]) for i in items)
        assert total_gf == total_ga

    def test_all_teams_have_positive_played_count(self, hermetic: HermeticEnv):
        for item in hermetic.dynamo.items_with_sk_suffix("STANDINGS"):
            assert int(item["played"]["N"]) > 0, f"{item['pk']['S']} has 0 games played"

    def test_no_negative_stats(self, hermetic: HermeticEnv):
        fields = ["played", "won", "drawn", "lost", "goals_for", "goals_against", "points"]
        for item in hermetic.dynamo.items_with_sk_suffix("STANDINGS"):
            for field in fields:
                val = int(item[field]["N"])
                team = item["pk"]["S"]
                assert val >= 0, f"{team}.{field} is negative: {val}"

    def test_exactly_20_teams_in_standings(self, hermetic: HermeticEnv):
        items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        assert len(items) == 20


class TestStorageIntegrity:

    def test_s3_put_then_get_round_trip(self, hermetic: HermeticEnv):
        data = {"team": "Liverpool", "goals": 45, "season": "2023-24"}
        hermetic.store.put_json("test/round-trip.json", data)
        retrieved = hermetic.store.get_json("test/round-trip.json")
        assert retrieved == data

    def test_dynamo_put_then_get_round_trip(self, hermetic: HermeticEnv):
        item = {
            "pk": {"S": "TEAM#TestFC"},
            "sk": {"S": "SEASON#2023-24#STANDINGS"},
            "points": {"N": "42"},
        }
        hermetic.store.put_item(item)
        retrieved = hermetic.store.get_item("TEAM#TestFC", "SEASON#2023-24#STANDINGS")
        assert retrieved is not None
        assert retrieved["points"]["N"] == "42"

    def test_list_keys_returns_correct_prefix(self, hermetic: HermeticEnv):
        hermetic.store.put_json("alpha/file1.json", {"x": 1})
        hermetic.store.put_json("alpha/file2.json", {"x": 2})
        hermetic.store.put_json("beta/file3.json", {"x": 3})

        alpha_keys = hermetic.store.list_keys("alpha/")
        beta_keys = hermetic.store.list_keys("beta/")

        assert len(alpha_keys) == 2
        assert len(beta_keys) == 1
        assert all(k.startswith("alpha/") for k in alpha_keys)


class TestSimulatorDeterminism:
    """The sports simulator must be deterministic — same data every run."""

    def test_same_results_on_repeated_calls(self, hermetic: HermeticEnv):
        data1 = hermetic.sports_api.get_results_for_league(EPL_LEAGUE_ID)
        hermetic.sports_api.reset()
        data2 = hermetic.sports_api.get_results_for_league(EPL_LEAGUE_ID)

        assert len(data1) == len(data2)
        for r1, r2 in zip(data1[:10], data2[:10]):
            assert r1["idEvent"] == r2["idEvent"]
            assert r1["intHomeScore"] == r2["intHomeScore"]

    def test_fixture_count_consistent(self, hermetic: HermeticEnv):
        fixtures = hermetic.sports_api.get_fixtures_for_league(EPL_LEAGUE_ID)
        # Gameweeks 29-38: 10 matches × 10 GWs = 100 fixtures (approximately)
        assert len(fixtures) > 50

    def test_results_count_consistent(self, hermetic: HermeticEnv):
        results = hermetic.sports_api.get_results_for_league(EPL_LEAGUE_ID)
        # Gameweeks 1-28: ~280 matches
        assert len(results) > 100
