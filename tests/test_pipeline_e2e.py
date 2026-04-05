"""
End-to-end tests: ingest → process → notify full pipeline.
"""
from __future__ import annotations

import pytest

from mocks.sports_api_mock import EPL_LEAGUE_ID, NBA_LEAGUE_ID, CURRENT_SEASON
from tests.conftest import HermeticEnv


@pytest.mark.e2e
@pytest.mark.epl
class TestEPLPipelineE2E:

    def test_ingest_then_process_produces_standings(self, hermetic: HermeticEnv):
        # Step 1: ingest
        ingest_report = hermetic.ingester.ingest_league_results(
            hermetic.epl_league_id, hermetic.season
        )
        assert ingest_report.results_fetched > 0

        # Step 2: process
        proc_report = hermetic.processor.process_league_season(
            hermetic.epl_league_id, hermetic.season
        )
        assert proc_report.matches_processed == ingest_report.results_fetched
        assert proc_report.standings_written == 20

    def test_ingest_process_standings_sum_to_valid_points(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        total_pts = sum(int(i["points"]["N"]) for i in items)
        n = report.matches_processed

        # Each match gives 2 (draw) or 3 (decisive) points total
        assert 2 * n <= total_pts <= 3 * n

    def test_league_summary_reflects_ingested_data(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        summary = hermetic.dynamo.get_item_plain(
            f"LEAGUE#{hermetic.epl_league_id}",
            f"SEASON#{CURRENT_SEASON}#SUMMARY",
        )
        assert summary is not None
        assert int(summary["total_matches"]) > 0
        assert float(summary["avg_goals_per_match"]) > 0

    def test_full_pipeline_with_notifications(self, hermetic: HermeticEnv):
        # Ingest + process
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)

        # Simulate match events being published
        hermetic.notifier.publish_match_start("live-001", "EPL", "Man City", "Arsenal")
        hermetic.notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Haaland", 15, 1, 0)
        hermetic.notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Saka", 67, 1, 1)
        hermetic.notifier.publish_match_end("live-001", "EPL", "Man City", "Arsenal", 1, 1)

        # All events captured
        assert hermetic.sns.count == 4
        hermetic.sns.assert_published("MatchStarted", count=1)
        hermetic.sns.assert_published("GoalScored", count=2)
        hermetic.sns.assert_published("MatchEnded", count=1)

        # Final result is a draw
        end_msg = hermetic.sns.messages_of_type("MatchEnded")[0]
        assert end_msg["result"] == "DRAW"

    def test_s3_data_present_after_full_pipeline(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)

        keys = hermetic.s3.all_keys()
        result_keys = [k for k in keys if k.startswith("results/")]
        fixture_keys = [k for k in keys if k.startswith("fixtures/")]
        assert len(result_keys) >= 1
        assert len(fixture_keys) >= 1


@pytest.mark.e2e
@pytest.mark.nba
class TestNBAPipelineE2E:

    def test_nba_full_pipeline(self, hermetic: HermeticEnv):
        ingest = hermetic.ingester.ingest_league_results(hermetic.nba_league_id, hermetic.season)
        assert ingest.results_fetched > 0

        proc = hermetic.processor.process_league_season(hermetic.nba_league_id, hermetic.season)
        assert proc.matches_processed > 0
        assert proc.standings_written > 0

    def test_nba_and_epl_pipelines_do_not_interfere(self, hermetic: HermeticEnv):
        # Both leagues use the same bucket/table but different S3 key prefixes
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.ingester.ingest_league_results(hermetic.nba_league_id, hermetic.season)

        epl_keys = hermetic.s3.keys_with_prefix(f"results/{hermetic.epl_league_id}/")
        nba_keys = hermetic.s3.keys_with_prefix(f"results/{hermetic.nba_league_id}/")

        assert len(epl_keys) == 1
        assert len(nba_keys) == 1
        # Keys are distinct
        assert not set(epl_keys) & set(nba_keys)


@pytest.mark.e2e
class TestPipelineResilience:

    def test_process_with_no_results_returns_empty_report(self, hermetic: HermeticEnv):
        # Nothing ingested — processor should handle gracefully
        report = hermetic.processor.process_league_season("99999", hermetic.season)
        assert report.matches_processed == 0
        assert report.standings_written == 0

    def test_ingest_and_process_twice_is_idempotent(self, hermetic: HermeticEnv):
        # Two ingestions write two S3 files; processor reads both
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        report = hermetic.processor.process_league_season(hermetic.epl_league_id, hermetic.season)
        # Standings are upserted — 20 unique teams regardless of double-processing
        items = hermetic.dynamo.items_with_sk_suffix("STANDINGS")
        assert len(items) == 20
