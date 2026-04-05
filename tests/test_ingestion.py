"""
test_ingestion.py — Integration tests for SportsIngester.

Tests validate that the ingester correctly fetches data from the Sports API
simulator and lands the right JSON blobs onto S3, with no side effects
between tests (hermetic isolation via fresh mock clients each time).
"""
from __future__ import annotations

import json

import pytest

from tests.conftest import HermeticEnv


# ---------------------------------------------------------------------------
# Premier League fixture ingestion
# ---------------------------------------------------------------------------

@pytest.mark.epl
class TestEPLFixtureIngestion:

    def test_ingest_fixtures_writes_to_s3(self, hermetic: HermeticEnv):
        """Ingesting EPL fixtures should write exactly one object to S3."""
        report = hermetic.ingester.ingest_league_fixtures(
            hermetic.epl_league_id, hermetic.season
        )

        assert report.fixtures_fetched > 0, "Should fetch some upcoming fixtures"
        assert hermetic.s3.put_count == 1, "Exactly one S3 object should be written"
        assert len(report.errors) == 0

    def test_ingest_fixtures_correct_s3_key(self, hermetic: HermeticEnv):
        """S3 key should follow the fixtures/<league>/<season>/<date>.json pattern."""
        hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)

        keys = hermetic.s3.all_keys()
        assert len(keys) == 1
        assert keys[0].startswith(f"fixtures/{hermetic.epl_league_id}/{hermetic.season}/")
        assert keys[0].endswith(".json")

    def test_ingest_fixtures_valid_json_structure(self, hermetic: HermeticEnv):
        """Each S3 fixture record must have required fields."""
        hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)

        key = hermetic.s3.all_keys()[0]
        records = hermetic.s3.get_json(key)
        assert isinstance(records, list)
        assert len(records) > 0

        required_fields = {"match_id", "home_team", "away_team", "league", "kickoff_utc", "venue", "season"}
        for record in records[:5]:  # spot-check first 5
            missing = required_fields - set(record.keys())
            assert not missing, f"Fixture record missing fields: {missing}\nRecord: {record}"

    def test_ingest_fixtures_no_scores_for_upcoming(self, hermetic: HermeticEnv):
        """Fixture records must NOT contain scores — they haven't been played."""
        hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)

        key = hermetic.s3.all_keys()[0]
        records = hermetic.s3.get_json(key)
        for record in records:
            assert "home_score" not in record
            assert "away_score" not in record

    def test_ingest_fixtures_epl_league_metadata(self, hermetic: HermeticEnv):
        """All fixture records should reference the English Premier League."""
        hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)

        key = hermetic.s3.all_keys()[0]
        records = hermetic.s3.get_json(key)
        leagues = {r["league"] for r in records}
        assert "English Premier League" in leagues

    def test_ingest_fixtures_bytes_written_positive(self, hermetic: HermeticEnv):
        """IngestionReport.bytes_written must reflect actual data written."""
        report = hermetic.ingester.ingest_league_fixtures(hermetic.epl_league_id, hermetic.season)
        assert report.bytes_written > 0
        assert report.bytes_written == hermetic.s3.total_bytes_stored()


# ---------------------------------------------------------------------------
# Premier League results ingestion
# ---------------------------------------------------------------------------

@pytest.mark.epl
class TestEPLResultsIngestion:

    def test_ingest_results_reports_count(self, hermetic: HermeticEnv):
        report = hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        assert report.results_fetched > 0
        assert report.fixtures_fetched == 0   # results report uses results_fetched field
        assert len(report.errors) == 0

    def test_ingest_results_s3_key_prefix(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        keys = hermetic.s3.all_keys()
        assert any(k.startswith("results/") for k in keys)

    def test_ingest_results_contain_scores(self, hermetic: HermeticEnv):
        """Every ingested result must carry home_score and away_score."""
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        key = next(k for k in hermetic.s3.all_keys() if k.startswith("results/"))
        records = hermetic.s3.get_json(key)
        for record in records:
            assert "home_score" in record
            assert "away_score" in record
            assert isinstance(record["home_score"], int)
            assert isinstance(record["away_score"], int)
            assert record["home_score"] >= 0
            assert record["away_score"] >= 0

    def test_ingest_results_scores_are_realistic(self, hermetic: HermeticEnv):
        """Scores should be within a plausible football range (0–10)."""
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        key = next(k for k in hermetic.s3.all_keys() if k.startswith("results/"))
        records = hermetic.s3.get_json(key)
        for record in records:
            assert 0 <= record["home_score"] <= 10, f"Unrealistic home score: {record['home_score']}"
            assert 0 <= record["away_score"] <= 10, f"Unrealistic away score: {record['away_score']}"

    def test_ingest_results_isolation_between_calls(self, hermetic: HermeticEnv):
        """Calling ingest twice should NOT overwrite the first result."""
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        # Two separate S3 puts — both writes recorded
        assert hermetic.s3.put_count == 2


# ---------------------------------------------------------------------------
# NBA ingestion
# ---------------------------------------------------------------------------

@pytest.mark.nba
class TestNBAIngestion:

    def test_nba_results_ingested_successfully(self, hermetic: HermeticEnv):
        report = hermetic.ingester.ingest_league_results(hermetic.nba_league_id, hermetic.season)
        assert report.results_fetched > 0
        assert len(report.errors) == 0

    def test_nba_scores_are_basketball_range(self, hermetic: HermeticEnv):
        """NBA scores should be in the basketball range (70–160 pts)."""
        hermetic.ingester.ingest_league_results(hermetic.nba_league_id, hermetic.season)

        key = next(k for k in hermetic.s3.all_keys() if k.startswith("results/"))
        records = hermetic.s3.get_json(key)
        for record in records:
            assert record["home_score"] >= 70, f"NBA score too low: {record['home_score']}"
            assert record["home_score"] <= 160, f"NBA score too high: {record['home_score']}"


# ---------------------------------------------------------------------------
# Player stats ingestion
# ---------------------------------------------------------------------------

class TestPlayerStatsIngestion:

    def test_player_stats_land_on_s3(self, hermetic: HermeticEnv):
        report = hermetic.ingester.ingest_player_stats("133604", hermetic.season)
        assert hermetic.s3.put_count == 1
        assert report.bytes_written > 0

    def test_player_stats_s3_key_format(self, hermetic: HermeticEnv):
        hermetic.ingester.ingest_player_stats("133604", hermetic.season)
        keys = hermetic.s3.all_keys()
        assert keys[0].startswith("players/133604/")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestIngestionErrorHandling:

    def test_network_failure_captured_in_report(self, mock_s3: MockS3Client):
        """A network error should be captured in errors[], not re-raised."""
        from mocks.sports_api_mock import SportsAPISimulator
        from src.ingestion.sports_ingester import SportsIngester

        flaky_api = SportsAPISimulator(network_failure_rate=1.0)  # always fails
        ingester = SportsIngester(http_session=flaky_api, s3_client=mock_s3, bucket="test")

        report = ingester.ingest_league_fixtures(EPL_LEAGUE_ID, CURRENT_SEASON)

        assert len(report.errors) > 0
        assert report.fixtures_fetched == 0
        assert mock_s3.put_count == 0   # nothing written on failure

    def test_partial_failure_does_not_corrupt_previous_data(self, hermetic: HermeticEnv):
        """A failed second call must not affect data from the first successful call."""
        # Successful ingest
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)
        first_put_count = hermetic.s3.put_count
        first_keys = set(hermetic.s3.all_keys())

        # Now fail — swap in a failing session
        from mocks.sports_api_mock import SportsAPISimulator
        hermetic.ingester._http = SportsAPISimulator(network_failure_rate=1.0)
        hermetic.ingester.ingest_league_results(hermetic.epl_league_id, hermetic.season)

        # Original data untouched
        assert hermetic.s3.put_count == first_put_count
        assert set(hermetic.s3.all_keys()) == first_keys


# ---------------------------------------------------------------------------
# Allow importing constants used above in the module
# ---------------------------------------------------------------------------
from mocks.sports_api_mock import EPL_LEAGUE_ID, CURRENT_SEASON
