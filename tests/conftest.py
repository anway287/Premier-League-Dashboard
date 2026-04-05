"""
conftest.py — Hermetic test fixtures.

Every test in this suite gets a completely isolated environment:

  • Fresh mock S3 + DynamoDB clients (no shared state between tests)
  • Fresh notification sink (captures messages per-test)
  • Fresh SportsAPISimulator instance
  • Per-run resource names derived from a unique run_id
  • Optional: Terraform-provisioned LocalStack environment (skipped when
    HERMETIC_USE_LOCALSTACK=0, the default for fast unit-style runs)

The hermetic fixture is the single source of truth — request it in any test
and you get a named tuple of all pre-wired clients + application instances.
"""
from __future__ import annotations

import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator

import pytest

from mocks.notification_mock import MockNotificationClient
from mocks.sports_api_mock import SportsAPISimulator, EPL_LEAGUE_ID, NBA_LEAGUE_ID, CURRENT_SEASON
from mocks.storage_mock import MockDynamoClient, MockS3Client
from src.ingestion.sports_ingester import SportsIngester
from src.notifications.notifier import Notifier
from src.processing.stats_processor import StatsProcessor
from src.storage.data_store import DataStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TERRAFORM_DIR = Path(__file__).parent.parent / "terraform"
USE_LOCALSTACK = os.getenv("HERMETIC_USE_LOCALSTACK", "0") == "1"


# ---------------------------------------------------------------------------
# Hermetic environment bundle
# ---------------------------------------------------------------------------

@dataclass
class HermeticEnv:
    """
    All clients and application instances for a single isolated test.

    Fields
    ------
    run_id          Unique identifier for this test run / environment
    s3              MockS3Client (or LocalStack-backed boto3 client)
    dynamo          MockDynamoClient (or LocalStack-backed boto3 client)
    sns             MockNotificationClient (or LocalStack SNS client)
    sports_api      SportsAPISimulator
    ingester        SportsIngester wired to the above clients
    processor       StatsProcessor wired to the above clients
    notifier        Notifier wired to the above clients
    store           DataStore wired to the above clients
    bucket          Name of the isolated S3 bucket for this run
    table           Name of the isolated DynamoDB table for this run
    epl_league_id   Convenience constant
    nba_league_id   Convenience constant
    season          Current test season
    """
    run_id: str
    s3: MockS3Client
    dynamo: MockDynamoClient
    sns: MockNotificationClient
    sports_api: SportsAPISimulator
    ingester: SportsIngester
    processor: StatsProcessor
    notifier: Notifier
    store: DataStore
    bucket: str
    table: str
    epl_league_id: str
    nba_league_id: str
    season: str


# ---------------------------------------------------------------------------
# Session-scoped: shared data between tests in the same run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def run_id() -> str:
    """Unique ID for the entire test session — used as Terraform prefix."""
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"ht-{ts}-{short}"


@pytest.fixture(scope="session")
def sports_api_data():
    """
    Session-scoped SportsAPISimulator — building the data set is expensive,
    so we share it.  Tests must NOT mutate this instance directly; use
    the per-test `sports_api` fixture (function-scoped reset) instead.
    """
    return SportsAPISimulator()


# ---------------------------------------------------------------------------
# Per-test: fresh isolated environment
# ---------------------------------------------------------------------------

@pytest.fixture
def sports_api(sports_api_data: SportsAPISimulator) -> Generator[SportsAPISimulator, None, None]:
    """Reset call log before each test; data is shared (immutable)."""
    sports_api_data.reset()
    yield sports_api_data


@pytest.fixture
def mock_s3() -> MockS3Client:
    return MockS3Client()


@pytest.fixture
def mock_dynamo() -> MockDynamoClient:
    return MockDynamoClient()


@pytest.fixture
def mock_sns() -> MockNotificationClient:
    return MockNotificationClient()


@pytest.fixture
def hermetic(
    run_id: str,
    mock_s3: MockS3Client,
    mock_dynamo: MockDynamoClient,
    mock_sns: MockNotificationClient,
    sports_api: SportsAPISimulator,
) -> HermeticEnv:
    """
    The primary fixture.  Wires all mocks together into a ready-to-use env.

    Example
    -------
        def test_ingestion(hermetic):
            report = hermetic.ingester.ingest_league_results(
                hermetic.epl_league_id, hermetic.season
            )
            assert report.results_fetched > 0
            assert hermetic.s3.put_count == 1
    """
    test_id = uuid.uuid4().hex[:8]
    bucket = f"test-sports-raw-{test_id}"
    table = f"test-sports-stats-{test_id}"
    topic_arn = f"arn:aws:sns:us-east-1:000000000000:test-alerts-{test_id}"

    ingester = SportsIngester(
        http_session=sports_api,
        s3_client=mock_s3,
        bucket=bucket,
    )
    processor = StatsProcessor(
        s3_client=mock_s3,
        dynamo_client=mock_dynamo,
        bucket=bucket,
        table=table,
    )
    notifier = Notifier(
        sns_client=mock_sns,
        topic_arn=topic_arn,
    )
    store = DataStore(
        s3_client=mock_s3,
        dynamo_client=mock_dynamo,
        bucket=bucket,
        table=table,
    )

    return HermeticEnv(
        run_id=f"{run_id}-{test_id}",
        s3=mock_s3,
        dynamo=mock_dynamo,
        sns=mock_sns,
        sports_api=sports_api,
        ingester=ingester,
        processor=processor,
        notifier=notifier,
        store=store,
        bucket=bucket,
        table=table,
        epl_league_id=EPL_LEAGUE_ID,
        nba_league_id=NBA_LEAGUE_ID,
        season=CURRENT_SEASON,
    )


# ---------------------------------------------------------------------------
# Terraform-backed LocalStack fixture (opt-in)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def tf_env(run_id: str):
    """
    Provisions a real LocalStack environment via Terraform.
    Skipped unless HERMETIC_USE_LOCALSTACK=1.

    Yields a dict with resource names (bucket, table, queue_url, topic_arn).
    Tears down via `terraform destroy` after the session.
    """
    if not USE_LOCALSTACK:
        pytest.skip("LocalStack integration disabled — set HERMETIC_USE_LOCALSTACK=1")

    tf_vars = [
        f"-var=run_prefix={run_id}",
        "-var=localstack_mode=true",
    ]

    # Init + Apply
    subprocess.run(
        ["terraform", "init", "-reconfigure"],
        cwd=TERRAFORM_DIR, check=True, capture_output=True,
    )
    subprocess.run(
        ["terraform", "apply", "-auto-approve"] + tf_vars,
        cwd=TERRAFORM_DIR, check=True, capture_output=True,
    )

    # Read outputs
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=TERRAFORM_DIR, check=True, capture_output=True, text=True,
    )
    import json
    outputs = json.loads(result.stdout)

    env = {
        "bucket": outputs["raw_data_bucket"]["value"],
        "table": outputs["stats_table"]["value"],
        "queue_url": outputs["events_queue_url"]["value"],
        "topic_arn": outputs["alerts_topic_arn"]["value"],
        "run_prefix": run_id,
    }

    yield env

    # Teardown
    subprocess.run(
        ["terraform", "destroy", "-auto-approve"] + tf_vars,
        cwd=TERRAFORM_DIR, check=True, capture_output=True,
    )


# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------

def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests that require LocalStack")
    config.addinivalue_line("markers", "slow: marks tests that are intentionally slow")
    config.addinivalue_line("markers", "epl: tests using Premier League data")
    config.addinivalue_line("markers", "nba: tests using NBA data")
    config.addinivalue_line("markers", "e2e: full pipeline end-to-end tests")
