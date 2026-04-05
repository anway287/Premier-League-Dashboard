"""
Central configuration.  Values are read from environment variables so that
conftest.py can inject per-run resource names without modifying any files.
"""
from __future__ import annotations

import os


class Config:
    # AWS / LocalStack
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    localstack_endpoint: str = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")

    # Resource names — injected by conftest.py per-run via env vars
    raw_data_bucket: str = os.getenv("RAW_DATA_BUCKET", "default-sports-raw")
    stats_table: str = os.getenv("STATS_TABLE", "default-sports-stats")
    events_queue_url: str = os.getenv("EVENTS_QUEUE_URL", "")
    alerts_topic_arn: str = os.getenv("ALERTS_TOPIC_ARN", "")

    # Sports API
    sports_api_base_url: str = os.getenv(
        "SPORTS_API_BASE_URL",
        "https://www.thesportsdb.com/api/v1/json/3",
    )
    sports_api_timeout: int = int(os.getenv("SPORTS_API_TIMEOUT", "10"))

    # Pipeline behaviour
    ingestion_batch_size: int = int(os.getenv("INGESTION_BATCH_SIZE", "50"))
    processing_workers: int = int(os.getenv("PROCESSING_WORKERS", "4"))


cfg = Config()
