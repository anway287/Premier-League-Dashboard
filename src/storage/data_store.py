"""
DataStore — thin wrapper around S3 + DynamoDB providing typed get/put helpers
used by both the ingester and the processor.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import boto3

from src.config import cfg

logger = logging.getLogger(__name__)


class DataStore:
    def __init__(
        self,
        s3_client=None,
        dynamo_client=None,
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
    # S3 helpers
    # ------------------------------------------------------------------

    def put_json(self, key: str, data: Any) -> None:
        body = json.dumps(data, default=str).encode()
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        logger.debug("S3 put s3://%s/%s (%d bytes)", self._bucket, key, len(body))

    def get_json(self, key: str) -> Any:
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        return json.loads(resp["Body"].read())

    def list_keys(self, prefix: str) -> list[str]:
        resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def delete_object(self, key: str) -> None:
        self._s3.delete_object(Bucket=self._bucket, Key=key)

    # ------------------------------------------------------------------
    # DynamoDB helpers
    # ------------------------------------------------------------------

    def put_item(self, item: dict) -> None:
        self._dynamo.put_item(TableName=self._table, Item=item)

    def get_item(self, pk: str, sk: str) -> dict | None:
        resp = self._dynamo.get_item(
            TableName=self._table,
            Key={"pk": {"S": pk}, "sk": {"S": sk}},
        )
        return resp.get("Item")

    def delete_item(self, pk: str, sk: str) -> None:
        self._dynamo.delete_item(
            TableName=self._table,
            Key={"pk": {"S": pk}, "sk": {"S": sk}},
        )
