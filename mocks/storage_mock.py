"""
MockStorageClient — in-memory implementations of S3 and DynamoDB clients.

These mocks are structurally compatible with boto3 so the real application
code (DataStore, SportsIngester, StatsProcessor) runs unchanged in tests.

Design goals
------------
- Deterministic: no randomness, same input → same output
- Isolated: each test gets a fresh instance via conftest fixtures
- Observable: expose helpers to inspect stored data without going through
  the application layer
- Realistic: raise the same exceptions boto3 raises for missing keys/items

Usage
-----
    mock_s3     = MockS3Client()
    mock_dynamo = MockDynamoClient()

    # Inject into your application classes:
    store = DataStore(s3_client=mock_s3, dynamo_client=mock_dynamo)
"""
from __future__ import annotations

import copy
import json
from io import BytesIO
from typing import Any


# ---------------------------------------------------------------------------
# Mock S3
# ---------------------------------------------------------------------------

class _MockS3Body:
    """Mimics the streaming body returned by boto3 get_object."""

    def __init__(self, data: bytes) -> None:
        self._buf = BytesIO(data)

    def read(self) -> bytes:
        return self._buf.read()

    def __iter__(self):
        yield from self._buf


class MockS3Client:
    """
    In-memory S3 bucket.  All buckets share the same namespace here since
    hermetic tests use a single bucket per run.
    """

    def __init__(self) -> None:
        # key → (bytes, content_type)
        self._objects: dict[str, tuple[bytes, str]] = {}
        self._put_calls: list[dict] = []
        self._get_calls: list[dict] = []

    # ------------------------------------------------------------------
    # boto3-compatible interface
    # ------------------------------------------------------------------

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str = "application/octet-stream") -> dict:
        self._objects[Key] = (Body, ContentType)
        self._put_calls.append({"Bucket": Bucket, "Key": Key, "size": len(Body)})
        return {"ETag": f'"{hash(Body)}"'}

    def get_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self._objects:
            raise self._no_such_key(Key)
        data, content_type = self._objects[Key]
        return {
            "Body": _MockS3Body(data),
            "ContentType": content_type,
            "ContentLength": len(data),
        }

    def delete_object(self, Bucket: str, Key: str) -> dict:
        self._objects.pop(Key, None)
        return {}

    def list_objects_v2(self, Bucket: str, Prefix: str = "") -> dict:
        contents = [
            {"Key": k, "Size": len(v[0])}
            for k, v in self._objects.items()
            if k.startswith(Prefix)
        ]
        return {
            "Contents": contents,
            "KeyCount": len(contents),
            "IsTruncated": False,
        }

    def create_bucket(self, Bucket: str, **kwargs) -> dict:
        # No-op — in-memory store doesn't need bucket creation
        return {}

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------

    def get_json(self, key: str) -> Any:
        data, _ = self._objects[key]
        return json.loads(data)

    def all_keys(self) -> list[str]:
        return list(self._objects.keys())

    def keys_with_prefix(self, prefix: str) -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    def total_bytes_stored(self) -> int:
        return sum(len(v[0]) for v in self._objects.values())

    @property
    def put_count(self) -> int:
        return len(self._put_calls)

    @property
    def get_count(self) -> int:
        return len(self._get_calls)

    def reset(self) -> None:
        self._objects.clear()
        self._put_calls.clear()
        self._get_calls.clear()

    @staticmethod
    def _no_such_key(key: str):
        from botocore.exceptions import ClientError
        return ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": f"The specified key does not exist: {key}"}},
            "GetObject",
        )


# ---------------------------------------------------------------------------
# Mock DynamoDB
# ---------------------------------------------------------------------------

class MockDynamoClient:
    """
    In-memory DynamoDB table.

    Items are stored with a composite key (pk + sk), matching the real table
    schema.  Supports put_item, get_item, delete_item, and a simplified
    query that scans and filters in-memory (sufficient for test volumes).
    """

    def __init__(self) -> None:
        # (pk_value, sk_value) → item dict
        self._items: dict[tuple[str, str], dict] = {}
        self._write_count: int = 0

    # ------------------------------------------------------------------
    # boto3-compatible interface
    # ------------------------------------------------------------------

    def put_item(self, TableName: str, Item: dict, **kwargs) -> dict:
        pk = Item["pk"]["S"]
        sk = Item["sk"]["S"]
        self._items[(pk, sk)] = copy.deepcopy(Item)
        self._write_count += 1
        return {}

    def get_item(self, TableName: str, Key: dict, **kwargs) -> dict:
        pk = Key["pk"]["S"]
        sk = Key["sk"]["S"]
        item = self._items.get((pk, sk))
        return {"Item": copy.deepcopy(item)} if item else {}

    def delete_item(self, TableName: str, Key: dict, **kwargs) -> dict:
        pk = Key["pk"]["S"]
        sk = Key["sk"]["S"]
        self._items.pop((pk, sk), None)
        return {}

    def batch_write_item(self, RequestItems: dict) -> dict:
        for table_name, requests in RequestItems.items():
            for req in requests:
                if "PutRequest" in req:
                    item = req["PutRequest"]["Item"]
                    pk = item["pk"]["S"]
                    sk = item["sk"]["S"]
                    self._items[(pk, sk)] = copy.deepcopy(item)
                    self._write_count += 1
                elif "DeleteRequest" in req:
                    key = req["DeleteRequest"]["Key"]
                    pk = key["pk"]["S"]
                    sk = key["sk"]["S"]
                    self._items.pop((pk, sk), None)
        return {"UnprocessedItems": {}}

    def query(self, TableName: str, **kwargs) -> dict:
        """
        Minimal query implementation — supports KeyConditionExpression for sk
        equality (used by get_standings and GSI queries in tests).
        """
        key_condition = kwargs.get("KeyConditionExpression")
        index = kwargs.get("IndexName", "")

        results = []
        for (pk, sk), item in self._items.items():
            if key_condition is not None:
                # Evaluate the boto3 ConditionBase expression
                # We support simple equality checks against pk and sk
                if not self._matches_condition(key_condition, pk, sk):
                    continue
            results.append(copy.deepcopy(item))

        return {"Items": results, "Count": len(results), "ScannedCount": len(self._items)}

    def scan(self, TableName: str, **kwargs) -> dict:
        items = [copy.deepcopy(v) for v in self._items.values()]
        return {"Items": items, "Count": len(items), "ScannedCount": len(items)}

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------

    def get_item_plain(self, pk: str, sk: str) -> dict | None:
        """Return item as raw dict with DynamoDB type descriptors stripped."""
        raw = self._items.get((pk, sk))
        if raw is None:
            return None
        return {k: list(v.values())[0] for k, v in raw.items()}

    def all_items(self) -> list[dict]:
        return [copy.deepcopy(v) for v in self._items.values()]

    def items_with_pk_prefix(self, prefix: str) -> list[dict]:
        return [
            copy.deepcopy(v) for (pk, _), v in self._items.items()
            if pk.startswith(prefix)
        ]

    def items_with_sk_suffix(self, suffix: str) -> list[dict]:
        return [
            copy.deepcopy(v) for (_, sk), v in self._items.items()
            if sk.endswith(suffix)
        ]

    @property
    def item_count(self) -> int:
        return len(self._items)

    @property
    def write_count(self) -> int:
        return self._write_count

    def reset(self) -> None:
        self._items.clear()
        self._write_count = 0

    @staticmethod
    def _matches_condition(condition, pk: str, sk: str) -> bool:
        """
        Very small subset of boto3 ConditionBase evaluation for tests.
        Handles Key("pk").eq(val) and Key("sk").eq(val) used in the app.
        """
        try:
            expr = condition.get_expression()
            fmt = expr["format"]
            names = {v: k for k, v in expr.get("names", {}).items()}
            values = {k: list(v.values())[0] for k, v in expr.get("values", {}).items()}

            if " = " in fmt:
                parts = fmt.split(" = ")
                attr_placeholder = parts[0].strip()
                val_placeholder = parts[1].strip()
                attr = names.get(attr_placeholder, attr_placeholder).lstrip("#")
                val = values.get(val_placeholder, val_placeholder)
                if attr == "pk":
                    return pk == val
                elif attr == "sk":
                    return sk == val
            return True  # can't evaluate, pass through
        except Exception:
            return True  # defensive — let tests handle assertion failures
