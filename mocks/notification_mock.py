"""
MockNotificationClient — captures SNS publish calls for test assertion.

Replaces the boto3 SNS client in tests.  Stores every published message in an
in-memory list so tests can assert on message content, count, and ordering
without an actual SNS topic or network connection.

Usage
-----
    mock_sns = MockNotificationClient()
    notifier  = Notifier(sns_client=mock_sns, topic_arn="arn:aws:sns:us-east-1:000000000000:test")

    notifier.publish_goal(...)

    assert mock_sns.count == 1
    assert mock_sns.latest["event_type"] == "GoalScored"
    assert mock_sns.messages_of_type("GoalScored")[0]["player"] == "Erling Haaland"
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CapturedMessage:
    message_id: str
    topic_arn: str
    subject: str
    body: dict[str, Any]
    attributes: dict[str, str]
    raw_message: str


class MockNotificationClient:
    """
    Hermetic SNS client mock.  Thread-safe for parallel test execution.
    """

    def __init__(self) -> None:
        self._messages: list[CapturedMessage] = []

    # ------------------------------------------------------------------
    # SNS client interface (boto3-compatible)
    # ------------------------------------------------------------------

    def publish(
        self,
        TopicArn: str,
        Message: str,
        Subject: str = "",
        MessageAttributes: dict | None = None,
    ) -> dict:
        msg_id = str(uuid.uuid4())
        try:
            body = json.loads(Message)
        except json.JSONDecodeError:
            body = {"raw": Message}

        attrs: dict[str, str] = {}
        for k, v in (MessageAttributes or {}).items():
            attrs[k] = v.get("StringValue", "")

        captured = CapturedMessage(
            message_id=msg_id,
            topic_arn=TopicArn,
            subject=Subject,
            body=body,
            attributes=attrs,
            raw_message=Message,
        )
        self._messages.append(captured)
        return {"MessageId": msg_id}

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        return len(self._messages)

    @property
    def messages(self) -> list[CapturedMessage]:
        return list(self._messages)

    @property
    def latest(self) -> dict[str, Any] | None:
        return self._messages[-1].body if self._messages else None

    def messages_of_type(self, event_type: str) -> list[dict[str, Any]]:
        return [
            m.body for m in self._messages
            if m.body.get("event_type") == event_type
        ]

    def subjects(self) -> list[str]:
        return [m.subject for m in self._messages]

    def assert_published(self, event_type: str, count: int | None = None) -> None:
        matching = self.messages_of_type(event_type)
        if not matching:
            raise AssertionError(
                f"Expected at least one '{event_type}' notification, got none.\n"
                f"Published types: {[m.body.get('event_type') for m in self._messages]}"
            )
        if count is not None and len(matching) != count:
            raise AssertionError(
                f"Expected {count} '{event_type}' notifications, got {len(matching)}."
            )

    def assert_not_published(self, event_type: str) -> None:
        matching = self.messages_of_type(event_type)
        if matching:
            raise AssertionError(
                f"Expected no '{event_type}' notifications, but got {len(matching)}."
            )

    def reset(self) -> None:
        self._messages.clear()

    def __repr__(self) -> str:
        types = [m.body.get("event_type", "?") for m in self._messages]
        return f"MockNotificationClient(count={self.count}, types={types})"
