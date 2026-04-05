"""
pytest plugin that collects per-test metrics (pass/fail, duration, flakiness).
Writes results to metrics/results.json after the session.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import pytest


@dataclass
class TestResult:
    name: str
    node_id: str
    outcome: str           # passed / failed / skipped / error
    duration_s: float
    markers: list[str]
    error_message: Optional[str] = None
    run_count: int = 1     # incremented when same test is re-run (flakiness tracking)
    passed_count: int = 0
    failed_count: int = 0


@dataclass
class SessionMetrics:
    run_id: str
    started_at: float
    ended_at: float = 0.0
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errored: int = 0
    results: list[TestResult] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.passed / self.total

    @property
    def duration_s(self) -> float:
        return self.ended_at - self.started_at

    def to_prometheus(self) -> str:
        lines = [
            "# HELP test_pass_rate Fraction of tests that passed (0-1)",
            "# TYPE test_pass_rate gauge",
            f"test_pass_rate {self.pass_rate:.4f}",
            "",
            "# HELP test_total Total number of tests run",
            "# TYPE test_total counter",
            f"test_total {self.total}",
            "",
            "# HELP test_passed_total Tests that passed",
            "# TYPE test_passed_total counter",
            f"test_passed_total {self.passed}",
            "",
            "# HELP test_failed_total Tests that failed",
            "# TYPE test_failed_total counter",
            f"test_failed_total {self.failed}",
            "",
            "# HELP test_skipped_total Tests that were skipped",
            "# TYPE test_skipped_total counter",
            f"test_skipped_total {self.skipped}",
            "",
            "# HELP test_session_duration_seconds Total test session duration",
            "# TYPE test_session_duration_seconds gauge",
            f"test_session_duration_seconds {self.duration_s:.3f}",
            "",
        ]

        # Per-test duration metrics
        lines.append("# HELP test_duration_seconds Duration of each test")
        lines.append("# TYPE test_duration_seconds gauge")
        for r in self.results:
            safe_name = r.name.replace('"', '\\"').replace("\n", "")
            labels = f'test="{safe_name}",outcome="{r.outcome}"'
            lines.append(f"test_duration_seconds{{{labels}}} {r.duration_s:.4f}")

        lines.append("")
        return "\n".join(lines)


class MetricsCollector:
    """
    pytest plugin — hook into test lifecycle to record metrics.
    Register via: pytest_plugins = ['metrics.collector']
    or conftest: config.pluginmanager.register(MetricsCollector(), "metrics")
    """

    OUTPUT_FILE = Path(__file__).parent / "results.json"
    PROM_FILE = Path(__file__).parent / "metrics.prom"

    def __init__(self) -> None:
        import uuid
        self.session = SessionMetrics(
            run_id=str(uuid.uuid4())[:8],
            started_at=time.time(),
        )
        self._start_times: dict[str, float] = {}

        # Flakiness tracking: node_id → list of outcomes across reruns
        self._flakiness: dict[str, list[str]] = {}

    # -- pytest hooks --

    def pytest_runtest_logreport(self, report):
        if report.when != "call":
            return

        node_id = report.nodeid
        duration = getattr(report, "duration", 0.0)

        if report.passed:
            outcome = "passed"
            self.session.passed += 1
        elif report.failed:
            outcome = "failed"
            self.session.failed += 1
        elif report.skipped:
            outcome = "skipped"
            self.session.skipped += 1
        else:
            outcome = "error"
            self.session.errored += 1

        self.session.total += 1

        error_msg = None
        if report.failed:
            error_msg = str(report.longrepr)[:500] if report.longrepr else None

        # Collect markers
        markers: list[str] = []
        if hasattr(report, "keywords"):
            markers = [k for k in report.keywords if not k.startswith("_")]

        result = TestResult(
            name=report.nodeid.split("::")[-1],
            node_id=node_id,
            outcome=outcome,
            duration_s=round(duration, 4),
            markers=markers,
            error_message=error_msg,
        )
        self.session.results.append(result)

        # Track for flakiness
        self._flakiness.setdefault(node_id, []).append(outcome)

    def pytest_sessionfinish(self, session, exitstatus):
        self.session.ended_at = time.time()
        self._write_json()
        self._write_prometheus()

    def _write_json(self) -> None:
        data = {
            "run_id": self.session.run_id,
            "started_at": self.session.started_at,
            "ended_at": self.session.ended_at,
            "duration_s": round(self.session.duration_s, 3),
            "pass_rate": round(self.session.pass_rate, 4),
            "total": self.session.total,
            "passed": self.session.passed,
            "failed": self.session.failed,
            "skipped": self.session.skipped,
            "errored": self.session.errored,
            "flaky_tests": self._compute_flaky(),
            "results": [asdict(r) for r in self.session.results],
        }
        self.OUTPUT_FILE.write_text(json.dumps(data, indent=2))

    def _write_prometheus(self) -> None:
        self.PROM_FILE.write_text(self.session.to_prometheus())

    def _compute_flaky(self) -> list[dict]:
        flaky = []
        for node_id, outcomes in self._flakiness.items():
            if len(set(outcomes)) > 1:  # mixed pass/fail = flaky
                flaky.append({
                    "node_id": node_id,
                    "outcomes": outcomes,
                    "flake_rate": outcomes.count("failed") / len(outcomes),
                })
        return flaky
