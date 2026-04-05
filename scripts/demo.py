"""
Interactive demo — shows the full pipeline end-to-end without running pytest.
Run: python scripts/demo.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mocks.notification_mock import MockNotificationClient
from mocks.sports_api_mock import EPL_LEAGUE_ID, NBA_LEAGUE_ID, CURRENT_SEASON, SportsAPISimulator
from mocks.storage_mock import MockDynamoClient, MockS3Client
from src.ingestion.sports_ingester import SportsIngester
from src.notifications.notifier import Notifier
from src.processing.stats_processor import StatsProcessor


def separator(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print('─' * 60)


def run_epl_demo():
    separator("1 · Initialising mock infrastructure")
    s3     = MockS3Client()
    dynamo = MockDynamoClient()
    sns    = MockNotificationClient()
    api    = SportsAPISimulator()
    print("  ✓  Mock S3, DynamoDB, SNS, Sports API — all ready")

    separator("2 · Ingesting Premier League results (GW 1-28)")
    ingester = SportsIngester(http_session=api, s3_client=s3, bucket="demo-bucket")
    report   = ingester.ingest_league_results(EPL_LEAGUE_ID, CURRENT_SEASON)
    print(f"  ✓  {report.results_fetched} results fetched")
    print(f"  ✓  {report.bytes_written:,} bytes written to S3")
    print(f"  ✓  S3 key: {s3.all_keys()[0]}")

    separator("3 · Ingesting upcoming fixtures (GW 29-38)")
    fix_report = ingester.ingest_league_fixtures(EPL_LEAGUE_ID, CURRENT_SEASON)
    print(f"  ✓  {fix_report.fixtures_fetched} upcoming fixtures staged")

    separator("4 · Processing stats → DynamoDB standings")
    processor = StatsProcessor(s3_client=s3, dynamo_client=dynamo, bucket="demo-bucket", table="demo-table")
    proc      = processor.process_league_season(EPL_LEAGUE_ID, CURRENT_SEASON)
    print(f"  ✓  {proc.matches_processed} matches processed")
    print(f"  ✓  {proc.standings_written} team standings written")

    separator("5 · Premier League Top 5")
    standings = sorted(
        dynamo.items_with_sk_suffix("STANDINGS"),
        key=lambda i: (-int(i["points"]["N"]), -int(i["goal_difference"]["N"]))
    )
    print(f"  {'#':<3} {'Team':<25} {'P':>3} {'W':>3} {'D':>3} {'L':>3} {'GF':>4} {'GA':>4} {'GD':>4} {'Pts':>4}")
    print(f"  {'─'*75}")
    for pos, item in enumerate(standings[:5], 1):
        team = item["pk"]["S"].replace("TEAM#", "")
        print(
            f"  {pos:<3} {team:<25}"
            f" {item['played']['N']:>3} {item['won']['N']:>3} {item['drawn']['N']:>3} {item['lost']['N']:>3}"
            f" {item['goals_for']['N']:>4} {item['goals_against']['N']:>4}"
            f" {item['goal_difference']['N']:>4} {item['points']['N']:>4}"
        )

    separator("6 · Live match simulation — Man City vs Arsenal")
    notifier = Notifier(sns_client=sns, topic_arn="arn:aws:sns:us-east-1:000000000000:demo-alerts")

    events = [
        lambda: notifier.publish_match_start("live-001", "EPL", "Man City", "Arsenal"),
        lambda: notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Erling Haaland", 15, 1, 0),
        lambda: notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Bukayo Saka", 33, 1, 1),
        lambda: notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Phil Foden", 55, 2, 1),
        lambda: notifier.publish_red_card("live-001", "EPL", "Man City", "Arsenal", "Gabriel", 72),
        lambda: notifier.publish_goal("live-001", "EPL", "Man City", "Arsenal", "Erling Haaland", 89, 3, 1),
        lambda: notifier.publish_match_end("live-001", "EPL", "Man City", "Arsenal", 3, 1),
    ]

    for fn in events:
        result = fn()
        msg = sns.messages[-1]
        minute = f" {msg.body.get('minute')}'  " if msg.body.get('minute') else "      "
        detail = msg.body.get('detail', msg.body.get('event_type', ''))
        print(f"  [{msg.body['event_type']:<20}]{minute}{detail}")

    separator("7 · Notification summary")
    print(f"  Total events published : {sns.count}")
    for etype in ("MatchStarted", "GoalScored", "RedCard", "MatchEnded"):
        count = len(sns.messages_of_type(etype))
        print(f"  {etype:<20} : {count}")

    separator("8 · League summary from DynamoDB")
    summary = dynamo.get_item_plain(f"LEAGUE#{EPL_LEAGUE_ID}", f"SEASON#{CURRENT_SEASON}#SUMMARY")
    if summary:
        print(f"  Total matches     : {summary['total_matches']}")
        print(f"  Total goals       : {summary['total_goals']}")
        print(f"  Avg goals / match : {summary['avg_goals_per_match']}")
        print(f"  Top points team   : {summary['top_scorer_team']}")

    print("\n  Run 'make test' to execute the full hermetic test suite.\n")


if __name__ == "__main__":
    run_epl_demo()
