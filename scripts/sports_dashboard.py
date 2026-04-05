"""
Visual sports dashboard — served at http://localhost:8080
Shows EPL standings, match results, top scorers, and live match feed.
Run: python3 scripts/sports_dashboard.py
"""
from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mocks.sports_api_mock import SportsAPISimulator, EPL_LEAGUE_ID, CURRENT_SEASON
from mocks.storage_mock import MockS3Client, MockDynamoClient
from mocks.notification_mock import MockNotificationClient
from src.ingestion.sports_ingester import SportsIngester
from src.processing.stats_processor import StatsProcessor
from src.notifications.notifier import Notifier


def build_data():
    s3     = MockS3Client()
    dynamo = MockDynamoClient()
    sns    = MockNotificationClient()
    api    = SportsAPISimulator()

    ingester  = SportsIngester(http_session=api, s3_client=s3, bucket="dash")
    processor = StatsProcessor(s3_client=s3, dynamo_client=dynamo, bucket="dash", table="stats")
    notifier  = Notifier(sns_client=sns, topic_arn="arn:aws:sns:us-east-1:000:alerts")

    ingester.ingest_league_results(EPL_LEAGUE_ID, CURRENT_SEASON)
    processor.process_league_season(EPL_LEAGUE_ID, CURRENT_SEASON)

    # Standings
    standings = sorted(
        dynamo.items_with_sk_suffix("STANDINGS"),
        key=lambda i: (-int(i["points"]["N"]), -int(i["goal_difference"]["N"]))
    )

    # Recent results (last 10)
    results_raw = api.get_results_for_league(EPL_LEAGUE_ID)
    recent = sorted(results_raw, key=lambda e: e.get("dateEvent", ""), reverse=True)[:10]

    # Top scorers (teams by goals_for)
    top_scorers = sorted(standings, key=lambda i: -int(i["goals_for"]["N"]))[:5]

    # League summary
    summary = dynamo.get_item_plain(f"LEAGUE#{EPL_LEAGUE_ID}", f"SEASON#{CURRENT_SEASON}#SUMMARY")

    # Live match feed
    notifier.publish_match_start("live-001", "Premier League", "Man City", "Arsenal")
    notifier.publish_goal("live-001", "Premier League", "Man City", "Arsenal", "Erling Haaland", 15, 1, 0)
    notifier.publish_goal("live-001", "Premier League", "Man City", "Arsenal", "Bukayo Saka", 33, 1, 1)
    notifier.publish_goal("live-001", "Premier League", "Man City", "Arsenal", "Phil Foden", 55, 2, 1)
    notifier.publish_red_card("live-001", "Premier League", "Man City", "Arsenal", "Gabriel", 72)
    notifier.publish_goal("live-001", "Premier League", "Man City", "Arsenal", "Erling Haaland", 89, 3, 1)
    notifier.publish_match_end("live-001", "Premier League", "Man City", "Arsenal", 3, 1)

    return standings, recent, top_scorers, summary, sns.messages


TEAM_BADGES = {
    "Manchester City":   "🔵", "Arsenal":          "🔴", "Liverpool":        "🔴",
    "Manchester United": "🔴", "Chelsea":           "🔵", "Tottenham Hotspur":"⚪",
    "Newcastle United":  "⚫", "Aston Villa":       "🟣", "Brighton":         "🔵",
    "West Ham United":   "🔵", "Fulham":            "⚪", "Wolves":           "🟡",
    "Everton":           "🔵", "Crystal Palace":    "🔴", "Brentford":        "🔴",
    "Nottingham Forest": "🔴", "Luton Town":        "🟠", "Burnley":          "🟤",
    "Sheffield United":  "🔴", "Bournemouth":       "🟥",
}

EVENT_ICONS = {
    "MatchStarted": "🟢", "MatchEnded": "🏁",
    "GoalScored":   "⚽", "RedCard":    "🟥",
    "YellowCard":   "🟨", "Substitution":"🔄",
    "VAR_REVIEW":   "📺", "PenaltyAwarded":"🎯",
}

def badge(team: str) -> str:
    return TEAM_BADGES.get(team, "⚪")

def pos_style(pos: int) -> str:
    if pos <= 4:   return "background:#1a6b3c;color:#7cfc00"
    if pos == 5:   return "background:#1a3d6b;color:#87ceeb"
    if pos >= 18:  return "background:#6b1a1a;color:#ff8080"
    return "background:#2a2a2a;color:#ccc"


def render_html(standings, recent, top_scorers, summary, feed) -> str:
    # ── Standings rows ────────────────────────────────────────────────
    rows = ""
    for pos, item in enumerate(standings, 1):
        team = item["pk"]["S"].replace("TEAM#", "")
        p, w, d, l = item["played"]["N"], item["won"]["N"], item["drawn"]["N"], item["lost"]["N"]
        gf, ga, gd = item["goals_for"]["N"], item["goals_against"]["N"], item["goal_difference"]["N"]
        pts = item["points"]["N"]
        gd_str = f"+{gd}" if int(gd) > 0 else gd
        rows += f"""
        <tr>
          <td><span class="pos-badge" style="{pos_style(pos)}">{pos}</span></td>
          <td class="team-name">{badge(team)} {team}</td>
          <td>{p}</td><td>{w}</td><td>{d}</td><td>{l}</td>
          <td>{gf}</td><td>{ga}</td>
          <td class="{'gd-pos' if int(gd)>=0 else 'gd-neg'}">{gd_str}</td>
          <td><strong>{pts}</strong></td>
        </tr>"""

    # ── Recent results ────────────────────────────────────────────────
    result_cards = ""
    for r in recent:
        hs, as_ = int(r.get("intHomeScore", 0)), int(r.get("intAwayScore", 0))
        ht, at  = r["strHomeTeam"], r["strAwayTeam"]
        date    = r.get("dateEvent", "")
        winner  = "home" if hs > as_ else ("away" if as_ > hs else "draw")
        result_cards += f"""
        <div class="match-card">
          <div class="match-date">{date}</div>
          <div class="match-teams">
            <span class="{'team-winner' if winner=='home' else ''}">{badge(ht)} {ht}</span>
            <span class="score-box">{hs} – {as_}</span>
            <span class="{'team-winner' if winner=='away' else ''}">{at} {badge(at)}</span>
          </div>
        </div>"""

    # ── Top scoring teams ─────────────────────────────────────────────
    scorer_rows = ""
    for i, item in enumerate(top_scorers, 1):
        team = item["pk"]["S"].replace("TEAM#", "")
        gf   = int(item["goals_for"]["N"])
        bar  = int(gf / 60 * 100)
        scorer_rows += f"""
        <div class="scorer-row">
          <span class="scorer-rank">#{i}</span>
          <span class="scorer-team">{badge(team)} {team}</span>
          <div class="scorer-bar-wrap">
            <div class="scorer-bar" style="width:{bar}%"></div>
          </div>
          <span class="scorer-goals">{gf} goals</span>
        </div>"""

    # ── Live feed ─────────────────────────────────────────────────────
    feed_items = ""
    for msg in feed:
        etype  = msg.body.get("event_type", "")
        icon   = EVENT_ICONS.get(etype, "📌")
        detail = msg.body.get("detail", etype)
        minute = f"<span class='feed-min'>{msg.body['minute']}'</span>" if msg.body.get("minute") else ""
        feed_items += f"<div class='feed-item'>{icon} {minute} {detail}</div>"

    avg  = summary["avg_goals_per_match"] if summary else "—"
    tot  = summary["total_goals"]         if summary else "—"
    top  = summary["top_scorer_team"]     if summary else "—"
    mtch = summary["total_matches"]       if summary else "—"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Premier League Dashboard</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0d0d0d; color: #eee; }}

    header {{
      background: linear-gradient(135deg, #3d0140, #1a0066);
      padding: 20px 32px;
      display: flex;
      align-items: center;
      gap: 16px;
      border-bottom: 3px solid #6a0dad;
    }}
    header h1 {{ font-size: 1.8rem; font-weight: 700; }}
    header .season-tag {{
      margin-left: auto;
      background: rgba(255,255,255,0.15);
      padding: 4px 12px;
      border-radius: 20px;
      font-size: 0.85rem;
    }}

    .stat-bar {{
      display: flex;
      gap: 16px;
      padding: 16px 32px;
      background: #161616;
      border-bottom: 1px solid #2a2a2a;
    }}
    .stat-card {{
      background: #1e1e1e;
      border-radius: 10px;
      padding: 12px 20px;
      flex: 1;
      text-align: center;
      border: 1px solid #2e2e2e;
    }}
    .stat-card .val {{ font-size: 1.6rem; font-weight: 700; color: #a855f7; }}
    .stat-card .lbl {{ font-size: 0.75rem; color: #888; margin-top: 2px; }}

    .main-grid {{
      display: grid;
      grid-template-columns: 1fr 380px;
      gap: 20px;
      padding: 20px 32px;
    }}

    .panel {{
      background: #161616;
      border-radius: 12px;
      border: 1px solid #2a2a2a;
      overflow: hidden;
    }}
    .panel-header {{
      background: #1e1e1e;
      padding: 12px 20px;
      font-weight: 600;
      font-size: 0.95rem;
      border-bottom: 1px solid #2a2a2a;
      display: flex;
      align-items: center;
      gap: 8px;
    }}

    table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
    th {{ padding: 8px 12px; text-align: center; color: #888; font-weight: 500;
          font-size: 0.78rem; border-bottom: 1px solid #2a2a2a; background: #1a1a1a; }}
    th:nth-child(2) {{ text-align: left; }}
    td {{ padding: 9px 12px; text-align: center; border-bottom: 1px solid #1e1e1e; }}
    td:nth-child(2) {{ text-align: left; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #1e1e1e; }}

    .team-name {{ font-weight: 500; white-space: nowrap; }}
    .pos-badge {{
      display: inline-block; width: 26px; height: 26px;
      border-radius: 6px; font-weight: 700; font-size: 0.8rem;
      line-height: 26px; text-align: center;
    }}
    .gd-pos {{ color: #4ade80; }}
    .gd-neg {{ color: #f87171; }}

    .right-col {{ display: flex; flex-direction: column; gap: 20px; }}

    /* Match cards */
    .match-card {{
      padding: 10px 16px;
      border-bottom: 1px solid #222;
    }}
    .match-card:last-child {{ border-bottom: none; }}
    .match-date {{ font-size: 0.72rem; color: #666; margin-bottom: 4px; }}
    .match-teams {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 0.85rem;
    }}
    .score-box {{
      background: #2a2a2a;
      border-radius: 6px;
      padding: 3px 10px;
      font-weight: 700;
      font-size: 1rem;
      color: #fff;
      min-width: 52px;
      text-align: center;
    }}
    .team-winner {{ color: #4ade80; font-weight: 600; }}

    /* Scorer bars */
    .scorer-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 10px 16px;
      border-bottom: 1px solid #1e1e1e;
    }}
    .scorer-row:last-child {{ border-bottom: none; }}
    .scorer-rank {{ color: #888; font-size: 0.8rem; width: 22px; }}
    .scorer-team {{ font-size: 0.85rem; min-width: 170px; }}
    .scorer-bar-wrap {{
      flex: 1; background: #222; border-radius: 4px; height: 8px; overflow: hidden;
    }}
    .scorer-bar {{ height: 100%; background: linear-gradient(90deg, #a855f7, #ec4899); border-radius: 4px; }}
    .scorer-goals {{ font-size: 0.82rem; color: #a855f7; font-weight: 600; min-width: 60px; text-align: right; }}

    /* Live feed */
    .feed-item {{
      padding: 9px 16px;
      border-bottom: 1px solid #1e1e1e;
      font-size: 0.85rem;
      line-height: 1.4;
    }}
    .feed-item:last-child {{ border-bottom: none; }}
    .feed-min {{ color: #a855f7; font-weight: 600; margin-right: 4px; }}

    .live-badge {{
      background: #dc2626;
      color: #fff;
      font-size: 0.65rem;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 4px;
      margin-left: 6px;
      animation: blink 1.5s infinite;
    }}
    @keyframes blink {{ 0%,100%{{opacity:1}} 50%{{opacity:0.4}} }}

    footer {{
      text-align: center;
      padding: 16px;
      color: #444;
      font-size: 0.78rem;
      border-top: 1px solid #1e1e1e;
    }}

    /* Zone legend */
    .legend {{
      display: flex;
      gap: 16px;
      padding: 10px 20px;
      font-size: 0.75rem;
      color: #888;
      border-top: 1px solid #2a2a2a;
      background: #111;
    }}
    .legend-dot {{
      display: inline-block;
      width: 10px; height: 10px;
      border-radius: 3px;
      margin-right: 5px;
    }}
  </style>
</head>
<body>

<header>
  <span style="font-size:2rem">⚽</span>
  <div>
    <h1>Premier League</h1>
    <div style="font-size:0.85rem;color:#bbb;margin-top:2px">Hermetic Test Framework — Sports Pipeline</div>
  </div>
  <div class="season-tag">🗓️ Season {CURRENT_SEASON}</div>
</header>

<div class="stat-bar">
  <div class="stat-card"><div class="val">{mtch}</div><div class="lbl">Matches Played</div></div>
  <div class="stat-card"><div class="val">{tot}</div><div class="lbl">Total Goals</div></div>
  <div class="stat-card"><div class="val">{avg}</div><div class="lbl">Avg Goals / Match</div></div>
  <div class="stat-card"><div class="val">🏆 {top}</div><div class="lbl">League Leaders</div></div>
  <div class="stat-card"><div class="val">20</div><div class="lbl">Teams</div></div>
</div>

<div class="main-grid">

  <!-- LEFT: Standings -->
  <div class="panel">
    <div class="panel-header">🏆 League Standings — GW 28</div>
    <table>
      <thead>
        <tr>
          <th>#</th><th>Team</th><th>P</th><th>W</th><th>D</th><th>L</th>
          <th>GF</th><th>GA</th><th>GD</th><th>Pts</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <div class="legend">
      <span><span class="legend-dot" style="background:#1a6b3c"></span> Champions League</span>
      <span><span class="legend-dot" style="background:#1a3d6b"></span> Europa League</span>
      <span><span class="legend-dot" style="background:#6b1a1a"></span> Relegation</span>
    </div>
  </div>

  <!-- RIGHT col -->
  <div class="right-col">

    <!-- Live Match -->
    <div class="panel">
      <div class="panel-header">
        🔴 Man City vs Arsenal
        <span class="live-badge">LIVE</span>
        <span style="margin-left:auto;font-weight:700;font-size:1rem">3 – 1</span>
      </div>
      <div>{feed_items}</div>
    </div>

    <!-- Top Scoring Teams -->
    <div class="panel">
      <div class="panel-header">🎯 Top Attacking Teams</div>
      {scorer_rows}
    </div>

    <!-- Recent Results -->
    <div class="panel">
      <div class="panel-header">📋 Recent Results</div>
      {result_cards}
    </div>

  </div>
</div>

<footer>
  Data generated by SportsAPISimulator — hermetic, deterministic, no live API needed
  &nbsp;|&nbsp; Refresh the page to reload
</footer>

</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):

    _html_cache: str | None = None

    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self._respond(404, b"Not found", "text/plain")
            return
        if DashboardHandler._html_cache is None:
            standings, recent, top_scorers, summary, feed = build_data()
            DashboardHandler._html_cache = render_html(standings, recent, top_scorers, summary, feed)
        body = DashboardHandler._html_cache.encode()
        self._respond(200, body, "text/html; charset=utf-8")

    def _respond(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # Basic security headers
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "default-src 'self' 'unsafe-inline'")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    port = 8080
    server = HTTPServer(("127.0.0.1", port), DashboardHandler)
    print(f"\n  ⚽  Sports Dashboard → http://localhost:{port}")
    print(f"  Opens in your browser automatically...\n")
    import webbrowser, threading
    threading.Timer(0.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Dashboard stopped.")
