from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import requests as _requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text

app = FastAPI(title="DataFabrik API", version="0.1.0")

DATABASE_URL  = os.environ["DATABASE_URL"]
AIRFLOW_URL   = os.environ.get("AIRFLOW_URL",      "http://airflow-webserver:8080")
AIRFLOW_USER  = os.environ.get("AIRFLOW_USER",     "admin")
AIRFLOW_PASS  = os.environ.get("AIRFLOW_PASSWORD", "admin")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# ── data helpers ──────────────────────────────────────────────────────────────

def _airflow(path: str) -> dict:
    resp = _requests.get(
        f"{AIRFLOW_URL}/api/v1{path}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def _duration(start: str | None, end: str | None) -> str:
    if not start or not end or start == "null" or end == "null":
        return "—"
    s = datetime.fromisoformat(start[:19])
    e = datetime.fromisoformat(end[:19])
    secs = int((e - s).total_seconds())
    return f"{secs}s"


def _fmt_date(dt: str | None) -> str:
    if not dt:
        return "—"
    return dt[:16].replace("T", " ")


def _success_rate(runs_30d: list[dict]) -> tuple[int, int, float | None]:
    """Return (total, successful, pct) for a list of run dicts."""
    finished = [r for r in runs_30d if r.get("state") in ("success", "failed")]
    total = len(finished)
    successful = sum(1 for r in finished if r.get("state") == "success")
    pct = round(successful / total * 100, 1) if total > 0 else None
    return total, successful, pct


def get_pipeline_data() -> list[dict]:
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

    dags_resp = _airflow("/dags?limit=100")
    dags = {d["dag_id"]: d for d in dags_resp.get("dags", [])}

    latest_runs: dict[str, dict] = {}
    monthly_runs: dict[str, list] = {}
    for dag_id in dags:
        latest = _airflow(f"/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date")
        latest_runs[dag_id] = latest.get("dag_runs", [{}])[0] if latest.get("dag_runs") else {}

        monthly = _airflow(
            f"/dags/{dag_id}/dagRuns"
            f"?limit=500&order_by=-start_date&start_date_gte={thirty_days_ago}"
        )
        monthly_runs[dag_id] = monthly.get("dag_runs", [])

    with engine.connect() as conn:
        il_rows = conn.execute(text("""
            SELECT pipeline_id, rows_extracted,
                   ROUND(duration_seconds::numeric, 1),
                   status,
                   COALESCE(watermark_to::text, '')
            FROM pipeline_metadata.ingestion_log
            WHERE (pipeline_id, extracted_at) IN (
                SELECT pipeline_id, MAX(extracted_at)
                FROM pipeline_metadata.ingestion_log
                GROUP BY pipeline_id
            )
        """)).fetchall()
    ingestion = {r[0]: r for r in il_rows}

    pipelines = []
    for dag_id, dag in dags.items():
        run = latest_runs.get(dag_id) or {}
        il  = ingestion.get(dag_id)
        state = "paused" if dag.get("is_paused") else (run.get("state") or "no runs")
        dur = f"{il[2]}s" if il and il[2] else _duration(run.get("start_date"), run.get("end_date"))
        total_30d, ok_30d, pct = _success_rate(monthly_runs.get(dag_id, []))
        pipelines.append({
            "id":        dag_id,
            "state":     state,
            "last_run":  _fmt_date(run.get("start_date")),
            "duration":  dur,
            "rows":      str(il[1]) if il and il[1] is not None else "—",
            "watermark": il[4] or "—" if il else "—",
            "runs_30d":  total_30d,
            "ok_30d":    ok_30d,
            "pct_30d":   pct,
        })

    pipelines.sort(key=lambda p: p["id"])
    return pipelines


# ── JSON API ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/")
def root() -> dict:
    return {"service": "datafabrik-api", "version": "0.1.0"}


@app.get("/api/pipelines")
def api_pipelines() -> list[dict]:
    return get_pipeline_data()


# ── HTML dashboard ─────────────────────────────────────────────────────────────

_STATE_BADGE = {
    "success":  ('<span class="badge ok">✓ success</span>',   "row-ok"),
    "failed":   ('<span class="badge fail">✗ failed</span>',  "row-fail"),
    "running":  ('<span class="badge run">↻ running</span>',  "row-run"),
    "queued":   ('<span class="badge run">⋯ queued</span>',   "row-run"),
    "paused":   ('<span class="badge pause">⏸ paused</span>', "row-pause"),
    "no runs":  ('<span class="badge none">— no runs</span>', ""),
}

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f1117; color: #e2e8f0; min-height: 100vh; }
header { background: #1a1f2e; border-bottom: 1px solid #2d3748;
         padding: 18px 32px; display: flex; align-items: center; gap: 14px; }
header h1 { font-size: 1.2rem; font-weight: 700; letter-spacing: .5px; }
header .sub { font-size: .8rem; color: #718096; margin-left: auto; }
.content { padding: 28px 32px; }
.summary { display: flex; gap: 16px; margin-bottom: 28px; }
.stat { background: #1a1f2e; border: 1px solid #2d3748; border-radius: 10px;
        padding: 16px 24px; min-width: 110px; }
.stat .num { font-size: 2rem; font-weight: 700; line-height: 1; }
.stat .lbl { font-size: .75rem; color: #718096; margin-top: 4px; text-transform: uppercase; }
.num.ok   { color: #48bb78; }
.num.fail { color: #fc8181; }
.num.pause{ color: #ecc94b; }
.num.total{ color: #63b3ed; }
table { width: 100%; border-collapse: collapse; background: #1a1f2e;
        border: 1px solid #2d3748; border-radius: 10px; overflow: hidden; }
thead th { text-align: left; padding: 12px 16px; font-size: .72rem;
           text-transform: uppercase; letter-spacing: .6px; color: #718096;
           border-bottom: 1px solid #2d3748; }
tbody tr { border-bottom: 1px solid #1e2535; transition: background .15s; }
tbody tr:last-child { border-bottom: none; }
tbody tr:hover { background: #1e2535; }
tbody td { padding: 13px 16px; font-size: .88rem; }
.badge { display: inline-block; padding: 3px 10px; border-radius: 20px;
         font-size: .75rem; font-weight: 600; }
.badge.ok    { background: #1a3a2a; color: #48bb78; }
.badge.fail  { background: #3a1a1a; color: #fc8181; }
.badge.run   { background: #1a2a3a; color: #63b3ed; }
.badge.pause { background: #3a3a1a; color: #ecc94b; }
.badge.none  { background: #2d3748; color: #718096; }
.row-fail td:first-child { border-left: 3px solid #fc8181; }
.row-ok   td:first-child { border-left: 3px solid #48bb78; }
.row-run  td:first-child { border-left: 3px solid #63b3ed; }
.row-pause td:first-child{ border-left: 3px solid #ecc94b; }
a.af-link { color: #63b3ed; text-decoration: none; font-size: .8rem; }
a.af-link:hover { text-decoration: underline; }
.refresh { font-size: .75rem; color: #4a5568; margin-top: 16px; text-align: right; }
.bar-wrap { background: #2d3748; border-radius: 4px; height: 6px; width: 100px; margin-top: 5px; }
.bar-fill { height: 6px; border-radius: 4px; }
.bar-hi  { background: #48bb78; }
.bar-mid { background: #ecc94b; }
.bar-lo  { background: #fc8181; }
.pct-lbl { font-size: .78rem; font-weight: 600; }
.pct-hi  { color: #48bb78; }
.pct-mid { color: #ecc94b; }
.pct-lo  { color: #fc8181; }
"""

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    try:
        pipelines = get_pipeline_data()
        error = None
    except Exception as exc:
        pipelines = []
        error = str(exc)

    ok     = sum(1 for p in pipelines if p["state"] == "success")
    failed = sum(1 for p in pipelines if p["state"] == "failed")
    paused = sum(1 for p in pipelines if p["state"] == "paused")
    total  = len(pipelines)

    rows_html = ""
    for p in pipelines:
        badge, row_cls = _STATE_BADGE.get(p["state"], (f'<span class="badge none">{p["state"]}</span>', ""))
        af_url = f"http://localhost:8080/dags/{p['id']}/grid"

        pct = p["pct_30d"]
        if pct is None:
            pct_cell = '<span style="color:#4a5568;font-size:.8rem">no data</span>'
        else:
            tier = "hi" if pct >= 90 else ("mid" if pct >= 70 else "lo")
            pct_cell = (
                f'<span class="pct-lbl pct-{tier}">{pct}%</span>'
                f'<div class="bar-wrap"><div class="bar-fill bar-{tier}" style="width:{pct}%"></div></div>'
                f'<span style="font-size:.72rem;color:#718096">{p["ok_30d"]}/{p["runs_30d"]} runs</span>'
            )

        rows_html += f"""
        <tr class="{row_cls}">
          <td><strong>{p['id']}</strong><br>
              <a class="af-link" href="{af_url}" target="_blank">open in Airflow ↗</a></td>
          <td>{badge}</td>
          <td>{p['last_run']}</td>
          <td>{p['duration']}</td>
          <td>{pct_cell}</td>
          <td>{p['rows']}</td>
          <td style="font-size:.8rem;color:#718096">{p['watermark']}</td>
        </tr>"""

    error_banner = f'<div style="background:#3a1a1a;color:#fc8181;padding:12px 16px;border-radius:8px;margin-bottom:20px">⚠ {error}</div>' if error else ""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>DataFabrik — Pipeline Health</title>
  <style>{_CSS}</style>
</head>
<body>
<header>
  <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
    <rect width="28" height="28" rx="6" fill="#3182ce"/>
    <path d="M7 14h14M14 7v14" stroke="white" stroke-width="2.5" stroke-linecap="round"/>
  </svg>
  <h1>DataFabrik &mdash; Pipeline Health</h1>
  <span class="sub">auto-refreshes every 30s &nbsp;|&nbsp; {now}</span>
</header>
<div class="content">
  {error_banner}
  <div class="summary">
    <div class="stat"><div class="num total">{total}</div><div class="lbl">Total</div></div>
    <div class="stat"><div class="num ok">{ok}</div><div class="lbl">Passing</div></div>
    <div class="stat"><div class="num fail">{failed}</div><div class="lbl">Failed</div></div>
    <div class="stat"><div class="num pause">{paused}</div><div class="lbl">Paused</div></div>
  </div>
  <table>
    <thead>
      <tr>
        <th>Pipeline</th>
        <th>State</th>
        <th>Last Run (UTC)</th>
        <th>Duration</th>
        <th>30-day Success Rate</th>
        <th>Rows</th>
        <th>Watermark</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <p class="refresh">Page refreshes automatically every 30 seconds.</p>
</div>
</body>
</html>"""
