"""DataFabrik FastAPI backend — pipeline health dashboard and metadata API."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import requests as _requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text

app = FastAPI(title="DataFabrik API", version="0.1.0")

DATABASE_URL = os.environ["DATABASE_URL"]
AIRFLOW_URL  = os.environ.get("AIRFLOW_URL",      "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER",     "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASSWORD", "admin")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# ── helpers ───────────────────────────────────────────────────────────────────

def _airflow(path: str) -> dict:
    resp = _requests.get(
        f"{AIRFLOW_URL}/api/v1{path}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def _duration(start: str | None, end: str | None) -> str:
    if not start or not end or start in ("null", ""):
        return "—"
    s = datetime.fromisoformat(start[:19])
    e = datetime.fromisoformat(end[:19])
    return f"{int((e - s).total_seconds())}s"


def _fmt_date(dt: str | None) -> str:
    if not dt:
        return "—"
    return dt[:16].replace("T", " ")


def _success_rate(runs: list[dict]) -> tuple[int, int, float | None]:
    """Return (total_finished, successful, pct) for a list of run dicts."""
    finished   = [r for r in runs if r.get("state") in ("success", "failed")]
    total      = len(finished)
    successful = sum(1 for r in finished if r.get("state") == "success")
    pct        = round(successful / total * 100, 1) if total > 0 else None
    return total, successful, pct


# ── data fetchers ─────────────────────────────────────────────────────────────

def _fetch_dag_runs(dags: dict) -> tuple[dict, dict]:
    """Return (latest_run_per_dag, monthly_runs_per_dag)."""
    thirty_ago = (
        datetime.now(timezone.utc) - timedelta(days=30)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    latest: dict[str, dict] = {}
    monthly: dict[str, list] = {}
    for dag_id in dags:
        r = _airflow(f"/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date")
        latest[dag_id] = r.get("dag_runs", [{}])[0] if r.get("dag_runs") else {}

        m = _airflow(
            f"/dags/{dag_id}/dagRuns"
            f"?limit=500&order_by=-start_date&start_date_gte={thirty_ago}"
        )
        monthly[dag_id] = m.get("dag_runs", [])
    return latest, monthly


def _fetch_ingestion_log() -> dict:
    """Return latest ingestion_log row per pipeline_id."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
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
    return {r[0]: r for r in rows}


def get_pipeline_data() -> list[dict]:
    """Merge Airflow DAG state with ingestion_log for all pipelines."""
    dags_resp = _airflow("/dags?limit=100")
    dags      = {d["dag_id"]: d for d in dags_resp.get("dags", [])}
    latest_runs, monthly_runs = _fetch_dag_runs(dags)
    ingestion = _fetch_ingestion_log()

    pipelines = []
    for dag_id, dag in dags.items():
        run   = latest_runs.get(dag_id) or {}
        il    = ingestion.get(dag_id)
        state = "paused" if dag.get("is_paused") else (run.get("state") or "no runs")
        dur   = (
            f"{il[2]}s" if il and il[2]
            else _duration(run.get("start_date"), run.get("end_date"))
        )
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


def get_run_history(limit: int = 25) -> list[dict]:
    """Return the most recent DAG-level run records from pipeline_runs."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT pipeline_id, dag_run_id, state, started_at,"
            "       duration_seconds, error_message"
            " FROM pipeline_metadata.pipeline_runs"
            " ORDER BY started_at DESC NULLS LAST"
            " LIMIT :limit"
        ), {"limit": limit}).fetchall()
    return [
        {
            "pipeline_id":   r[0],
            "dag_run_id":    r[1],
            "state":         r[2],
            "started_at":    _fmt_date(str(r[3]) if r[3] else None),
            "duration":      f"{r[4]}s" if r[4] is not None else "—",
            "error_message": r[5] or "",
        }
        for r in rows
    ]


def get_lineage() -> list[dict]:
    """Return the static source→transform→delivery topology per pipeline."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT pipeline_id, source_type, source_location,"
            "       transform_type, transform_target,"
            "       delivery_type, delivery_location"
            " FROM pipeline_metadata.pipeline_lineage"
            " ORDER BY pipeline_id"
        )).fetchall()
    return [
        {
            "pipeline_id": r[0],
            "source":      f"{r[1]} → {r[2]}" if r[1] else "—",
            "transform":   f"{r[3]} ({r[4]})"  if r[3] else "—",
            "delivery":    f"{r[5]} → {r[6]}"  if r[5] else "—",
        }
        for r in rows
    ]


# ── JSON API ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict:
    """Health check used by Docker."""
    return {"status": "ok"}


@app.get("/")
def root() -> dict:
    """Service info."""
    return {"service": "datafabrik-api", "version": "0.1.0"}


@app.get("/api/pipelines")
def api_pipelines() -> list[dict]:
    """Pipeline health + 30-day success rate."""
    return get_pipeline_data()


@app.get("/api/runs")
def api_runs(limit: int = 25) -> list[dict]:
    """Recent DAG-level run records from pipeline_metadata.pipeline_runs."""
    return get_run_history(limit)


@app.get("/api/lineage")
def api_lineage() -> list[dict]:
    """Static source→transform→delivery topology per pipeline."""
    return get_lineage()


# ── HTML dashboard ─────────────────────────────────────────────────────────────

_STATE_BADGE: dict[str, tuple[str, str]] = {
    "success": ('<span class="badge ok">✓ success</span>',   "row-ok"),
    "failed":  ('<span class="badge fail">✗ failed</span>',  "row-fail"),
    "running": ('<span class="badge run">↻ running</span>',  "row-run"),
    "queued":  ('<span class="badge run">⋯ queued</span>',   "row-run"),
    "paused":  ('<span class="badge pause">⏸ paused</span>', "row-pause"),
    "no runs": ('<span class="badge none">— no runs</span>', ""),
}

_CSS = (
    "* { box-sizing: border-box; margin: 0; padding: 0; }\n"
    "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;"
    "       background: #0f1117; color: #e2e8f0; min-height: 100vh; }\n"
    "header { background: #1a1f2e; border-bottom: 1px solid #2d3748;"
    "         padding: 18px 32px; display: flex; align-items: center; gap: 14px; }\n"
    "header h1 { font-size: 1.2rem; font-weight: 700; letter-spacing: .5px; }\n"
    "header .sub { font-size: .8rem; color: #718096; margin-left: auto; }\n"
    ".content { padding: 28px 32px; }\n"
    ".summary { display: flex; gap: 16px; margin-bottom: 28px; }\n"
    ".stat { background: #1a1f2e; border: 1px solid #2d3748; border-radius: 10px;"
    "        padding: 16px 24px; min-width: 110px; }\n"
    ".stat .num { font-size: 2rem; font-weight: 700; line-height: 1; }\n"
    ".stat .lbl { font-size: .75rem; color: #718096; margin-top: 4px;"
    "             text-transform: uppercase; }\n"
    ".num.ok    { color: #48bb78; } .num.fail  { color: #fc8181; }\n"
    ".num.pause { color: #ecc94b; } .num.total { color: #63b3ed; }\n"
    "h2 { font-size: 1rem; font-weight: 600; margin: 28px 0 14px; }\n"
    "table { width: 100%; border-collapse: collapse; background: #1a1f2e;"
    "        border: 1px solid #2d3748; border-radius: 10px; overflow: hidden;"
    "        margin-bottom: 32px; }\n"
    "thead th { text-align: left; padding: 12px 16px; font-size: .72rem;"
    "           text-transform: uppercase; letter-spacing: .6px; color: #718096;"
    "           border-bottom: 1px solid #2d3748; }\n"
    "tbody tr { border-bottom: 1px solid #1e2535; transition: background .15s; }\n"
    "tbody tr:last-child { border-bottom: none; }\n"
    "tbody tr:hover { background: #1e2535; }\n"
    "tbody td { padding: 13px 16px; font-size: .88rem; }\n"
    ".badge { display: inline-block; padding: 3px 10px; border-radius: 20px;"
    "         font-size: .75rem; font-weight: 600; }\n"
    ".badge.ok    { background: #1a3a2a; color: #48bb78; }\n"
    ".badge.fail  { background: #3a1a1a; color: #fc8181; }\n"
    ".badge.run   { background: #1a2a3a; color: #63b3ed; }\n"
    ".badge.pause { background: #3a3a1a; color: #ecc94b; }\n"
    ".badge.none  { background: #2d3748; color: #718096; }\n"
    ".row-fail td:first-child { border-left: 3px solid #fc8181; }\n"
    ".row-ok   td:first-child { border-left: 3px solid #48bb78; }\n"
    ".row-run  td:first-child { border-left: 3px solid #63b3ed; }\n"
    ".row-pause td:first-child{ border-left: 3px solid #ecc94b; }\n"
    "a.af-link { color: #63b3ed; text-decoration: none; font-size: .8rem; }\n"
    "a.af-link:hover { text-decoration: underline; }\n"
    ".refresh { font-size: .75rem; color: #4a5568; margin-top: 16px; text-align: right; }\n"
    ".bar-wrap { background: #2d3748; border-radius: 4px; height: 6px;"
    "            width: 100px; margin-top: 5px; }\n"
    ".bar-fill { height: 6px; border-radius: 4px; }\n"
    ".bar-hi { background: #48bb78; } .bar-mid { background: #ecc94b; }"
    " .bar-lo { background: #fc8181; }\n"
    ".pct-lbl { font-size: .78rem; font-weight: 600; }\n"
    ".pct-hi { color: #48bb78; } .pct-mid { color: #ecc94b; }"
    " .pct-lo { color: #fc8181; }\n"
    ".err { font-size: .75rem; color: #fc8181; max-width: 320px;"
    "       white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n"
    ".dim { color: #4a5568; font-size: .8rem; }\n"
)


def _pct_cell(pct: float | None, ok: int, total: int) -> str:
    if pct is None:
        return '<span class="dim">no data</span>'
    tier = "hi" if pct >= 90 else ("mid" if pct >= 70 else "lo")
    return (
        f'<span class="pct-lbl pct-{tier}">{pct}%</span>'
        f'<div class="bar-wrap">'
        f'<div class="bar-fill bar-{tier}" style="width:{pct}%"></div></div>'
        f'<span class="dim">{ok}/{total} runs</span>'
    )


def _pipeline_rows(pipelines: list[dict]) -> str:
    html = ""
    for p in pipelines:
        badge, row_cls = _STATE_BADGE.get(
            p["state"],
            (f'<span class="badge none">{p["state"]}</span>', ""),
        )
        af_url   = f"http://localhost:8080/dags/{p['id']}/grid"
        pct_html = _pct_cell(p["pct_30d"], p["ok_30d"], p["runs_30d"])
        html += (
            f'<tr class="{row_cls}">'
            f'<td><strong>{p["id"]}</strong><br>'
            f'<a class="af-link" href="{af_url}" target="_blank">'
            f"open in Airflow ↗</a></td>"
            f"<td>{badge}</td>"
            f'<td>{p["last_run"]}</td>'
            f'<td>{p["duration"]}</td>'
            f"<td>{pct_html}</td>"
            f'<td>{p["rows"]}</td>'
            f'<td class="dim">{p["watermark"]}</td>'
            f"</tr>"
        )
    return html


def _history_rows(runs: list[dict]) -> str:
    html = ""
    for r in runs:
        badge, row_cls = _STATE_BADGE.get(
            r["state"],
            (f'<span class="badge none">{r["state"]}</span>', ""),
        )
        err = (
            f'<span class="err" title="{r["error_message"]}">'
            f'{r["error_message"][:80]}</span>'
            if r["error_message"] else '<span class="dim">—</span>'
        )
        html += (
            f'<tr class="{row_cls}">'
            f'<td><strong>{r["pipeline_id"]}</strong></td>'
            f"<td>{badge}</td>"
            f'<td>{r["started_at"]}</td>'
            f'<td>{r["duration"]}</td>'
            f"<td>{err}</td>"
            f"</tr>"
        )
    return html


def _lineage_rows(lineage: list[dict]) -> str:
    html = ""
    for l in lineage:
        html += (
            f'<tr>'
            f'<td><strong>{l["pipeline_id"]}</strong></td>'
            f'<td class="dim">{l["source"]}</td>'
            f'<td class="dim">{l["transform"]}</td>'
            f'<td class="dim">{l["delivery"]}</td>'
            f"</tr>"
        )
    return html


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    """Pipeline health dashboard — auto-refreshes every 30 seconds."""
    try:
        pipelines = get_pipeline_data()
        fetch_err = None
    except Exception as exc:  # noqa: BLE001
        pipelines = []
        fetch_err = str(exc)

    try:
        history = get_run_history(25)
    except Exception:  # noqa: BLE001
        history = []

    try:
        lineage = get_lineage()
    except Exception:  # noqa: BLE001
        lineage = []

    ok     = sum(1 for p in pipelines if p["state"] == "success")
    failed = sum(1 for p in pipelines if p["state"] == "failed")
    paused = sum(1 for p in pipelines if p["state"] == "paused")
    total  = len(pipelines)

    error_banner = (
        f'<div style="background:#3a1a1a;color:#fc8181;padding:12px 16px;'
        f'border-radius:8px;margin-bottom:20px">⚠ {fetch_err}</div>'
        if fetch_err else ""
    )
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

  <h2>Pipelines</h2>
  <table>
    <thead><tr>
      <th>Pipeline</th><th>State</th><th>Last Run (UTC)</th>
      <th>Duration</th><th>30-day Success Rate</th><th>Rows</th><th>Watermark</th>
    </tr></thead>
    <tbody>{_pipeline_rows(pipelines)}</tbody>
  </table>

  <h2>Run History <span class="dim">(last 25)</span></h2>
  <table>
    <thead><tr>
      <th>Pipeline</th><th>State</th><th>Started (UTC)</th>
      <th>Duration</th><th>Failure Reason</th>
    </tr></thead>
    <tbody>{_history_rows(history) or '<tr><td colspan="5" class="dim" style="padding:20px">No runs recorded yet — runs appear here after the next DAG execution.</td></tr>'}</tbody>
  </table>

  <h2>Lineage</h2>
  <table>
    <thead><tr>
      <th>Pipeline</th><th>Source</th><th>Transform</th><th>Delivery</th>
    </tr></thead>
    <tbody>{_lineage_rows(lineage) or '<tr><td colspan="4" class="dim" style="padding:20px">No lineage recorded yet — appears after the first DAG run.</td></tr>'}</tbody>
  </table>

  <p class="refresh">Page refreshes automatically every 30 seconds.</p>
</div>
</body>
</html>"""
