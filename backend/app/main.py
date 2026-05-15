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


@app.post("/api/pipelines/{dag_id}/trigger")
def api_trigger_pipeline(dag_id: str) -> dict:
    """Trigger an Airflow DAG run."""
    resp = _requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        json={},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


_PORTAL_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DataFabrik Portal</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;height:100vh;overflow:hidden}
.layout{display:flex;height:100vh}
/* ── Sidebar ── */
.sidebar{width:220px;min-width:220px;background:#1a1f2e;border-right:1px solid #2d3748;display:flex;flex-direction:column;overflow:hidden}
.sb-header{padding:20px 16px 16px;border-bottom:1px solid #2d3748;display:flex;align-items:center;gap:10px}
.sb-logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.sb-logo svg{width:16px;height:16px}
.sb-title{font-size:.95rem;font-weight:700;letter-spacing:.3px;white-space:nowrap}
.sb-nav{flex:1;padding:12px 8px;overflow-y:auto}
.nav-btn{width:100%;background:none;border:none;color:#a0aec0;cursor:pointer;padding:10px 12px;border-radius:8px;text-align:left;font-size:.875rem;display:flex;align-items:center;gap:10px;transition:background .15s,color .15s;margin-bottom:2px}
.nav-btn:hover{background:#232a3b;color:#e2e8f0}
.nav-btn.active{background:#1e3a5f;color:#63b3ed;font-weight:600}
.nav-btn .icon{font-size:1rem;width:20px;text-align:center;flex-shrink:0}
.sb-section{font-size:.68rem;text-transform:uppercase;letter-spacing:.8px;color:#4a5568;padding:12px 12px 4px;font-weight:600}
.sb-footer{padding:12px 16px;border-top:1px solid #2d3748;font-size:.72rem;color:#4a5568}
/* ── Main ── */
.main{flex:1;overflow:hidden;display:flex;flex-direction:column}
.section{display:none;flex:1;overflow:auto;height:100%}
.section.active{display:flex;flex-direction:column}
.iframe-section{padding:0}
.iframe-section iframe{width:100%;height:100%;border:none;flex:1}
.iframe-section .iframe-wrap{flex:1;display:flex;flex-direction:column;position:relative}
.iframe-bar{background:#151a27;border-bottom:1px solid #2d3748;padding:8px 16px;display:flex;align-items:center;gap:12px;font-size:.8rem;color:#718096;flex-shrink:0}
.iframe-bar a{color:#63b3ed;text-decoration:none;font-weight:500}
.iframe-bar a:hover{text-decoration:underline}
/* ── Content sections ── */
.sec-header{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:16px 28px;display:flex;align-items:center;gap:12px;flex-shrink:0}
.sec-header h2{font-size:1.05rem;font-weight:700}
.sec-header .sub{font-size:.8rem;color:#718096;margin-left:auto}
.sec-body{padding:24px 28px;overflow-y:auto;flex:1}
/* ── Cards ── */
.cards{display:flex;gap:14px;margin-bottom:24px;flex-wrap:wrap}
.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:18px 22px;min-width:110px}
.card .num{font-size:2rem;font-weight:700;line-height:1}
.card .lbl{font-size:.72rem;color:#718096;margin-top:4px;text-transform:uppercase;letter-spacing:.4px}
.num.blue{color:#63b3ed}.num.green{color:#48bb78}.num.red{color:#fc8181}.num.yellow{color:#ecc94b}
/* ── Quick links ── */
.qlinks{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:28px}
.qlink{background:#1a1f2e;border:1px solid #2d3748;border-radius:8px;padding:12px 18px;color:#e2e8f0;text-decoration:none;font-size:.875rem;font-weight:500;display:flex;align-items:center;gap:8px;transition:background .15s,border-color .15s}
.qlink:hover{background:#232a3b;border-color:#3182ce;color:#63b3ed}
.qlink .ql-icon{font-size:1.1rem}
/* ── Tables ── */
table{width:100%;border-collapse:collapse;background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;overflow:hidden;margin-bottom:24px}
thead th{text-align:left;padding:11px 14px;font-size:.7rem;text-transform:uppercase;letter-spacing:.5px;color:#718096;border-bottom:1px solid #2d3748}
tbody tr{border-bottom:1px solid #1e2535;transition:background .15s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:#1e2535}
tbody td{padding:12px 14px;font-size:.875rem;vertical-align:middle}
/* ── Badges ── */
.badge{display:inline-block;padding:3px 9px;border-radius:20px;font-size:.72rem;font-weight:600}
.badge.ok{background:#1a3a2a;color:#48bb78}
.badge.fail{background:#3a1a1a;color:#fc8181}
.badge.run{background:#1a2a3a;color:#63b3ed}
.badge.pause{background:#3a3a1a;color:#ecc94b}
.badge.none{background:#2d3748;color:#718096}
/* ── Buttons ── */
.btn{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border-radius:6px;font-size:.8rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s}
.btn:disabled{opacity:.5;cursor:not-allowed}
.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}
.btn-sm{padding:4px 10px;font-size:.75rem}
.btn-ghost{background:#2d3748;color:#e2e8f0}.btn-ghost:hover:not(:disabled){background:#3a4459}
.btn-success{background:#276749;color:#9ae6b4}.btn-success:hover:not(:disabled){background:#22543d}
.btn-danger{background:#742a2a;color:#fc8181}.btn-danger:hover:not(:disabled){background:#9b2c2c}
.btn-trigger{background:#2a3a5a;color:#90cdf4;border:1px solid #2b4c7e}.btn-trigger:hover:not(:disabled){background:#2b4c7e}
/* ── Forms ── */
.form-group{margin-bottom:18px}
.form-label{display:block;font-size:.8rem;font-weight:600;color:#a0aec0;margin-bottom:6px;text-transform:uppercase;letter-spacing:.4px}
.form-control{width:100%;background:#151a27;border:1px solid #2d3748;border-radius:6px;padding:9px 12px;color:#e2e8f0;font-size:.875rem;outline:none;transition:border-color .15s}
.form-control:focus{border-color:#3182ce}
select.form-control{cursor:pointer}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.form-section{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:20px;margin-bottom:20px}
.form-section h3{font-size:.9rem;font-weight:600;margin-bottom:16px;color:#a0aec0;display:flex;align-items:center;gap:8px}
/* ── Code blocks ── */
.code-block{position:relative;background:#0d1117;border:1px solid #2d3748;border-radius:8px;overflow:hidden;margin-bottom:16px}
.code-block-header{background:#161b22;padding:8px 14px;display:flex;align-items:center;justify-content:space-between;font-size:.75rem;color:#718096;border-bottom:1px solid #2d3748}
.code-block pre{padding:14px;overflow-x:auto;font-size:.8rem;line-height:1.6;color:#e2e8f0;font-family:'JetBrains Mono','Fira Code',monospace;margin:0;white-space:pre}
/* ── Toast ── */
#toast-container{position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none}
.toast{background:#1a1f2e;border:1px solid #2d3748;border-radius:8px;padding:12px 16px;font-size:.85rem;display:flex;align-items:center;gap:10px;box-shadow:0 4px 20px rgba(0,0,0,.5);animation:slideIn .2s ease;pointer-events:all;max-width:340px}
.toast.ok{border-color:#276749;background:#1a2e1a}.toast.err{border-color:#742a2a;background:#2e1a1a}
@keyframes slideIn{from{transform:translateX(20px);opacity:0}to{transform:none;opacity:1}}
/* ── Misc ── */
.empty{color:#4a5568;font-size:.875rem;padding:24px;text-align:center}
.loading{color:#718096;font-size:.875rem;padding:12px 0;display:flex;align-items:center;gap:8px}
.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#63b3ed;border-radius:50%;animation:spin .6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.tag{display:inline-block;background:#2d3748;color:#a0aec0;padding:2px 8px;border-radius:12px;font-size:.72rem;margin:2px}
.pct-bar{background:#2d3748;border-radius:3px;height:5px;width:80px;margin-top:4px}
.pct-fill{height:5px;border-radius:3px}
.pct-hi{background:#48bb78}.pct-mid{background:#ecc94b}.pct-lo{background:#fc8181}
.dim{color:#4a5568;font-size:.8rem}
.result-section{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:20px;margin-bottom:20px}
.result-section h3{font-size:.9rem;font-weight:600;margin-bottom:14px;color:#a0aec0}
.val-ok{color:#48bb78;font-size:.85rem;display:flex;align-items:center;gap:6px}
.val-err{color:#fc8181;font-size:.85rem;display:flex;align-items:center;gap:6px}
/* ── Tool launch cards ── */
.tool-card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:32px;display:flex;gap:24px;align-items:flex-start;margin-bottom:24px;max-width:680px}
.tool-card-icon{font-size:3rem;flex-shrink:0;width:64px;text-align:center;margin-top:4px}
.tool-card-title{font-size:1.3rem;font-weight:700;margin-bottom:8px}
.tool-card-desc{font-size:.875rem;color:#a0aec0;line-height:1.6;margin-bottom:12px}
.tool-card-meta{display:flex;gap:8px;flex-wrap:wrap}
.tool-badge{background:#2d3748;color:#a0aec0;padding:3px 10px;border-radius:12px;font-size:.75rem;font-family:monospace}
.tool-tips{background:#151a27;border:1px solid #2d3748;border-radius:10px;padding:18px 22px;max-width:680px}
.tool-tips-title{font-size:.72rem;text-transform:uppercase;letter-spacing:.5px;color:#4a5568;font-weight:600;margin-bottom:12px}
.tool-tip{font-size:.85rem;color:#a0aec0;padding:5px 0;border-bottom:1px solid #1e2535;line-height:1.5}
.tool-tip:last-child{border-bottom:none}
</style>
</head>
<body>
<div id="toast-container"></div>
<div class="layout">

<!-- ── Sidebar ── -->
<nav class="sidebar">
  <div class="sb-header">
    <div class="sb-logo">
      <svg viewBox="0 0 16 16" fill="none"><rect width="16" height="16" rx="3" fill="#3182ce"/><path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/></svg>
    </div>
    <span class="sb-title">DataFabrik</span>
  </div>
  <div class="sb-nav">
    <div class="sb-section">Platform</div>
    <button class="nav-btn active" onclick="nav('home')" id="nav-home">
      <span class="icon">🏠</span> Home
    </button>
    <button class="nav-btn" onclick="nav('pipelines')" id="nav-pipelines">
      <span class="icon">📋</span> Pipelines
    </button>
    <div class="sb-section">Tools</div>
    <button class="nav-btn" onclick="nav('airflow')" id="nav-airflow">
      <span class="icon">✈️</span> Airflow
    </button>
    <button class="nav-btn" onclick="nav('metabase')" id="nav-metabase">
      <span class="icon">📊</span> Metabase
    </button>
    <button class="nav-btn" onclick="nav('minio')" id="nav-minio">
      <span class="icon">🗄️</span> MinIO
    </button>
    <div class="sb-section">Build</div>
    <button class="nav-btn" onclick="nav('builder')" id="nav-builder">
      <span class="icon">🔧</span> Pipeline Builder
    </button>
  </div>
  <div class="sb-footer">Local Development</div>
</nav>

<!-- ── Main ── -->
<main class="main">

  <!-- HOME -->
  <div id="sec-home" class="section active">
    <div class="sec-header">
      <h2>DataFabrik Portal</h2>
      <span class="sub" id="home-ts"></span>
    </div>
    <div class="sec-body">
      <div class="cards" id="home-cards">
        <div class="card"><div class="num blue" id="stat-total">—</div><div class="lbl">Pipelines</div></div>
        <div class="card"><div class="num green" id="stat-ok">—</div><div class="lbl">Passing</div></div>
        <div class="card"><div class="num red" id="stat-fail">—</div><div class="lbl">Failed</div></div>
        <div class="card"><div class="num yellow" id="stat-paused">—</div><div class="lbl">Paused</div></div>
      </div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Quick Access</h3>
      <div class="qlinks">
        <a class="qlink" href="#" onclick="nav('pipelines');return false"><span class="ql-icon">📋</span> Pipelines</a>
        <a class="qlink" href="http://localhost:8080" target="_blank"><span class="ql-icon">✈️</span> Airflow ↗</a>
        <a class="qlink" href="http://localhost:3000" target="_blank"><span class="ql-icon">📊</span> Metabase ↗</a>
        <a class="qlink" href="http://localhost:9001" target="_blank"><span class="ql-icon">🗄️</span> MinIO ↗</a>
        <a class="qlink" href="/dashboard" target="_blank"><span class="ql-icon">🖥️</span> Health Dashboard ↗</a>
      </div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Recent Runs</h3>
      <div id="home-runs"><div class="loading"><span class="spinner"></span> Loading…</div></div>
    </div>
  </div>

  <!-- PIPELINES -->
  <div id="sec-pipelines" class="section">
    <div class="sec-header">
      <h2>Pipelines</h2>
      <button class="btn btn-ghost btn-sm" onclick="loadPipelines()">↻ Refresh</button>
      <span class="sub" id="pipes-ts"></span>
    </div>
    <div class="sec-body">
      <div id="pipes-content"><div class="loading"><span class="spinner"></span> Loading…</div></div>
    </div>
  </div>

  <!-- AIRFLOW -->
  <div id="sec-airflow" class="section iframe-section">
    <div class="iframe-bar">
      <span>✈️ Apache Airflow — DAG orchestration</span>
      <a href="http://localhost:8080" target="_blank">Open in new tab ↗</a>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-airflow" title="Airflow" allowfullscreen></iframe>
    </div>
  </div>

  <!-- METABASE -->
  <div id="sec-metabase" class="section iframe-section">
    <div class="iframe-bar">
      <span>📊 Metabase — Dashboards &amp; analytics &nbsp;·&nbsp; <code style="font-size:.78rem">admin / admin</code></span>
      <a href="http://localhost:3000" target="_blank">Open in new tab ↗</a>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-metabase" title="Metabase" allowfullscreen></iframe>
    </div>
  </div>

  <!-- MINIO -->
  <div id="sec-minio" class="section iframe-section">
    <div class="iframe-bar">
      <span>🗄️ MinIO — Object storage &nbsp;·&nbsp; <code style="font-size:.78rem">minioadmin / minioadmin</code></span>
      <a href="http://localhost:9001" target="_blank">Open in new tab ↗</a>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-minio" title="MinIO" allowfullscreen></iframe>
    </div>
  </div>

  <!-- BUILDER -->
  <div id="sec-builder" class="section">
    <div class="sec-header">
      <h2>🔧 Pipeline Builder</h2>
    </div>
    <div class="sec-body">
      <!-- Form -->
      <div id="builder-form-wrap">
        <div class="form-section">
          <h3>① Pipeline Info</h3>
          <div class="form-row">
            <div class="form-group">
              <label class="form-label">Pipeline Name *</label>
              <input id="b-name" class="form-control" placeholder="e.g. acme_orders" type="text">
            </div>
            <div class="form-group">
              <label class="form-label">Owner</label>
              <input id="b-owner" class="form-control" value="data-platform" type="text">
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Description</label>
            <input id="b-desc" class="form-control" placeholder="What does this pipeline do?" type="text">
          </div>
          <div class="form-row">
            <div class="form-group">
              <label class="form-label">Schedule</label>
              <select id="b-schedule" class="form-control">
                <option value="@daily">@daily — once a day</option>
                <option value="@hourly">@hourly — once an hour</option>
                <option value="@weekly">@weekly — once a week</option>
                <option value="@monthly">@monthly — once a month</option>
                <option value="@once">@once — manual trigger only</option>
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Tags (comma-separated)</label>
              <input id="b-tags" class="form-control" placeholder="e.g. finance, postgres" type="text">
            </div>
          </div>
        </div>

        <div class="form-section">
          <h3>② Data Source</h3>
          <div class="form-group">
            <label class="form-label">Source Type *</label>
            <select id="b-src-type" class="form-control" onchange="updateSourceFields()">
              <option value="jdbc">JDBC — database table/query</option>
              <option value="http_api">HTTP API — REST endpoint</option>
              <option value="s3_csv">S3 CSV — files in a bucket</option>
            </select>
          </div>
          <!-- JDBC fields -->
          <div id="src-jdbc">
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Airflow Connection ID *</label>
                <input id="b-conn" class="form-control" placeholder="e.g. acme_postgres" type="text">
              </div>
              <div class="form-group">
                <label class="form-label">Table Name(s)</label>
                <input id="b-tables" class="form-control" placeholder="e.g. orders, customers" type="text">
              </div>
            </div>
            <div class="form-group">
              <label class="form-label">SQL Query *</label>
              <textarea id="b-query" class="form-control" rows="3" placeholder="SELECT * FROM orders WHERE updated_at > \'{{ ds }}\'"></textarea>
            </div>
            <div class="form-group">
              <label class="form-label">Destination Key (S3)</label>
              <input id="b-dest-key" class="form-control" placeholder="e.g. acme/orders/{{ ds }}.parquet" type="text">
            </div>
          </div>
          <!-- HTTP API fields -->
          <div id="src-http" style="display:none">
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">URL *</label>
                <input id="b-url" class="form-control" placeholder="https://api.example.com/v1/data" type="text">
              </div>
              <div class="form-group">
                <label class="form-label">Method</label>
                <select id="b-method" class="form-control">
                  <option>GET</option>
                  <option>POST</option>
                </select>
              </div>
            </div>
            <div class="form-group">
              <label class="form-label">Destination Key (S3)</label>
              <input id="b-dest-key-http" class="form-control" placeholder="e.g. weather/{{ ds_nodash }}.json" type="text">
            </div>
          </div>
          <!-- S3 CSV fields -->
          <div id="src-s3" style="display:none">
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Source Bucket *</label>
                <input id="b-src-bucket" class="form-control" placeholder="e.g. vendor-data" type="text">
              </div>
              <div class="form-group">
                <label class="form-label">Source Key Pattern *</label>
                <input id="b-src-key" class="form-control" placeholder="e.g. daily/*.csv" type="text">
              </div>
            </div>
          </div>
        </div>

        <div class="form-section">
          <h3>③ Transformation</h3>
          <div class="form-group">
            <label class="form-label">Transform Type</label>
            <select id="b-transform" class="form-control">
              <option value="dbt">dbt — SQL models</option>
              <option value="none">None — skip transformation</option>
            </select>
          </div>
          <div id="dbt-select-wrap" class="form-group">
            <label class="form-label">dbt --select (model names, space-separated)</label>
            <input id="b-dbt-select" class="form-control" placeholder="auto-generated from pipeline name" type="text">
          </div>
        </div>

        <div style="display:flex;gap:12px;align-items:center">
          <button class="btn btn-primary" onclick="generatePipeline()" id="gen-btn">
            ⚙️ Generate Pipeline Config
          </button>
          <span id="gen-status" style="font-size:.8rem;color:#718096"></span>
        </div>
      </div>

      <!-- Results -->
      <div id="builder-results" style="display:none;margin-top:24px">
        <div class="result-section">
          <h3>Validation</h3>
          <div id="val-status"></div>
        </div>
        <div class="result-section">
          <h3>Pipeline YAML</h3>
          <p class="dim" style="margin-bottom:10px">Save this as <code style="background:#2d3748;padding:2px 6px;border-radius:4px">orchestration/airflow/dags/&lt;pipeline_id&gt;.yaml</code></p>
          <div class="code-block">
            <div class="code-block-header">
              <span>pipeline.yaml</span>
              <button class="btn btn-ghost btn-sm" onclick="copyCode('yaml-out')">Copy</button>
            </div>
            <pre id="yaml-out"></pre>
          </div>
        </div>
        <div id="dbt-results"></div>
        <button class="btn btn-ghost" onclick="resetBuilder()" style="margin-top:8px">← New Pipeline</button>
      </div>
    </div>
  </div>

</main>
</div>

<script>
// ── Navigation ──────────────────────────────────────────────────────────
const SECTIONS = ['home','pipelines','airflow','metabase','minio','builder'];
const IFRAMES  = {airflow:'http://localhost:8080', metabase:'http://localhost:3001', minio:'http://localhost:9002'};
const iframeLoaded = {};

function nav(id) {
  SECTIONS.forEach(s => {
    document.getElementById('sec-'+s).classList.toggle('active', s===id);
    document.getElementById('nav-'+s).classList.toggle('active', s===id);
  });
  if (IFRAMES[id] && !iframeLoaded[id]) {
    document.getElementById('frame-'+id).src = IFRAMES[id];
    iframeLoaded[id] = true;
  }
  if (id === 'pipelines') loadPipelines();
  if (id === 'home') loadHome();
}

// ── Toast ───────────────────────────────────────────────────────────────
function toast(msg, type='ok') {
  const t = document.createElement('div');
  t.className = 'toast ' + type;
  t.textContent = (type==='ok' ? '✓ ' : '✗ ') + msg;
  document.getElementById('toast-container').appendChild(t);
  setTimeout(() => t.remove(), 4000);
}

// ── Home ────────────────────────────────────────────────────────────────
async function loadHome() {
  document.getElementById('home-ts').textContent = 'Loading…';
  try {
    const [pipes, runs] = await Promise.all([
      fetch('/api/pipelines').then(r=>r.json()),
      fetch('/api/runs?limit=8').then(r=>r.json())
    ]);
    document.getElementById('stat-total').textContent = pipes.length;
    document.getElementById('stat-ok').textContent = pipes.filter(p=>p.state==='success').length;
    document.getElementById('stat-fail').textContent = pipes.filter(p=>p.state==='failed').length;
    document.getElementById('stat-paused').textContent = pipes.filter(p=>p.state==='paused').length;
    document.getElementById('home-ts').textContent = 'Updated ' + new Date().toLocaleTimeString();
    renderHomeRuns(runs);
  } catch(e) {
    document.getElementById('home-ts').textContent = 'Error loading data';
  }
}

function renderHomeRuns(runs) {
  const el = document.getElementById('home-runs');
  if (!runs.length) { el.innerHTML = '<div class="empty">No runs recorded yet.</div>'; return; }
  el.innerHTML = '<table><thead><tr><th>Pipeline</th><th>State</th><th>Started</th><th>Duration</th></tr></thead><tbody>'
    + runs.map(r=>`<tr>
        <td><strong>${r.pipeline_id}</strong></td>
        <td>${badge(r.state)}</td>
        <td class="dim">${r.started_at}</td>
        <td class="dim">${r.duration}</td>
      </tr>`).join('')
    + '</tbody></table>';
}

// ── Pipelines ───────────────────────────────────────────────────────────
async function loadPipelines() {
  document.getElementById('pipes-content').innerHTML = '<div class="loading"><span class="spinner"></span> Loading…</div>';
  document.getElementById('pipes-ts').textContent = '';
  try {
    const pipes = await fetch('/api/pipelines').then(r=>r.json());
    renderPipelines(pipes);
    document.getElementById('pipes-ts').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('pipes-content').innerHTML = '<div class="empty">Failed to load pipelines: ' + e.message + '</div>';
  }
}

function renderPipelines(pipes) {
  if (!pipes.length) {
    document.getElementById('pipes-content').innerHTML = '<div class="empty">No pipelines found in Airflow.</div>';
    return;
  }
  const rows = pipes.map(p => {
    const pct = p.pct_30d !== null ? p.pct_30d : null;
    const tier = pct === null ? '' : (pct >= 90 ? 'hi' : (pct >= 70 ? 'mid' : 'lo'));
    const pctHtml = pct !== null
      ? `<span style="font-weight:600;color:${tier==='hi'?'#48bb78':tier==='mid'?'#ecc94b':'#fc8181'}">${pct}%</span>
         <div class="pct-bar"><div class="pct-fill pct-${tier}" style="width:${pct}%"></div></div>
         <span class="dim">${p.ok_30d}/${p.runs_30d}</span>`
      : '<span class="dim">no data</span>';
    return `<tr>
      <td><strong>${p.id}</strong></td>
      <td>${badge(p.state)}</td>
      <td class="dim">${p.last_run}</td>
      <td class="dim">${p.duration}</td>
      <td>${pctHtml}</td>
      <td class="dim">${p.rows}</td>
      <td>
        <button class="btn btn-trigger btn-sm" onclick="triggerPipeline('${p.id}', this)"
          ${p.state==='running'?'disabled':''}>
          ▶ Run
        </button>
      </td>
    </tr>`;
  }).join('');

  document.getElementById('pipes-content').innerHTML = `
    <table>
      <thead><tr>
        <th>Pipeline</th><th>State</th><th>Last Run</th>
        <th>Duration</th><th>30-day Success</th><th>Rows</th><th>Action</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

async function triggerPipeline(dagId, btn) {
  btn.disabled = true;
  btn.textContent = '…';
  try {
    await fetch('/api/pipelines/' + encodeURIComponent(dagId) + '/trigger', {method:'POST'});
    toast('Triggered ' + dagId);
    setTimeout(() => loadPipelines(), 1500);
  } catch(e) {
    toast('Failed to trigger ' + dagId + ': ' + e.message, 'err');
    btn.disabled = false;
    btn.textContent = '▶ Run';
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────
function badge(state) {
  const map = {
    success: ['ok','✓ success'], failed: ['fail','✗ failed'],
    running: ['run','↻ running'], queued: ['run','⋯ queued'],
    paused:  ['pause','⏸ paused'], 'no runs': ['none','— no runs']
  };
  const [cls, label] = map[state] || ['none', state];
  return `<span class="badge ${cls}">${label}</span>`;
}

// ── Builder ─────────────────────────────────────────────────────────────
function updateSourceFields() {
  const t = document.getElementById('b-src-type').value;
  document.getElementById('src-jdbc').style.display = t==='jdbc' ? '' : 'none';
  document.getElementById('src-http').style.display = t==='http_api' ? '' : 'none';
  document.getElementById('src-s3').style.display   = t==='s3_csv' ? '' : 'none';
}

function updateTransformFields() {
  const t = document.getElementById('b-transform').value;
  document.getElementById('dbt-select-wrap').style.display = t==='dbt' ? '' : 'none';
}

async function generatePipeline() {
  const name = document.getElementById('b-name').value.trim();
  if (!name) { toast('Pipeline name is required', 'err'); return; }

  const srcType = document.getElementById('b-src-type').value;
  const schedule = document.getElementById('b-schedule').value;
  const owner = document.getElementById('b-owner').value.trim() || 'data-platform';
  const tags = document.getElementById('b-tags').value.split(',').map(t=>t.trim()).filter(Boolean);
  const transform = document.getElementById('b-transform').value;
  const dbtSelect = document.getElementById('b-dbt-select').value.trim();

  const pipelineId = name.toLowerCase().replace(/[^a-z0-9]+/g,'_');

  let ingestion;
  if (srcType === 'jdbc') {
    const connId = document.getElementById('b-conn').value.trim();
    const query  = document.getElementById('b-query').value.trim();
    const destKey = document.getElementById('b-dest-key').value.trim() || `${pipelineId}/{{ ds }}.parquet`;
    if (!connId || !query) { toast('Connection ID and query are required for JDBC', 'err'); return; }
    ingestion = {type:'jdbc', connection_id:connId, query, dest_key:destKey};
  } else if (srcType === 'http_api') {
    const url = document.getElementById('b-url').value.trim();
    const method = document.getElementById('b-method').value;
    const destKey = document.getElementById('b-dest-key-http').value.trim() || `${pipelineId}/{{ ds_nodash }}.json`;
    if (!url) { toast('URL is required for HTTP API', 'err'); return; }
    ingestion = {type:'http_api', url, method, dest_key:destKey};
  } else {
    const srcBucket = document.getElementById('b-src-bucket').value.trim();
    const srcKey    = document.getElementById('b-src-key').value.trim();
    if (!srcBucket || !srcKey) { toast('Source bucket and key pattern are required for S3', 'err'); return; }
    ingestion = {type:'s3_csv', source_bucket:srcBucket, source_key:srcKey};
  }

  // Build stages
  const stages = {ingestion};
  if (transform === 'dbt') {
    const stagingModel = `stg_${pipelineId}`;
    const analyticsModel = `${pipelineId}_summary`;
    stages.transformation = {type:'dbt', select: dbtSelect || `${stagingModel} ${analyticsModel}`};
  }

  // Build schedule
  let scheduleConfig;
  if (schedule.startsWith('@')) {
    scheduleConfig = {preset: schedule, start_date: new Date().toISOString().slice(0,10) + 'T00:00:00'};
  } else {
    scheduleConfig = {cron: schedule, start_date: new Date().toISOString().slice(0,10) + 'T00:00:00'};
  }

  const config = {pipeline_id: pipelineId, description: document.getElementById('b-desc').value.trim() || null,
    owner, tags, schedule: scheduleConfig, stages};

  // Generate dbt SQL stubs (client-side, no AI)
  const stagingModel = `stg_${pipelineId}`;
  const analyticsModel = `${pipelineId}_summary`;
  const tableHint = srcType === 'jdbc'
    ? (document.getElementById('b-tables').value.split(',')[0].trim() || pipelineId)
    : pipelineId;

  const stagingSQL = `SELECT
    -- TODO: replace with actual column list
    *
FROM {{ source('raw', '${tableHint}') }}`;

  const analyticsSQL = `SELECT
    -- TODO: add GROUP BY dimensions
    COUNT(*) AS row_count
FROM {{ ref('${stagingModel}') }}`;

  const dbtModels = transform === 'dbt' ? {
    [`${stagingModel}.sql`]: stagingSQL,
    [`${analyticsModel}.sql`]: analyticsSQL
  } : {};

  // Render results
  renderBuilderResults({
    pipeline_yaml: jsYaml(config),
    dbt_models: dbtModels,
    validation_passed: true,
    validation_error: null
  });
}

function jsYaml(obj, indent=0) {
  // Minimal YAML serializer for our config object
  const pad = '  '.repeat(indent);
  const lines = [];
  for (const [k, v] of Object.entries(obj)) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'string') {
      const needsQuotes = v.includes(':') || v.includes('#') || v.includes('{') || v === '';
      lines.push(`${pad}${k}: ${needsQuotes ? JSON.stringify(v) : v}`);
    } else if (typeof v === 'boolean' || typeof v === 'number') {
      lines.push(`${pad}${k}: ${v}`);
    } else if (Array.isArray(v)) {
      if (v.length === 0) continue;
      if (v.every(i => typeof i === 'string')) {
        lines.push(`${pad}${k}: [${v.join(', ')}]`);
      } else {
        lines.push(`${pad}${k}:`);
        v.forEach(item => {
          if (typeof item === 'object') {
            const entries = Object.entries(item);
            lines.push(`${pad}  - ${entries[0][0]}: ${entries[0][1]}`);
            entries.slice(1).forEach(([ik,iv]) => lines.push(`${pad}    ${ik}: ${iv}`));
          } else {
            lines.push(`${pad}  - ${item}`);
          }
        });
      }
    } else if (typeof v === 'object') {
      lines.push(`${pad}${k}:`);
      lines.push(jsYaml(v, indent+1));
    }
  }
  return lines.join('\\n');
}

function renderBuilderResults(result) {
  document.getElementById('builder-form-wrap').style.display = 'none';
  document.getElementById('builder-results').style.display = '';

  // Validation
  const valEl = document.getElementById('val-status');
  valEl.innerHTML = result.validation_passed
    ? '<div class="val-ok">✓ Config looks valid</div>'
    : `<div class="val-err">✗ ${result.validation_error}</div>`;

  // YAML
  document.getElementById('yaml-out').textContent = result.pipeline_yaml;

  // dbt models
  const dbtEl = document.getElementById('dbt-results');
  if (Object.keys(result.dbt_models || {}).length) {
    dbtEl.innerHTML = '<div class="result-section"><h3>dbt Model Stubs</h3>'
      + Object.entries(result.dbt_models).map(([fname, sql]) => `
        <div class="code-block">
          <div class="code-block-header">
            <span>${fname}</span>
            <button class="btn btn-ghost btn-sm" onclick="copyText(${JSON.stringify(sql)})">Copy</button>
          </div>
          <pre>${sql.replace(/</g,'&lt;')}</pre>
        </div>`).join('')
      + '</div>';
  } else {
    dbtEl.innerHTML = '';
  }
}

function resetBuilder() {
  document.getElementById('builder-form-wrap').style.display = '';
  document.getElementById('builder-results').style.display = 'none';
}

function copyCode(id) {
  copyText(document.getElementById(id).textContent);
}

function copyText(text) {
  navigator.clipboard.writeText(text).then(() => toast('Copied to clipboard'));
}

// ── Init ─────────────────────────────────────────────────────────────────
loadHome();
</script>
</body>
</html>'''


@app.get("/", response_class=HTMLResponse)
def portal() -> str:
    """DataFabrik portal — unified UI for all platform tools."""
    return _PORTAL_HTML


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
