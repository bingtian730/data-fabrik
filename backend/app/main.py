"""DataFabrik FastAPI backend — pipeline health dashboard and metadata API."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import csv
import io
import re

import requests as _requests
import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
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


# ── Admin API ──────────────────────────────────────────────────────────────────

_RESTART_CONTAINERS = [
    "datafabrik-airflow-webserver",
    "datafabrik-airflow-scheduler",
    "datafabrik-metabase",
    "datafabrik-minio",
    "datafabrik-nginx-proxy",
    "datafabrik-presto",
]

_HEALTH_CHECKS: dict[str, str] = {
    "airflow":  f"{AIRFLOW_URL}/health",
    "minio":    "http://minio:9000/minio/health/live",
    "metabase": "http://metabase:3000/api/health",
    "presto":   "http://presto:8080/v1/info",
}


def _check_one(service: str) -> str:
    """Return 'up' / 'degraded' / 'down' for a single service."""
    if service == "postgres":
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return "up"
        except Exception:
            return "down"
    url = _HEALTH_CHECKS.get(service)
    if not url:
        return "unknown"
    try:
        r = _requests.get(url, timeout=5)
        return "up" if r.status_code < 500 else "degraded"
    except Exception:
        return "down"


@app.get("/api/admin/health")
def api_admin_health() -> dict:
    """Ping every service; returns {service: status} mapping."""
    services = ["postgres", *_HEALTH_CHECKS.keys()]
    return {svc: _check_one(svc) for svc in services}


@app.get("/api/admin/health/{service}")
def api_admin_health_one(service: str) -> dict:
    """Ping a single named service; used by per-card progress bars."""
    if service not in ("postgres", *_HEALTH_CHECKS.keys()):
        raise HTTPException(status_code=404, detail=f"Unknown service '{service}'")
    return {"service": service, "status": _check_one(service)}


@app.post("/api/admin/restart")
def api_admin_restart() -> dict:
    """Restart non-critical containers in a background thread; returns immediately."""
    import threading

    try:
        import docker as _docker_mod
        client = _docker_mod.from_env()
    except ImportError:
        raise HTTPException(status_code=503, detail="docker SDK not installed in container")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Docker socket unavailable: {exc}")

    def _do_restart() -> None:
        for name in _RESTART_CONTAINERS:
            try:
                container = client.containers.get(name)
                container.restart(timeout=10)
            except Exception:
                pass

    threading.Thread(target=_do_restart, daemon=True).start()

    return {"message": "Restart initiated — services back online in ~30–60 seconds."}


# ── Tool sign-in relays ───────────────────────────────────────────────────────

_RELAY_CSS = (
    'body{background:#0f1117;color:#e2e8f0;font-family:-apple-system,sans-serif;'
    'display:flex;align-items:center;justify-content:center;height:100vh;margin:0;flex-direction:column;gap:12px}'
    '.sp{width:20px;height:20px;border:2px solid #2d3748;border-top-color:#63b3ed;'
    'border-radius:50%;animation:spin .7s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
)


@app.get("/tools/airflow", response_class=HTMLResponse)
def tool_airflow() -> HTMLResponse:
    """
    Auto-login relay for Airflow.

    Cookies on 'localhost' are shared across all ports (RFC 6265 has no port
    scope).  We fetch Airflow's login page server-side to get a valid
    session cookie + CSRF token, then forward the session cookie to the
    browser and embed the CSRF token in an auto-submit form.  When the
    browser POSTs to localhost:8080/login/ it already holds the matching
    session, so Airflow's CSRF check passes.
    """
    import re as _re2
    csrf_token = ""
    session_val = ""
    try:
        s = _requests.Session()
        r = s.get("http://airflow-webserver:8080/login/", timeout=5)
        m = _re2.search(r'name="csrf_token"\s+[^>]*value="([^"]+)"', r.text)
        csrf_token = m.group(1) if m else ""
        session_val = r.cookies.get("session", "")
    except Exception:
        pass  # fall through — form will still attempt login

    body = (
        '<!DOCTYPE html><html><head><meta charset="UTF-8">'
        f'<style>{_RELAY_CSS}</style></head><body>'
        '<div class="sp"></div>'
        '<p style="color:#718096;font-size:.85rem">Signing in to Airflow…</p>'
        '<form id="f" action="http://localhost:8082/login/" method="POST" style="display:none">'
        f'<input name="csrf_token" value="{csrf_token}">'
        f'<input name="username"   value="{AIRFLOW_USER}">'
        f'<input name="password"   value="{AIRFLOW_PASS}">'
        '</form>'
        '<script>document.getElementById("f").submit();</script>'
        '</body></html>'
    )
    resp = HTMLResponse(body)
    if session_val:
        # Forward Airflow's session cookie — the browser will send it to
        # localhost:8080 (same host, different port) when the form submits.
        resp.set_cookie("session", session_val, httponly=True, samesite="lax", path="/")
    return resp


@app.get("/tools/minio", response_class=HTMLResponse)
def tool_minio() -> HTMLResponse:
    """Sign in to MinIO console via its JSON API, then redirect."""
    minio_user = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_pass = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    body = (
        '<!DOCTYPE html><html><head><meta charset="UTF-8">'
        f'<style>{_RELAY_CSS}</style></head><body>'
        '<div class="sp"></div>'
        '<p style="color:#718096;font-size:.85rem">Signing in to MinIO…</p>'
        '<script>'
        f'fetch("http://localhost:9001/api/v1/login",{{method:"POST",'
        f'credentials:"include",headers:{{"Content-Type":"application/json"}},'
        f'body:JSON.stringify({{accessKey:"{minio_user}",secretKey:"{minio_pass}"}})}}'
        f').then(()=>window.location="http://localhost:9001")'
        f'.catch(()=>window.location="http://localhost:9001");'
        '</script>'
        '</body></html>'
    )
    return HTMLResponse(body)


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
/* ── Service status ── */
.svc-grid{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px}
.svc-card{background:#1a1f2e;border:1px solid #2d3748;border-radius:8px;padding:10px 14px 13px;display:flex;align-items:center;gap:8px;min-width:140px;position:relative;overflow:hidden}
.svc-dot{width:8px;height:8px;border-radius:50%;background:#4a5568;flex-shrink:0;transition:background .3s}
.svc-dot.up{background:#48bb78}.svc-dot.down{background:#fc8181}.svc-dot.degraded{background:#ecc94b}.svc-dot.checking{background:#4a5568}
.svc-name{font-size:.8rem;font-weight:600;color:#e2e8f0}.svc-status-txt{font-size:.72rem;color:#718096;margin-left:auto;text-transform:capitalize}
.svc-bar{position:absolute;bottom:0;left:0;right:0;height:3px;background:#1e2535}
.svc-bar-fill{height:3px;width:0%;border-radius:0 2px 2px 0;background:#3182ce}
/* ── Credentials ── */
.cred-grid{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:24px}
.cred-card{background:#151a27;border:1px solid #2d3748;border-radius:8px;padding:12px 16px;min-width:200px}
.cred-tool{font-size:.78rem;font-weight:600;color:#a0aec0;margin-bottom:8px}
.cred-row{font-size:.75rem;color:#718096;display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:3px}
.cred-val{color:#e2e8f0;font-family:monospace;font-size:.78rem}
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
    <button class="nav-btn" onclick="nav('onboard')" id="nav-onboard">
      <span class="icon">🚀</span> Onboard Customer
    </button>
    <button class="nav-btn" onclick="nav('upload')" id="nav-upload">
      <span class="icon">📤</span> Upload Data
    </button>
    <button class="nav-btn" onclick="nav('workflow')" id="nav-workflow">
      <span class="icon">🧹</span> Workflow Wizard
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
      <button class="btn btn-ghost btn-sm" onclick="checkHealth()" id="health-btn">↻ Check Services</button>
      <button class="btn btn-danger btn-sm" onclick="restartServices()" id="restart-btn">🔄 Restart Services</button>
      <button class="btn btn-ghost btn-sm" onclick="openAllTools()">🔑 Open All Tools</button>
      <span class="sub" id="home-ts"></span>
    </div>
    <div class="sec-body">
      <div class="cards" id="home-cards">
        <div class="card"><div class="num blue" id="stat-total">—</div><div class="lbl">Pipelines</div></div>
        <div class="card"><div class="num green" id="stat-ok">—</div><div class="lbl">Passing</div></div>
        <div class="card"><div class="num red" id="stat-fail">—</div><div class="lbl">Failed</div></div>
        <div class="card"><div class="num yellow" id="stat-paused">—</div><div class="lbl">Paused</div></div>
      </div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Services</h3>
      <div class="svc-grid">
        <div class="svc-card"><span class="svc-dot" id="dot-postgres"></span><span class="svc-name">Postgres</span><span class="svc-status-txt" id="status-postgres">—</span><div class="svc-bar"><div class="svc-bar-fill" id="barfill-postgres"></div></div></div>
        <div class="svc-card"><span class="svc-dot" id="dot-airflow"></span><span class="svc-name">Airflow</span><span class="svc-status-txt" id="status-airflow">—</span><div class="svc-bar"><div class="svc-bar-fill" id="barfill-airflow"></div></div></div>
        <div class="svc-card"><span class="svc-dot" id="dot-minio"></span><span class="svc-name">MinIO</span><span class="svc-status-txt" id="status-minio">—</span><div class="svc-bar"><div class="svc-bar-fill" id="barfill-minio"></div></div></div>
        <div class="svc-card"><span class="svc-dot" id="dot-metabase"></span><span class="svc-name">Metabase</span><span class="svc-status-txt" id="status-metabase">—</span><div class="svc-bar"><div class="svc-bar-fill" id="barfill-metabase"></div></div></div>
        <div class="svc-card"><span class="svc-dot" id="dot-presto"></span><span class="svc-name">Presto</span><span class="svc-status-txt" id="status-presto">—</span><div class="svc-bar"><div class="svc-bar-fill" id="barfill-presto"></div></div></div>
      </div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Quick Access</h3>
      <div class="qlinks">
        <a class="qlink" href="#" onclick="nav('pipelines');return false"><span class="ql-icon">📋</span> Pipelines</a>
        <a class="qlink" href="/tools/airflow" target="_blank"><span class="ql-icon">✈️</span> Airflow ↗</a>
        <a class="qlink" href="http://localhost:3000" target="_blank"><span class="ql-icon">📊</span> Metabase ↗</a>
        <a class="qlink" href="http://localhost:9001" target="_blank"><span class="ql-icon">🗄️</span> MinIO ↗</a>
        <a class="qlink" href="/dashboard" target="_blank"><span class="ql-icon">🖥️</span> Health Dashboard ↗</a>
        <a class="qlink" href="#" onclick="nav('onboard');return false"><span class="ql-icon">🚀</span> Onboard Customer</a>
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
                <label class="form-label">Source Table * (schema.table)</label>
                <input id="b-table" class="form-control" placeholder="e.g. public.orders" type="text">
              </div>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label class="form-label">Watermark Column</label>
                <input id="b-watermark" class="form-control" placeholder="updated_at" type="text" value="updated_at">
              </div>
              <div class="form-group">
                <label class="form-label">Destination Prefix (S3)</label>
                <input id="b-dest-prefix" class="form-control" placeholder="auto-generated from pipeline name" type="text">
              </div>
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

  <!-- ONBOARD CUSTOMER -->
  <div id="sec-onboard" class="section iframe-section">
    <div class="iframe-bar">
      <span>🚀 Onboard Customer — register a new data pipeline</span>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-onboard" title="Onboard Customer" allowfullscreen></iframe>
    </div>
  </div>

  <!-- UPLOAD DATA -->
  <div id="sec-upload" class="section iframe-section">
    <div class="iframe-bar">
      <span>📤 Upload Data — load a CSV into Postgres</span>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-upload" title="Upload Data" allowfullscreen></iframe>
    </div>
  </div>

  <!-- PIPELINE WIZARD -->
  <div id="sec-workflow" class="section iframe-section">
    <div class="iframe-bar">
      <span>🧹 Workflow Wizard — upload CSV, clean data, build aggregation pipelines</span>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-workflow" title="Workflow Wizard" allowfullscreen></iframe>
    </div>
  </div>

</main>
</div>

<script>
// ── Navigation ──────────────────────────────────────────────────────────
const SECTIONS = ['home','pipelines','airflow','metabase','minio','builder','onboard','upload','workflow'];
const IFRAMES  = {airflow:'/tools/airflow', metabase:'http://localhost:3001', minio:'http://localhost:9002',
                  onboard:'/onboard?embed=1', upload:'/upload?embed=1', workflow:'/workflow?embed=1'};
// Airflow is proxied via nginx on :8082 which strips X-Frame-Options
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

// ── Admin / Services ─────────────────────────────────────────────────────
const _SVC_NAMES = ['postgres','airflow','minio','metabase','presto'];

async function checkHealth() {
  const btn = document.getElementById('health-btn');
  if (btn) { btn.disabled = true; btn.textContent = '…'; }

  // Kick off each bar's indeterminate fill animation
  _SVC_NAMES.forEach(s => {
    const dot  = document.getElementById('dot-'+s);
    const st   = document.getElementById('status-'+s);
    const fill = document.getElementById('barfill-'+s);
    if (dot)  dot.className = 'svc-dot checking';
    if (st)   st.textContent = 'checking…';
    if (fill) {
      fill.style.cssText = 'width:0%;background:#3182ce;transition:none;opacity:1';
      fill.offsetWidth; // force reflow so animation restarts
      fill.style.transition = 'width 3s cubic-bezier(.05,.6,.1,1)';
      fill.style.width = '78%';
    }
  });

  let up = 0;
  // Fire one request per service in parallel so bars resolve independently
  await Promise.all(_SVC_NAMES.map(async svc => {
    let status = 'down';
    try {
      const r = await fetch('/api/admin/health/'+svc).then(res => res.json());
      status = r.status || 'down';
    } catch(_) {}

    if (status === 'up') up++;
    const dot  = document.getElementById('dot-'+svc);
    const st   = document.getElementById('status-'+svc);
    const fill = document.getElementById('barfill-'+svc);
    if (dot)  dot.className = 'svc-dot ' + status;
    if (st)   st.textContent = status;
    if (fill) {
      const color = status === 'up' ? '#48bb78' : status === 'down' ? '#fc8181' : '#ecc94b';
      fill.style.transition = 'width .2s ease, background .2s';
      fill.style.background = color;
      fill.style.width = '100%';
      setTimeout(() => {
        fill.style.transition = 'opacity .35s';
        fill.style.opacity = '0';
        setTimeout(() => { fill.style.cssText = 'width:0%;opacity:1;transition:none'; }, 380);
      }, 400);
    }
  }));

  const total = _SVC_NAMES.length;
  toast(up === total ? `All ${total} services up` : `${up}/${total} services up`, up === total ? 'ok' : 'err');
  if (btn) { btn.disabled = false; btn.textContent = '↻ Check Services'; }
}

async function restartServices() {
  if (!confirm('Restart Airflow, Metabase, MinIO, Nginx, and Presto?\\n\\nThey will be back in ~30 seconds.')) return;
  const btn = document.getElementById('restart-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Restarting…'; }
  try {
    await fetch('/api/admin/restart', {method:'POST'}).then(r => r.json());
    toast('Restart triggered — checking health in 30s');
    setTimeout(checkHealth, 30000);
  } catch(e) {
    toast('Restart failed: ' + e.message, 'err');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '🔄 Restart Services'; }
  }
}

function openAllTools() {
  window.open('/tools/airflow', '_blank');
  window.open('/tools/minio', '_blank');
  window.open('http://localhost:3000', '_blank');
  toast('Opening tools — signing in automatically…');
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
  checkHealth();
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
    const connId    = document.getElementById('b-conn').value.trim();
    const table     = document.getElementById('b-table').value.trim();
    const watermark = document.getElementById('b-watermark').value.trim() || 'updated_at';
    const destPfx   = document.getElementById('b-dest-prefix').value.trim() || null;
    if (!connId || !table) { toast('Connection ID and table are required for JDBC', 'err'); return; }
    ingestion = {type:'jdbc', connection_id:connId, table, watermark_column:watermark,
                 ...(destPfx && {dest_prefix:destPfx})};
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


# ── Onboarding API ───────────────────────────────────────────────────────────

_CONFIGS_DIR    = Path("/app/configs/pipelines")
_DBT_MODELS_DIR = Path("/app/dbt_models")

class _OnboardPayload(BaseModel):
    yaml_content: str


def _validate_config(data: dict) -> list[dict]:
    """Return [{field, message}] for every schema violation found."""
    errors: list[dict] = []

    pid = str(data.get("pipeline_id", ""))
    if not pid:
        errors.append({"field": "pipeline_id", "message": "Required"})
    elif not all(c.isalnum() or c in "_-" for c in pid):
        errors.append({"field": "pipeline_id",
                        "message": "Only letters, numbers, underscores, and dashes allowed"})

    stages = data.get("stages")
    if not isinstance(stages, dict):
        errors.append({"field": "stages", "message": "Required — must contain at least an ingestion block"})
        return errors

    ingestion = stages.get("ingestion")
    if not isinstance(ingestion, dict):
        errors.append({"field": "stages.ingestion", "message": "Required — every pipeline needs a source"})
    else:
        src_type = ingestion.get("type")
        valid_src = {"jdbc", "http_api", "s3_csv"}
        if src_type not in valid_src:
            errors.append({"field": "stages.ingestion.type",
                            "message": f"Must be one of: {', '.join(sorted(valid_src))}. Got '{src_type}'"})
        elif src_type == "jdbc":
            for f in ("connection_id", "table"):
                if not ingestion.get(f):
                    errors.append({"field": f"stages.ingestion.{f}", "message": "Required for JDBC source"})
            for bad in ("query", "dest_key"):
                if ingestion.get(bad):
                    errors.append({"field": f"stages.ingestion.{bad}",
                                   "message": f"Not a valid JDBC field — use 'table' instead of 'query', and remove 'dest_key'"})
        elif src_type == "http_api":
            for f in ("url", "dest_key"):
                if not ingestion.get(f):
                    errors.append({"field": f"stages.ingestion.{f}", "message": "Required for HTTP API source"})
        elif src_type == "s3_csv":
            for f in ("source_bucket", "source_key"):
                if not ingestion.get(f):
                    errors.append({"field": f"stages.ingestion.{f}", "message": "Required for S3 CSV source"})

    transform = stages.get("transformation")
    if transform is not None:
        if not isinstance(transform, dict):
            errors.append({"field": "stages.transformation", "message": "Must be a mapping"})
        else:
            t_type = transform.get("type")
            if t_type not in ("dbt", "sql", "spark"):
                errors.append({"field": "stages.transformation.type",
                                "message": "Must be one of: dbt, sql, spark"})
            elif t_type == "sql":
                for f in ("connection_id", "sql_file"):
                    if not transform.get(f):
                        errors.append({"field": f"stages.transformation.{f}",
                                        "message": "Required for SQL transform"})

    schedule = data.get("schedule")
    if schedule is not None:
        if not isinstance(schedule, dict):
            errors.append({"field": "schedule", "message": "Must be a mapping"})
        else:
            if schedule.get("cron") and schedule.get("preset"):
                errors.append({"field": "schedule",
                                "message": "Specify cron or preset — not both"})
            if not schedule.get("start_date"):
                errors.append({"field": "schedule.start_date", "message": "Required"})

    return errors


@app.post("/api/onboard/validate")
def api_onboard_validate(payload: _OnboardPayload) -> dict:
    """Parse and validate a pipeline YAML string; return field-level errors."""
    try:
        data = yaml.safe_load(payload.yaml_content)
        if not isinstance(data, dict):
            return {"valid": False, "errors": [{"field": "root", "message": "Config must be a YAML mapping"}],
                    "pipeline_id": None}
    except yaml.YAMLError as exc:
        return {"valid": False, "errors": [{"field": "root", "message": f"Invalid YAML: {exc}"}],
                "pipeline_id": None}

    errors = _validate_config(data)
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "pipeline_id": data.get("pipeline_id") if not errors else None,
    }


@app.post("/api/onboard/submit")
def api_onboard_submit(payload: _OnboardPayload) -> dict:
    """Validate then write the pipeline config; Airflow picks it up within 30 s."""
    result = api_onboard_validate(payload)
    if not result["valid"]:
        return result

    pipeline_id = result["pipeline_id"]
    _CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    config_path = _CONFIGS_DIR / f"{pipeline_id}.yaml"
    config_path.write_text(payload.yaml_content)

    return {
        "valid": True,
        "errors": [],
        "pipeline_id": pipeline_id,
        "message": f"Pipeline '{pipeline_id}' registered. Airflow will load it within ~30 seconds.",
    }


# ── Onboarding page HTML ──────────────────────────────────────────────────────

_TMPL_JDBC = """pipeline_id: my_pipeline_daily
description: Load data from database daily
owner: data-platform
tags: [myteam]

schedule:
  preset: "@daily"
  start_date: "2026-01-01T00:00:00"

stages:
  ingestion:
    type: jdbc
    connection_id: my_connection_id
    table: public.my_table
    watermark_column: updated_at
  transformation:
    type: dbt
    select: stg_my_pipeline_daily"""

_TMPL_HTTP = """pipeline_id: my_api_pipeline
description: Fetch data from REST API daily
owner: data-platform
tags: [api]

schedule:
  preset: "@daily"
  start_date: "2026-01-01T00:00:00"

stages:
  ingestion:
    type: http_api
    url: https://api.example.com/v1/data
    method: GET
    dest_key: my_api/{{ ds_nodash }}.json
  transformation:
    type: dbt
    select: stg_my_api_pipeline"""

_TMPL_S3 = """pipeline_id: my_csv_pipeline
description: Load CSV files from S3 daily
owner: data-platform
tags: [csv]

schedule:
  preset: "@daily"
  start_date: "2026-01-01T00:00:00"

stages:
  ingestion:
    type: s3_csv
    source_bucket: customer-landing
    source_key: my-folder/*.csv
  transformation:
    type: dbt
    select: stg_my_csv_pipeline"""

_ONBOARD_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>DataFabrik — Pipeline Onboarding</title>'
    '<style>'
    '*{box-sizing:border-box;margin:0;padding:0}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;'
    'background:#0f1117;color:#e2e8f0;min-height:100vh}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;'
    'display:flex;align-items:center;gap:14px}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;'
    'display:flex;align-items:center;justify-content:center;flex-shrink:0}'
    '.logo svg{width:16px;height:16px}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.topbar a:hover{text-decoration:underline}'
    '.page{max-width:780px;margin:0 auto;padding:36px 24px}'
    '.hero{margin-bottom:36px}'
    '.hero h2{font-size:1.6rem;font-weight:700;margin-bottom:8px}'
    '.hero p{color:#a0aec0;font-size:.95rem;line-height:1.6}'
    '.steps{display:flex;gap:0;margin-bottom:36px}'
    '.step{display:flex;align-items:center;gap:8px;font-size:.8rem;color:#4a5568;flex:1}'
    '.step.done{color:#48bb78}.step.active{color:#63b3ed;font-weight:600}'
    '.step-num{width:22px;height:22px;border-radius:50%;background:#2d3748;'
    'display:flex;align-items:center;justify-content:center;font-size:.72rem;'
    'font-weight:700;flex-shrink:0}'
    '.step.done .step-num{background:#276749;color:#9ae6b4}'
    '.step.active .step-num{background:#2b4c7e;color:#90cdf4}'
    '.step-sep{flex:1;height:1px;background:#2d3748;margin:0 8px;max-width:40px}'
    '.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;'
    'padding:24px;margin-bottom:20px}'
    '.card h3{font-size:.85rem;font-weight:700;text-transform:uppercase;'
    'letter-spacing:.5px;color:#718096;margin-bottom:16px;'
    'display:flex;align-items:center;gap:8px}'
    '.tmpl-row{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}'
    '.tmpl-btn{background:#2d3748;border:1px solid #3a4459;color:#a0aec0;'
    'border-radius:6px;padding:6px 14px;font-size:.8rem;cursor:pointer;'
    'transition:background .15s,color .15s,border-color .15s}'
    '.tmpl-btn:hover,.tmpl-btn.sel{background:#2b4c7e;border-color:#3182ce;color:#90cdf4}'
    '.drop-zone{border:2px dashed #2d3748;border-radius:8px;padding:20px;'
    'text-align:center;cursor:pointer;margin-bottom:12px;'
    'transition:border-color .15s,background .15s;font-size:.85rem;color:#4a5568}'
    '.drop-zone:hover,.drop-zone.drag{border-color:#3182ce;background:#0d1627}'
    '.drop-zone input{display:none}'
    'textarea{width:100%;background:#0d1117;border:1px solid #2d3748;border-radius:8px;'
    'padding:14px;color:#e2e8f0;font-family:"JetBrains Mono","Fira Code",monospace;'
    'font-size:.8rem;line-height:1.6;resize:vertical;outline:none;'
    'transition:border-color .15s}'
    'textarea:focus{border-color:#3182ce}'
    '.actions{display:flex;gap:12px;align-items:center;margin-top:4px}'
    '.btn{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;'
    'border-radius:8px;font-size:.875rem;font-weight:600;cursor:pointer;'
    'border:none;transition:opacity .15s,background .15s}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}'
    '.btn-success{background:#276749;color:#9ae6b4}'
    '.btn-success:hover:not(:disabled){background:#22543d}'
    '.btn-ghost{background:#2d3748;color:#e2e8f0}'
    '.btn-ghost:hover:not(:disabled){background:#3a4459}'
    '.spin-wrap{display:flex;align-items:center;gap:8px;font-size:.85rem;color:#718096}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;'
    'border-top-color:#63b3ed;border-radius:50%;animation:spin .6s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.val-list{list-style:none;display:flex;flex-direction:column;gap:8px}'
    '.val-item{display:flex;align-items:flex-start;gap:10px;'
    'background:#151a27;border-radius:6px;padding:10px 14px}'
    '.val-item.ok{border-left:3px solid #48bb78}'
    '.val-item.err{border-left:3px solid #fc8181}'
    '.val-icon{font-size:1rem;flex-shrink:0;margin-top:1px}'
    '.val-field{font-size:.75rem;font-family:monospace;font-weight:600;'
    'color:#a0aec0;margin-bottom:2px}'
    '.val-msg{font-size:.8rem;color:#718096}'
    '.val-item.err .val-msg{color:#fc8181}'
    '.summary-ok{display:flex;align-items:center;gap:10px;'
    'background:#1a2e1a;border:1px solid #276749;border-radius:8px;'
    'padding:14px 18px;font-size:.9rem;color:#9ae6b4;font-weight:600}'
    '.summary-err{display:flex;align-items:center;gap:10px;'
    'background:#2e1a1a;border:1px solid #742a2a;border-radius:8px;'
    'padding:14px 18px;font-size:.9rem;color:#fc8181;font-weight:600}'
    '.success-box{background:#1a2e1a;border:1px solid #276749;border-radius:12px;'
    'padding:28px;text-align:center}'
    '.success-box .tick{font-size:2.5rem;margin-bottom:12px}'
    '.success-box h3{font-size:1.1rem;font-weight:700;color:#9ae6b4;margin-bottom:8px}'
    '.success-box p{font-size:.875rem;color:#68d391;line-height:1.6}'
    '.next-steps{margin-top:16px;text-align:left;'
    'background:#151a27;border-radius:8px;padding:14px 18px}'
    '.next-steps h4{font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;'
    'color:#4a5568;margin-bottom:10px}'
    '.next-step{font-size:.85rem;color:#a0aec0;padding:5px 0;'
    'border-bottom:1px solid #1e2535;display:flex;align-items:center;gap:8px}'
    '.next-step:last-child{border-bottom:none}'
    '.next-step a{color:#63b3ed;text-decoration:none}'
    '.next-step a:hover{text-decoration:underline}'
    '.hidden{display:none}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16" fill="none">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik</h1>'
    '<span style="color:#4a5568;font-size:.85rem">Pipeline Onboarding</span>'
    '<a href="/">← Back to portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="hero">'
    '<h2>Connect your data source</h2>'
    '<p>Paste or upload your pipeline configuration below. We\'ll validate it and '
    'register it with the platform automatically.</p>'
    '</div>'
    '<div class="steps" id="steps">'
    '<div class="step active" id="step1"><div class="step-num">1</div> Configure</div>'
    '<div class="step-sep"></div>'
    '<div class="step" id="step2"><div class="step-num">2</div> Validate</div>'
    '<div class="step-sep"></div>'
    '<div class="step" id="step3"><div class="step-num">3</div> Register</div>'
    '</div>'
    '<div class="card" id="card-configure">'
    '<h3>① Source configuration</h3>'
    '<div class="tmpl-row">'
    '<button class="tmpl-btn sel" id="tmpl-jdbc" onclick="useTemplate(\'jdbc\')">🔌 JDBC / Database</button>'
    '<button class="tmpl-btn" id="tmpl-http" onclick="useTemplate(\'http\')">🌐 REST API</button>'
    '<button class="tmpl-btn" id="tmpl-s3" onclick="useTemplate(\'s3\')">📦 S3 CSV</button>'
    '<button class="tmpl-btn" id="tmpl-blank" onclick="useTemplate(\'blank\')">✏️ Start blank</button>'
    '</div>'
    '<div class="drop-zone" id="drop-zone" '
    'ondragover="event.preventDefault();this.classList.add(\'drag\')" '
    'ondragleave="this.classList.remove(\'drag\')" '
    'ondrop="handleDrop(event)">'
    '<input type="file" id="file-input" accept=".yaml,.yml,.json" onchange="handleFile(this)">'
    'Drop a <strong>.yaml</strong> file here, or '
    '<span style="color:#63b3ed;cursor:pointer" onclick="document.getElementById(\'file-input\').click()">'
    'browse to upload</span>'
    '</div>'
    '<textarea id="yaml-editor" rows="18" spellcheck="false" '
    'placeholder="Paste your pipeline YAML config here..." '
    'oninput="onEdit()"></textarea>'
    '</div>'
    '<div class="card hidden" id="card-validation">'
    '<h3>② Validation results</h3>'
    '<div id="val-summary"></div>'
    '<ul class="val-list" id="val-list" style="margin-top:14px"></ul>'
    '</div>'
    '<div class="actions">'
    '<button class="btn btn-primary" id="btn-validate" onclick="doValidate()">🔍 Validate config</button>'
    '<button class="btn btn-success hidden" id="btn-register" onclick="doRegister()">🚀 Register pipeline</button>'
    '<div class="spin-wrap hidden" id="spinner"><div class="spinner"></div> Working…</div>'
    '</div>'
    '<div class="hidden" id="card-success" style="margin-top:24px">'
    '<div class="success-box">'
    '<div class="tick">✅</div>'
    '<h3 id="success-title">Pipeline registered!</h3>'
    '<p id="success-msg"></p>'
    '<div class="next-steps">'
    '<h4>Next steps</h4>'
    '<div class="next-step">① '
    '<a href="http://localhost:8080" target="_blank">Open Airflow ↗</a>'
    ' — your DAG will appear within ~30 seconds</div>'
    '<div class="next-step">② Add dbt models under '
    '<code style="background:#2d3748;padding:1px 6px;border-radius:4px">'
    'dbt/datafabrik_models/models/&lt;pipeline_id&gt;/</code></div>'
    '<div class="next-step">③ Trigger a test run from the '
    '<a href="/" target="_blank">portal Pipelines tab ↗</a></div>'
    '</div>'
    '</div>'
    '</div>'
    '</div>'
    '<script>'
    'const TMPLS={'
    'jdbc:' + repr(_TMPL_JDBC) + ','
    'http:' + repr(_TMPL_HTTP) + ','
    's3:'   + repr(_TMPL_S3)   + ','
    'blank:""'
    '};'
    'let lastValid=false;'
    'function useTemplate(t){'
    '  document.getElementById("yaml-editor").value=TMPLS[t];'
    '  ["jdbc","http","s3","blank"].forEach(k=>{'
    '    document.getElementById("tmpl-"+k).classList.toggle("sel",k===t);'
    '  });'
    '  onEdit();'
    '}'
    'function onEdit(){'
    '  lastValid=false;'
    '  document.getElementById("btn-register").classList.add("hidden");'
    '  document.getElementById("card-success").classList.add("hidden");'
    '  setStep(1);'
    '}'
    'function handleDrop(e){'
    '  e.preventDefault();'
    '  document.getElementById("drop-zone").classList.remove("drag");'
    '  const f=e.dataTransfer.files[0];'
    '  if(f) readFile(f);'
    '}'
    'function handleFile(inp){if(inp.files[0]) readFile(inp.files[0]);}'
    'function readFile(f){'
    '  const r=new FileReader();'
    '  r.onload=e=>{document.getElementById("yaml-editor").value=e.target.result;onEdit();};'
    '  r.readAsText(f);'
    '}'
    'function setStep(n){'
    '  [1,2,3].forEach(i=>{'
    '    const el=document.getElementById("step"+i);'
    '    el.className="step"+(i<n?" done":i===n?" active":"");'
    '  });'
    '}'
    'function showSpinner(v){'
    '  document.getElementById("spinner").classList.toggle("hidden",!v);'
    '  document.getElementById("btn-validate").disabled=v;'
    '}'
    'async function doValidate(){'
    '  const yaml=document.getElementById("yaml-editor").value.trim();'
    '  if(!yaml){alert("Please enter or upload a pipeline config first.");return;}'
    '  showSpinner(true);'
    '  document.getElementById("card-success").classList.add("hidden");'
    '  try{'
    '    const r=await fetch("/api/onboard/validate",{'
    '      method:"POST",headers:{"Content-Type":"application/json"},'
    '      body:JSON.stringify({yaml_content:yaml})'
    '    });'
    '    const d=await r.json();'
    '    renderValidation(d);'
    '  } catch(e){alert("Request failed: "+e.message);}'
    '  finally{showSpinner(false);}'
    '}'
    'function renderValidation(d){'
    '  const valCard=document.getElementById("card-validation");'
    '  const sumEl=document.getElementById("val-summary");'
    '  const listEl=document.getElementById("val-list");'
    '  valCard.classList.remove("hidden");'
    '  setStep(2);'
    '  if(d.valid){'
    '    sumEl.innerHTML=\'<div class="summary-ok">✓ &nbsp;Configuration is valid — ready to register</div>\';'
    '    listEl.innerHTML=\'<li class="val-item ok"><span class="val-icon">✓</span><div><div class="val-field">pipeline_id</div><div class="val-msg">\'+d.pipeline_id+\'</div></div></li>\';'
    '    document.getElementById("btn-register").classList.remove("hidden");'
    '    lastValid=true;'
    '  } else {'
    '    const cnt=d.errors.length;'
    '    sumEl.innerHTML=\'<div class="summary-err">✗ &nbsp;\'+cnt+\' issue\'+(cnt>1?"s":"")+" found — fix before registering</div>";'
    '    listEl.innerHTML=d.errors.map(e=>\'<li class="val-item err"><span class="val-icon">✗</span><div><div class="val-field">\'+e.field+\'</div><div class="val-msg">\'+e.message+"</div></div></li>").join("");'
    '    document.getElementById("btn-register").classList.add("hidden");'
    '    lastValid=false;'
    '  }'
    '}'
    'async function doRegister(){'
    '  const yaml=document.getElementById("yaml-editor").value.trim();'
    '  showSpinner(true);'
    '  document.getElementById("btn-register").disabled=true;'
    '  try{'
    '    const r=await fetch("/api/onboard/submit",{'
    '      method:"POST",headers:{"Content-Type":"application/json"},'
    '      body:JSON.stringify({yaml_content:yaml})'
    '    });'
    '    const d=await r.json();'
    '    if(d.valid){'
    '      setStep(3);'
    '      document.getElementById("success-title").textContent="Pipeline \\""+d.pipeline_id+"\\" registered!";'
    '      document.getElementById("success-msg").textContent=d.message;'
    '      document.getElementById("card-success").classList.remove("hidden");'
    '      document.getElementById("btn-register").classList.add("hidden");'
    '    } else {'
    '      renderValidation(d);'
    '    }'
    '  } catch(e){alert("Request failed: "+e.message);}'
    '  finally{showSpinner(false);document.getElementById("btn-register").disabled=false;}'
    '}'
    'useTemplate("jdbc");'
    '</script>'
    '<script>if(location.search.includes("embed=1"))document.querySelectorAll(\'a[href="/"]\').forEach(e=>e.style.display="none");</script>'
    '</body></html>'
)


@app.get("/onboard", response_class=HTMLResponse)
def onboard() -> str:
    """Customer pipeline onboarding UI."""
    return _ONBOARD_HTML


# ── CSV Upload ────────────────────────────────────────────────────────────────

def _safe_name(s: str) -> str:
    """Lowercase and replace non-alphanumeric chars with underscores."""
    return re.sub(r"[^a-z0-9_]", "_", s.lower().strip()).strip("_") or "col"


# ── Workflow: data cleaning pipeline ─────────────────────────────────────────

_VALID_TYPES = {"TEXT", "INTEGER", "NUMERIC", "DATE", "TIMESTAMP", "BOOLEAN"}
_VALID_OPS   = {"=", "!=", ">", ">=", "<", "<=", "LIKE", "IS NULL", "IS NOT NULL"}


def _infer_type(values: list[str]) -> str:
    nz = [v.strip() for v in values if v and v.strip()]
    if not nz:
        return "TEXT"
    if all(re.match(r"^-?\d+$", v) for v in nz):
        return "INTEGER"
    if all(re.match(r"^-?\d+\.?\d*$", v) for v in nz):
        return "NUMERIC"
    if all(re.match(r"^\d{4}-\d{2}-\d{2}$", v) for v in nz):
        return "DATE"
    if all(re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}", v) for v in nz):
        return "TIMESTAMP"
    if all(v.lower() in ("true", "false", "1", "0", "yes", "no") for v in nz):
        return "BOOLEAN"
    return "TEXT"


def _generate_clean_sql(table: str, columns: list, filters: list) -> str:
    select_parts = []
    for col in columns:
        if not col.get("include", True):
            continue
        src  = col["name"]
        out  = col.get("output_name") or src
        dtype = col.get("type", "TEXT")
        if dtype not in _VALID_TYPES:
            dtype = "TEXT"
        if dtype == "TEXT":
            expr = f'trim("{src}")'
        elif dtype == "NUMERIC":
            expr = f'round("{src}"::NUMERIC, 2)'
        else:
            expr = f'"{src}"::{dtype}'
        select_parts.append(f'        {expr} AS "{out}"')

    where_parts = []
    for f in filters:
        op  = f.get("operator", "=")
        col = f.get("column", "")
        val = f.get("value", "")
        if op not in _VALID_OPS or not col:
            continue
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f'"{col}" {op}')
        elif op == "LIKE":
            where_parts.append(f'"{col}" LIKE \'%{val}%\'')
        else:
            where_parts.append(f'"{col}" {op} \'{val}\'')

    select_str = ",\n".join(select_parts) or "        *"
    where_str  = ""
    if where_parts:
        where_str = "    WHERE " + "\n      AND ".join(where_parts) + "\n"

    return (
        f'with source as (\n'
        f'    select * from raw."{table}"\n'
        f'),\n'
        f'cleaned as (\n'
        f'    select\n'
        f'{select_str}\n'
        f'    from source\n'
        f'{where_str}'
        f')\n'
        f'select * from cleaned\n'
    )


class _ColConfig(BaseModel):
    name: str
    output_name: str = ""
    type: str = "TEXT"
    include: bool = True


class _FilterConfig(BaseModel):
    column: str
    operator: str = "="
    value: str = ""


class _BuildCleanPayload(BaseModel):
    table: str
    columns: list[_ColConfig]
    filters: list[_FilterConfig] = []


class _AggMetric(BaseModel):
    column: str
    fn: str
    output_name: str = ""


class _BuildAggPayload(BaseModel):
    table: str
    clean_model: str
    group_by: list[str]
    metrics: list[_AggMetric]


def _generate_agg_sql(clean_model: str, group_by: list[str], metrics: list[dict]) -> str:
    valid_fns = {"SUM", "COUNT", "AVG", "MIN", "MAX"}
    select_parts = []
    for c in group_by:
        select_parts.append(f'        "{c}"')
    for m in metrics:
        fn  = m.get("fn", "SUM").upper()
        col = m.get("column", "")
        out = m.get("output_name", "")
        if fn not in valid_fns:
            fn = "SUM"
        if fn == "COUNT" and col in ("*", ""):
            expr = "COUNT(*)"
            out  = out or "row_count"
        else:
            expr = f'{fn}("{col}")'
            out  = out or f'{col}_{fn.lower()}'
        select_parts.append(f'        {expr} AS "{out}"')
    select_str = ",\n".join(select_parts) or "        *"
    group_str  = ""
    if group_by:
        group_str = "    GROUP BY " + ", ".join(f'"{c}"' for c in group_by) + "\n"
    return (
        f'with source as (\n'
        f'    select * from analytics."{clean_model}"\n'
        f'),\n'
        f'aggregated as (\n'
        f'    select\n'
        f'{select_str}\n'
        f'    from source\n'
        f'{group_str}'
        f')\n'
        f'select * from aggregated\n'
    )


@app.post("/api/workflow/upload")
async def api_workflow_upload(table: str = Form(...), file: UploadFile = File(...)) -> dict:
    table_name = _safe_name(table)
    if not table_name:
        raise HTTPException(status_code=400, detail="Invalid table name")
    content = await file.read()
    reader  = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    rows    = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data rows")
    fieldnames = list(reader.fieldnames or [])
    samples: dict[str, list[str]] = {c: [] for c in fieldnames}
    for row in rows[:50]:
        for col in fieldnames:
            v = (row.get(col) or "").strip()
            if v and len(samples[col]) < 3:
                samples[col].append(v)
    columns = [
        {"name": c.strip(), "type": _infer_type(samples[c]), "samples": samples[c]}
        for c in fieldnames
    ]
    col_defs     = ", ".join(f'"{_safe_name(c)}" TEXT' for c in fieldnames)
    col_list     = ", ".join(f'"{_safe_name(c)}"' for c in fieldnames)
    placeholders = ", ".join(f":{_safe_name(c)}" for c in fieldnames)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{table_name}"'))
        conn.execute(text(
            f'CREATE TABLE raw."{table_name}" ({col_defs}, '
            f'uploaded_at TIMESTAMPTZ DEFAULT now())'
        ))
        for row in rows:
            data = {_safe_name(k): (v or "").strip() or None for k, v in row.items()}
            conn.execute(text(f'INSERT INTO raw."{table_name}" ({col_list}) VALUES ({placeholders})'), data)
    return {"table_name": table_name, "rows": len(rows), "columns": columns}


@app.post("/api/workflow/build-clean")
def api_build_clean(payload: _BuildCleanPayload) -> dict:
    table = _safe_name(payload.table)
    if not table:
        raise HTTPException(status_code=400, detail="Invalid table name")
    ts         = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    model_name = f"stg_clean_{table}_{ts}"
    pipeline_id = f"clean_{table}_{ts}"
    cols    = [c.model_dump() for c in payload.columns]
    filters = [f.model_dump() for f in payload.filters]
    sql = _generate_clean_sql(table, cols, filters)
    model_dir = _DBT_MODELS_DIR / "acme"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / f"{model_name}.sql").write_text(sql)
    pipeline_yaml = (
        f"pipeline_id: {pipeline_id}\n"
        f"description: Auto-generated cleaning pipeline for raw.{table}\n"
        f"tags: [generated, cleaning, {table}]\n\n"
        f"schedule:\n  preset: \"@daily\"\n  retries: 1\n\n"
        f"stages:\n"
        f"  ingestion:\n    type: jdbc\n    connection_id: acme_postgres\n    table: raw.{table}\n\n"
        f"  transformation:\n    type: dbt\n    select: {model_name}\n\n"
        f"  validation:\n"
        f"    - type: row_count\n      connection_id: postgres_default\n"
        f"      table: analytics.{model_name}\n      min_rows: 1\n"
    )
    _CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    (_CONFIGS_DIR / f"{pipeline_id}.yaml").write_text(pipeline_yaml)
    return {
        "pipeline_id": pipeline_id,
        "model_name":  model_name,
        "airflow_url": f"http://localhost:8080/dags/{pipeline_id}/grid",
        "sql_preview": sql,
    }


@app.post("/api/workflow/build-agg")
def api_build_agg(payload: _BuildAggPayload) -> dict:
    table = _safe_name(payload.table)
    if not table:
        raise HTTPException(status_code=400, detail="Invalid table name")
    ts          = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    model_name  = f"agg_{table}_{ts}"
    pipeline_id = f"agg_{table}_{ts}"
    sql = _generate_agg_sql(
        payload.clean_model, payload.group_by,
        [m.model_dump() for m in payload.metrics],
    )
    model_dir = _DBT_MODELS_DIR / "acme"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / f"{model_name}.sql").write_text(sql)
    pipeline_yaml = (
        f"pipeline_id: {pipeline_id}\n"
        f"description: Auto-generated aggregation pipeline for {payload.clean_model}\n"
        f"tags: [generated, aggregation, {table}]\n\n"
        f"schedule:\n  preset: \"@daily\"\n  retries: 1\n\n"
        f"stages:\n"
        f"  transformation:\n    type: dbt\n    select: {model_name}\n\n"
        f"  validation:\n"
        f"    - type: row_count\n      connection_id: postgres_default\n"
        f"      table: analytics.{model_name}\n      min_rows: 1\n"
    )
    _CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    (_CONFIGS_DIR / f"{pipeline_id}.yaml").write_text(pipeline_yaml)
    return {
        "pipeline_id": pipeline_id,
        "model_name":  model_name,
        "airflow_url": f"http://localhost:8080/dags/{pipeline_id}/grid",
        "sql_preview": sql,
    }


_WORKFLOW_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>DataFabrik — Workflow Wizard</title>'
    '<style>'
    '*{box-sizing:border-box;margin:0;padding:0}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;display:flex;align-items:center;gap:14px}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center}'
    '.logo svg{width:16px;height:16px}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.page{max-width:820px;margin:0 auto;padding:32px 24px}'
    '.stepper{display:flex;align-items:center;margin-bottom:36px}'
    '.si{display:flex;align-items:center;gap:8px;font-size:.8rem;color:#4a5568}'
    '.si.active{color:#63b3ed;font-weight:600}'
    '.si.done{color:#48bb78}'
    '.si-circle{width:24px;height:24px;border-radius:50%;background:#2d3748;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;flex-shrink:0}'
    '.si.active .si-circle{background:#2b4c7e;color:#90cdf4}'
    '.si.done .si-circle{background:#276749;color:#9ae6b4}'
    '.si-sep{flex:1;height:1px;background:#2d3748;margin:0 12px}'
    '.panel{display:none}.panel.active{display:block}'
    '.panel-head{margin-bottom:28px}'
    '.panel-head h2{font-size:1.4rem;font-weight:700;margin-bottom:6px}'
    '.panel-head p{color:#a0aec0;font-size:.9rem;line-height:1.6}'
    '.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:24px;margin-bottom:20px}'
    '.card-label{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#718096;margin-bottom:14px;display:flex;align-items:center;gap:10px}'
    '.drop-zone{border:2px dashed #2d3748;border-radius:10px;padding:40px 24px;text-align:center;cursor:pointer;transition:border-color .2s,background .2s;position:relative}'
    '.drop-zone:hover,.drop-zone.drag{border-color:#3182ce;background:#0d1627}'
    '.drop-zone.done{border-color:#48bb78;background:#0d1f14;border-style:solid}'
    '.drop-zone input{position:absolute;inset:0;opacity:0;cursor:pointer}'
    '.dz-icon{font-size:2rem;margin-bottom:10px;display:block}'
    '.dz-main{font-size:.9rem;color:#a0aec0;margin-bottom:4px}'
    '.dz-sub{font-size:.75rem;color:#4a5568}'
    '.dz-name{font-size:1rem;font-weight:600;color:#68d391}'
    '.field-row{display:flex;align-items:center;gap:10px;margin-top:14px}'
    '.field-label{font-size:.8rem;color:#718096;white-space:nowrap;min-width:80px}'
    '.field-prefix{font-size:.85rem;color:#4a5568}'
    'input[type=text]{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:8px 12px;color:#e2e8f0;font-size:.875rem;outline:none;flex:1;transition:border-color .15s}'
    'input[type=text]:focus{border-color:#3182ce}'
    '.source-bar{background:#151a27;border-radius:8px;padding:10px 16px;font-size:.82rem;color:#718096;margin-bottom:20px}'
    '.source-bar b{color:#e2e8f0}'
    '.section-title{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#718096;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between}'
    '.col-wrap{overflow-x:auto}'
    '.col-tbl{width:100%;border-collapse:collapse;font-size:.8rem}'
    '.col-tbl th{background:#232a3b;color:#4a5568;padding:7px 10px;text-align:left;border-bottom:1px solid #2d3748;white-space:nowrap;font-weight:600}'
    '.col-tbl td{padding:6px 10px;border-bottom:1px solid #151a27;vertical-align:middle}'
    '.col-tbl tr:last-child td{border-bottom:none}'
    '.col-tbl input[type=checkbox]{width:15px;height:15px;cursor:pointer;accent-color:#3182ce}'
    '.col-out{width:130px;background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:4px 8px;color:#e2e8f0;font-size:.78rem}'
    '.col-out:focus{border-color:#3182ce;outline:none}'
    'select.col-type{background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:4px 8px;color:#e2e8f0;font-size:.78rem;cursor:pointer}'
    '.col-sample{color:#4a5568;font-size:.72rem;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    '.filter-list{display:flex;flex-direction:column;gap:8px}'
    '.filter-row{display:flex;align-items:center;gap:8px}'
    'select.filter-col,select.filter-op{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:7px 10px;color:#e2e8f0;font-size:.8rem;cursor:pointer}'
    'select.filter-col{min-width:130px}'
    'select.filter-op{min-width:110px}'
    '.filter-val{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:7px 10px;color:#e2e8f0;font-size:.8rem;flex:1}'
    '.filter-val:focus{border-color:#3182ce;outline:none}'
    '.filter-rm{background:none;border:none;color:#4a5568;cursor:pointer;font-size:1.1rem;padding:0 4px;line-height:1}'
    '.filter-rm:hover{color:#fc8181}'
    '.no-filters{font-size:.8rem;color:#4a5568;padding:10px 0}'
    '.btn-link{background:none;border:none;color:#63b3ed;cursor:pointer;font-size:.78rem;padding:0}'
    '.btn-link:hover{text-decoration:underline}'
    '.panel-footer{display:flex;align-items:center;gap:12px;margin-top:8px}'
    '.btn{display:inline-flex;align-items:center;gap:6px;padding:10px 22px;border-radius:8px;font-size:.875rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s;text-decoration:none}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}'
    '.btn-ghost{background:#2d3748;color:#e2e8f0}.btn-ghost:hover{background:#3a4459}'
    '.spin-wrap{display:flex;align-items:center;gap:8px;font-size:.85rem;color:#718096}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#63b3ed;border-radius:50%;animation:spin .6s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.err-msg{color:#fc8181;font-size:.85rem}'
    '.success-block{text-align:center;padding:32px 0 24px}'
    '.success-block .s-icon{font-size:3rem;margin-bottom:16px;display:block}'
    '.success-block h2{font-size:1.4rem;font-weight:700;margin-bottom:8px}'
    '.success-block .pid{font-family:monospace;font-size:.85rem;color:#63b3ed;background:#0d1627;'
    'padding:6px 14px;border-radius:6px;display:inline-block;margin-bottom:20px}'
    '.sql-block{background:#0d1117;border:1px solid #2d3748;border-radius:8px;padding:16px;'
    'font-family:"JetBrains Mono","Fira Code",monospace;font-size:.75rem;color:#a0aec0;'
    'white-space:pre;overflow-x:auto;line-height:1.6;max-height:300px;overflow-y:auto}'
    '.chk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}'
    '.chk-item{display:flex;align-items:center;gap:8px;background:#0d1117;border:1px solid #2d3748;'
    'border-radius:6px;padding:8px 12px;font-size:.8rem;cursor:pointer;transition:border-color .2s}'
    '.chk-item:hover{border-color:#4a5568}.chk-item.sel{border-color:#3182ce;background:rgba(49,130,206,.08)}'
    '.chk-item input{width:14px;height:14px;cursor:pointer;accent-color:#3182ce}'
    '.metric-row{display:grid;grid-template-columns:1fr 90px 1fr 28px;gap:8px;align-items:center;margin-bottom:8px}'
    '.metric-row select,.metric-row input[type=text]{background:#0d1117;border:1px solid #2d3748;'
    'border-radius:5px;padding:6px 9px;color:#e2e8f0;font-size:.79rem;outline:none}'
    '.metric-row select:focus,.metric-row input[type=text]:focus{border-color:#3182ce}'
    'input[type=date]{background:#0d1117;border:1px solid #2d3748;border-radius:6px;'
    'padding:7px 10px;color:#e2e8f0;font-size:.8rem;outline:none}'
    'input[type=date]:focus{border-color:#3182ce}'
    '.date-row{display:grid;grid-template-columns:1.5fr 1fr 1fr;gap:12px;align-items:end}'
    '.date-label{font-size:.78rem;color:#718096;margin-bottom:5px}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16" fill="none">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik — Workflow Wizard</h1>'
    '<a href="/">← Back to Portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="stepper">'
    '<div class="si active" id="si1"><div class="si-circle">1</div>Upload CSV</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si2"><div class="si-circle">2</div>Configure Cleaning</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si3"><div class="si-circle">3</div>Build Cleaning</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si4"><div class="si-circle">4</div>Configure Aggregation</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si5"><div class="si-circle">5</div>Build Aggregation</div>'
    '</div>'
    '<div id="p1" class="panel active">'
    '<div class="panel-head"><h2>Upload your data</h2>'
    '<p>Start by uploading a CSV file. Columns and types will be detected automatically.</p></div>'
    '<div class="card">'
    '<div class="drop-zone" id="dz">'
    '<input type="file" id="fi" accept=".csv" onchange="onFile(this)">'
    '<div id="dz-idle"><span class="dz-icon">📂</span>'
    '<div class="dz-main">Drop a CSV here, or click to browse</div>'
    '<div class="dz-sub">.csv files only</div></div>'
    '<div id="dz-done" style="display:none"><span class="dz-icon">📄</span>'
    '<div class="dz-name" id="dz-fname"></div>'
    '<div class="dz-sub" id="dz-fsize"></div></div>'
    '</div>'
    '<div class="field-row">'
    '<span class="field-label">Table name</span>'
    '<span class="field-prefix">raw.</span>'
    '<input type="text" id="tbl-inp" placeholder="customers" oninput="checkStep1()">'
    '</div>'
    '</div>'
    '<div class="panel-footer">'
    '<button class="btn btn-primary" id="s1-btn" onclick="doUpload()" disabled>Continue →</button>'
    '<div id="s1-status"></div>'
    '</div>'
    '</div>'
    '<div id="p2" class="panel">'
    '<div class="panel-head"><h2>Configure cleaning</h2>'
    '<p>Choose which columns to keep, set their output types, and add row filters.</p></div>'
    '<div class="source-bar" id="src-bar"></div>'
    '<div class="card">'
    '<div class="section-title">Columns</div>'
    '<div class="col-wrap"><table class="col-tbl">'
    '<thead><tr>'
    '<th></th><th>Source column</th><th>Output name</th><th>Type</th><th>Sample values</th>'
    '</tr></thead>'
    '<tbody id="col-tbody"></tbody>'
    '</table></div>'
    '</div>'
    '<div class="card">'
    '<div class="section-title">Filters '
    '<button class="btn-link" onclick="addFilter()">+ Add filter</button>'
    '</div>'
    '<div class="filter-list" id="filter-list"></div>'
    '<div class="no-filters" id="no-filters">No filters — all rows will be included</div>'
    '</div>'
    '<div class="panel-footer">'
    '<button class="btn btn-ghost" onclick="goStep(1)">← Back</button>'
    '<button class="btn btn-primary" id="s2-btn" onclick="doBuild()">Build Cleaning Pipeline →</button>'
    '<div id="s2-status"></div>'
    '</div>'
    '</div>'
    '<div id="p3" class="panel">'
    '<div class="card">'
    '<div class="success-block">'
    '<span class="s-icon">✅</span>'
    '<h2>Cleaning Pipeline Created</h2>'
    '<div class="pid" id="p3-pid"></div>'
    '<div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-bottom:8px">'
    '<button class="btn btn-primary" id="run-btn" onclick="runPipeline()">▶ Run Now</button>'
    '<a class="btn btn-ghost" id="p3-link" href="#" target="_blank">Open in Airflow ↗</a>'
    '</div>'
    '<div id="run-status" style="font-size:.85rem;margin-top:6px"></div>'
    '</div>'
    '</div>'
    '<div class="card">'
    '<div class="section-title">Generated dbt SQL</div>'
    '<div class="sql-block" id="p3-sql"></div>'
    '</div>'
    '<div class="panel-footer" style="justify-content:center;padding-top:16px;border-top:1px solid #2d3748;margin-top:8px">'
    '<button class="btn btn-ghost" onclick="goStep(2)">&#8592; Back</button>'
    '<button class="btn btn-primary" onclick="renderStep4();goStep(4)">Continue to Aggregation &#8594;</button>'
    '</div>'
    '</div>'
    '<div id="p4" class="panel">'
    '<div class="panel-head"><h2>Configure Aggregation</h2>'
    '<p>Choose columns to group by, then define the metrics you want to compute.</p></div>'
    '<div class="card">'
    '<div class="section-title">Group By Columns</div>'
    '<div class="chk-grid" id="gb-grid"></div>'
    '</div>'
    '<div class="card">'
    '<div class="section-title">Metrics '
    '<button class="btn-link" onclick="addMetric()">+ Add metric</button>'
    '</div>'
    '<div id="metric-list"></div>'
    '<div id="no-metrics" style="font-size:.8rem;color:#4a5568;padding:10px 0">No metrics — add at least one</div>'
    '</div>'
    '<div class="panel-footer">'
    '<button class="btn btn-ghost" onclick="goStep(3)">&#8592; Back</button>'
    '<button class="btn btn-primary" id="s4-btn" onclick="doBuildAgg()">Build Aggregation Pipeline &#8594;</button>'
    '<div id="s4-status"></div>'
    '</div>'
    '</div>'
    '<div id="p5" class="panel">'
    '<div class="card">'
    '<div class="success-block">'
    '<span class="s-icon">&#127881;</span>'
    '<h2>All Pipelines Created!</h2>'
    '<div class="pid" id="p5-pid"></div>'
    '<div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-bottom:8px">'
    '<a class="btn btn-ghost" id="p5-clean-link" href="#" target="_blank">Cleaning DAG &#8599;</a>'
    '<a class="btn btn-primary" id="p5-agg-link" href="#" target="_blank">Aggregation DAG &#8599;</a>'
    '</div>'
    '</div>'
    '</div>'
    '<div class="card">'
    '<div class="section-title">Aggregation dbt SQL</div>'
    '<div class="sql-block" id="p5-sql"></div>'
    '</div>'
    '</div>'
    '</div>'
    '<script>'
    'const TYPES=["TEXT","INTEGER","NUMERIC","DATE","TIMESTAMP","BOOLEAN"];'
    'const OPS=[["=","equals"],["!=","not equals"],[">=","≥"],["<=","≤"],["LIKE","contains"],["IS NULL","is empty"],["IS NOT NULL","is not empty"]];'
    'let state={table:"",columns:[],file:null};'
    'let filterSeq=0;'
    'const dz=document.getElementById("dz");'
    'dz.addEventListener("dragover",e=>{e.preventDefault();dz.classList.add("drag")});'
    'dz.addEventListener("dragleave",()=>dz.classList.remove("drag"));'
    'dz.addEventListener("drop",e=>{e.preventDefault();dz.classList.remove("drag");'
    'const f=e.dataTransfer.files[0];if(f&&f.name.toLowerCase().endsWith(".csv"))applyFile(f);});'
    'function onFile(inp){if(inp.files[0])applyFile(inp.files[0]);}'
    'function applyFile(f){'
    'state.file=f;'
    'document.getElementById("dz-idle").style.display="none";'
    'document.getElementById("dz-done").style.display="";'
    'document.getElementById("dz-fname").textContent=f.name;'
    'document.getElementById("dz-fsize").textContent=(f.size/1024).toFixed(1)+" KB";'
    'dz.classList.add("done");'
    'const inp=document.getElementById("tbl-inp");'
    'inp.value=f.name.replace(/\\.csv$/i,"").replace(/[^a-z0-9]+/gi,"_").toLowerCase();'
    'checkStep1();}'
    'function checkStep1(){'
    'const t=document.getElementById("tbl-inp").value.trim();'
    'document.getElementById("s1-btn").disabled=!(state.file&&t);}'
    'async function doUpload(){'
    'const t=document.getElementById("tbl-inp").value.trim();'
    'const btn=document.getElementById("s1-btn");'
    'const st=document.getElementById("s1-status");'
    'btn.disabled=true;'
    'st.innerHTML="<div class=\\"spin-wrap\\"><div class=\\"spinner\\"></div>&nbsp;Uploading…</div>";'
    'const fd=new FormData();fd.append("table",t);fd.append("file",state.file);'
    'try{'
    'const r=await fetch("/api/workflow/upload",{method:"POST",body:fd});'
    'const j=await r.json();'
    'if(r.ok){state.table=j.table_name;renderStep2(j);}else{'
    'st.innerHTML=`<span class="err-msg">${j.detail||"Upload failed"}</span>`;btn.disabled=false;}'
    '}catch(e){st.innerHTML="<span class=\\"err-msg\\">Network error</span>";btn.disabled=false;}}'
    'function renderStep2(data){'
    'state.columns=data.columns;'
    'document.getElementById("src-bar").innerHTML='
    '`<b>raw.${state.table}</b> &nbsp;·&nbsp; ${data.rows} rows &nbsp;·&nbsp; ${data.columns.length} columns`;'
    'const tbody=document.getElementById("col-tbody");'
    'tbody.innerHTML=data.columns.map((c,i)=>'
    '`<tr>'
    '<td><input type="checkbox" class="col-check" checked data-i="${i}"></td>'
    '<td style="color:#718096;font-size:.78rem">${c.name}</td>'
    '<td><input type="text" class="col-out" value="${c.name}" data-i="${i}"></td>'
    '<td><select class="col-type" data-i="${i}">${TYPES.map(t=>`<option${t===c.type?" selected":""}>${t}</option>`).join("")}</select></td>'
    '<td class="col-sample">${c.samples.join(", ")}</td>'
    '</tr>`'
    ').join("");'
    'goStep(2);}'
    'function goStep(n){'
    'document.querySelectorAll(".panel").forEach(p=>p.classList.remove("active"));'
    'document.getElementById(`p${n}`).classList.add("active");'
    '["si1","si2","si3","si4","si5"].forEach((id,i)=>{'
    'const el=document.getElementById(id);'
    'if(el)el.className="si"+(i+1<n?" done":i+1===n?" active":"");});}'
    'function addFilter(){'
    'const id=++filterSeq;'
    'const cols=state.columns.map(c=>c.name);'
    'const row=document.createElement("div");'
    'row.className="filter-row";row.id=`fr${id}`;'
    'row.innerHTML='
    '`<select class="filter-col">${cols.map(n=>`<option>${n}</option>`).join("")}</select>'
    '<select class="filter-op" onchange="toggleVal(this)">${OPS.map(([v,l])=>`<option value="${v}">${l}</option>`).join("")}</select>'
    '<input type="text" class="filter-val" placeholder="value">'
    '<button class="filter-rm" onclick="this.parentElement.remove();syncFilters()">×</button>`;'
    'document.getElementById("filter-list").appendChild(row);'
    'document.getElementById("no-filters").style.display="none";}'
    'function toggleVal(sel){'
    'const val=sel.parentElement.querySelector(".filter-val");'
    'val.style.display=["IS NULL","IS NOT NULL"].includes(sel.value)?"none":"";}'
    'function syncFilters(){'
    'const rows=document.querySelectorAll(".filter-row");'
    'document.getElementById("no-filters").style.display=rows.length?"none":"";}'
    'async function doBuild(){'
    'const cols=[];'
    'document.querySelectorAll("#col-tbody tr").forEach((tr,i)=>{'
    'cols.push({name:state.columns[i].name,'
    'output_name:tr.querySelector(".col-out").value.trim(),'
    'type:tr.querySelector(".col-type").value,'
    'include:tr.querySelector(".col-check").checked});});'
    'const filters=[];'
    'document.querySelectorAll(".filter-row").forEach(row=>{'
    'const op=row.querySelector(".filter-op").value;'
    'const val=row.querySelector(".filter-val");'
    'filters.push({column:row.querySelector(".filter-col").value,operator:op,value:val?val.value.trim():""});});'
    'const btn=document.getElementById("s2-btn");'
    'const st=document.getElementById("s2-status");'
    'btn.disabled=true;'
    'st.innerHTML="<div class=\\"spin-wrap\\"><div class=\\"spinner\\"></div>&nbsp;Building pipeline…</div>";'
    'try{'
    'const r=await fetch("/api/workflow/build-clean",{method:"POST",'
    'headers:{"Content-Type":"application/json"},'
    'body:JSON.stringify({table:state.table,columns:cols,filters})});'
    'const j=await r.json();'
    'if(r.ok){'
    'document.getElementById("p3-pid").textContent=j.pipeline_id;'
    'document.getElementById("p3-link").href=j.airflow_url;'
    'document.getElementById("p3-sql").textContent=j.sql_preview;'
    'state.pipeline_id=j.pipeline_id;'
    'state.cleanModel=j.model_name;'
    'state.cleanAirflow=j.airflow_url;'
    'state.cleanedCols=cols.filter(c=>c.include);'
    'goStep(3);'
    '}else{st.innerHTML=`<span class="err-msg">${j.detail||"Build failed"}</span>`;btn.disabled=false;}'
    '}catch(e){st.innerHTML="<span class=\\"err-msg\\">Network error</span>";btn.disabled=false;}}'
    'async function runPipeline(){'
    'const btn=document.getElementById("run-btn");'
    'const st=document.getElementById("run-status");'
    'const pid=state.pipeline_id;'
    'btn.disabled=true;'
    'st.innerHTML="<span style=\\"color:#718096\\">⏳ Waiting for Airflow to register DAG…</span>";'
    'let found=false;'
    'for(let i=0;i<12;i++){'
    'await new Promise(r=>setTimeout(r,5000));'
    'try{'
    'const r=await fetch("/api/pipelines");'
    'const j=await r.json();'
    'const dags=Array.isArray(j)?j:(j.dags||[]);'
    'if(dags.some(d=>(d.dag_id||d.pipeline_id||d.id)===pid)){found=true;break;}'
    '}catch(_){}'
    'st.innerHTML=`<span style="color:#718096">⏳ Waiting for Airflow… (${(i+1)*5}s)</span>`;}'
    'if(!found){'
    'st.innerHTML="<span style=\\"color:#fc8181\\">DAG not detected after 60s — try opening Airflow manually.</span>";'
    'btn.disabled=false;return;}'
    'st.innerHTML="<span style=\\"color:#718096\\">🚀 Triggering run…</span>";'
    'try{'
    'const r=await fetch(`/api/pipelines/${pid}/trigger`,{method:"POST"});'
    'if(r.ok){'
    'st.innerHTML="<span style=\\"color:#68d391\\">✓ Pipeline triggered! Check Airflow for progress.</span>";'
    '}else{'
    'const j=await r.json();'
    'st.innerHTML=`<span style="color:#fc8181">Trigger failed: ${j.detail||r.status}</span>`;'
    'btn.disabled=false;}}'
    'catch(e){st.innerHTML="<span style=\\"color:#fc8181\\">Network error triggering run.</span>";btn.disabled=false;}}'
    'const FNS=["SUM","COUNT","AVG","MIN","MAX"];'
    'let metricSeq=0;'
    'function renderStep4(){'
    'const gb=document.getElementById("gb-grid");'
    'gb.innerHTML=state.cleanedCols.map(c=>'
    '`<label class="chk-item"><input type="checkbox" data-col="${c.output_name||c.name}">&nbsp;${c.output_name||c.name}</label>`'
    ').join("");'
    'document.getElementById("metric-list").innerHTML="";'
    'document.getElementById("no-metrics").style.display="";'
    'metricSeq=0;}'
    'function addMetric(){'
    'const id=++metricSeq;'
    'const cols=state.cleanedCols.map(c=>c.output_name||c.name);'
    'const row=document.createElement("div");'
    'row.className="metric-row";row.id=`mr${id}`;'
    'row.innerHTML='
    '`<select>${cols.map(c=>`<option>${c}</option>`).join("")}</select>`+'
    '`<select>${FNS.map(f=>`<option>${f}</option>`).join("")}</select>`+'
    '`<input type="text" placeholder="output name (optional)">`+'
    '`<button style="background:none;border:none;color:#4a5568;cursor:pointer;font-size:1.1rem" onclick="this.parentElement.remove();syncMetrics()">&#215;</button>`;'
    'document.getElementById("metric-list").appendChild(row);'
    'document.getElementById("no-metrics").style.display="none";}'
    'function syncMetrics(){'
    'document.getElementById("no-metrics").style.display=document.querySelectorAll(".metric-row").length?"none":"";}'
    'async function doBuildAgg(){'
    'const gb=[];'
    'document.querySelectorAll("#gb-grid input:checked").forEach(inp=>gb.push(inp.dataset.col));'
    'const mx=[];'
    'document.querySelectorAll(".metric-row").forEach(row=>{'
    'const sels=row.querySelectorAll("select");'
    'mx.push({column:sels[0].value,fn:sels[1].value,output_name:row.querySelector("input[type=text]").value.trim()});});'
    'if(!mx.length){document.getElementById("s4-status").innerHTML="<span class=\\"err-msg\\">Add at least one metric.</span>";return;}'
    'const btn=document.getElementById("s4-btn");'
    'const st=document.getElementById("s4-status");'
    'btn.disabled=true;'
    'st.innerHTML="<div class=\\"spin-wrap\\"><div class=\\"spinner\\"></div>&nbsp;Building aggregation pipeline…</div>";'
    'try{'
    'const r=await fetch("/api/workflow/build-agg",{method:"POST",headers:{"Content-Type":"application/json"},'
    'body:JSON.stringify({table:state.table,clean_model:state.cleanModel,group_by:gb,metrics:mx})});'
    'const j=await r.json();'
    'if(r.ok){'
    'document.getElementById("p5-pid").textContent=j.pipeline_id;'
    'document.getElementById("p5-clean-link").href=state.cleanAirflow;'
    'document.getElementById("p5-agg-link").href=j.airflow_url;'
    'document.getElementById("p5-sql").textContent=j.sql_preview;'
    'goStep(5);'
    '}else{st.innerHTML=`<span class="err-msg">${j.detail||"Build failed"}</span>`;btn.disabled=false;}'
    '}catch(e){st.innerHTML="<span class=\\"err-msg\\">Network error</span>";btn.disabled=false;}}'
    '</script>'
    '<script>if(location.search.includes("embed=1"))document.querySelectorAll(\'a[href="/"]\').forEach(e=>e.style.display="none");</script>'
    '</body></html>'
)


@app.get("/workflow", response_class=HTMLResponse)
def workflow_page() -> str:
    """Data cleaning pipeline wizard."""
    return _WORKFLOW_HTML


_UPLOAD_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>DataFabrik — Upload Data</title>'
    '<style>'
    '*{box-sizing:border-box;margin:0;padding:0}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;display:flex;align-items:center;gap:14px}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center}'
    '.logo svg{width:16px;height:16px}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.topbar a:hover{text-decoration:underline}'
    '.page{max-width:680px;margin:0 auto;padding:40px 24px}'
    '.hero{margin-bottom:32px}'
    '.hero h2{font-size:1.5rem;font-weight:700;margin-bottom:8px}'
    '.hero p{color:#a0aec0;font-size:.9rem;line-height:1.6}'
    '.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:24px;margin-bottom:20px}'
    '.card-label{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#718096;margin-bottom:12px}'
    '.drop-zone{border:2px dashed #2d3748;border-radius:10px;padding:48px 24px;text-align:center;'
    'cursor:pointer;transition:border-color .2s,background .2s;position:relative}'
    '.drop-zone:hover,.drop-zone.drag{border-color:#3182ce;background:#0d1627}'
    '.drop-zone.done{border-color:#48bb78;background:#0d1f14;border-style:solid}'
    '.drop-zone input{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}'
    '.dz-icon{font-size:2.2rem;margin-bottom:12px;display:block}'
    '.dz-main{font-size:.95rem;color:#a0aec0;margin-bottom:4px}'
    '.dz-sub{font-size:.78rem;color:#4a5568}'
    '.dz-name{font-size:1rem;font-weight:600;color:#68d391}'
    '.field-row{display:flex;align-items:center;gap:12px;margin-top:16px}'
    '.field-label{font-size:.8rem;color:#718096;white-space:nowrap;min-width:90px}'
    '.field-prefix{font-size:.85rem;color:#4a5568}'
    'input[type=text]{background:#0d1117;border:1px solid #2d3748;border-radius:6px;'
    'padding:8px 12px;color:#e2e8f0;font-size:.875rem;outline:none;flex:1;'
    'transition:border-color .15s}'
    'input[type=text]:focus{border-color:#3182ce}'
    '.preview-wrap{overflow-x:auto}'
    '.preview-tbl{width:100%;border-collapse:collapse;font-size:.75rem}'
    '.preview-tbl th{background:#232a3b;color:#718096;padding:6px 10px;text-align:left;'
    'border-bottom:1px solid #2d3748;white-space:nowrap}'
    '.preview-tbl td{padding:5px 10px;border-bottom:1px solid #1a1f2e;color:#a0aec0;white-space:nowrap}'
    '.preview-tbl tr:last-child td{border-bottom:none}'
    '.row-count{font-size:.75rem;color:#4a5568;margin-top:8px}'
    '.actions{display:flex;align-items:center;gap:16px}'
    '.btn{display:inline-flex;align-items:center;gap:6px;padding:11px 28px;border-radius:8px;'
    'font-size:.875rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}'
    '.spin-wrap{display:flex;align-items:center;gap:8px;font-size:.85rem;color:#718096}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#63b3ed;'
    'border-radius:50%;animation:spin .6s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.result-ok{color:#68d391;font-weight:600;font-size:.9rem}'
    '.result-err{color:#fc8181;font-size:.9rem}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16" fill="none">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik — Upload Data</h1>'
    '<a href="/">← Back to Portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="hero">'
    '<h2>Upload a CSV file</h2>'
    '<p>Drop any CSV — columns are auto-detected from the header row. '
    'The file is loaded into the <code style="color:#63b3ed;font-size:.85em">raw</code> schema '
    'and replaces any existing data in that table.</p>'
    '</div>'
    '<div class="card">'
    '<div class="card-label">1 &nbsp; Choose your file</div>'
    '<div class="drop-zone" id="dz">'
    '<input type="file" id="fi" accept=".csv" onchange="onFile(this)">'
    '<div id="dz-idle">'
    '<span class="dz-icon">📂</span>'
    '<div class="dz-main">Drop a CSV file here, or click to browse</div>'
    '<div class="dz-sub">.csv files only</div>'
    '</div>'
    '<div id="dz-done" style="display:none">'
    '<span class="dz-icon">✅</span>'
    '<div class="dz-name" id="dz-fname"></div>'
    '<div class="dz-sub" id="dz-fsize"></div>'
    '</div>'
    '</div>'
    '<div class="field-row">'
    '<span class="field-label">Table name</span>'
    '<span class="field-prefix">raw.</span>'
    '<input type="text" id="tbl-input" placeholder="my_table" oninput="checkReady()">'
    '</div>'
    '</div>'
    '<div class="card" id="preview-card" style="display:none">'
    '<div class="card-label">2 &nbsp; Preview <span style="font-weight:400;text-transform:none;color:#4a5568">&nbsp;first 5 rows</span></div>'
    '<div class="preview-wrap"><div id="preview-body"></div></div>'
    '<div class="row-count" id="row-count"></div>'
    '</div>'
    '<div class="actions">'
    '<button class="btn btn-primary" id="up-btn" onclick="doUpload()" disabled>Load into DataFabrik</button>'
    '<div id="status"></div>'
    '</div>'
    '</div>'
    '<script>'
    'let file=null,totalRows=0;'
    'const dz=document.getElementById("dz");'
    'dz.addEventListener("dragover",e=>{e.preventDefault();dz.classList.add("drag")});'
    'dz.addEventListener("dragleave",()=>dz.classList.remove("drag"));'
    'dz.addEventListener("drop",e=>{e.preventDefault();dz.classList.remove("drag");'
    'const f=e.dataTransfer.files[0];if(f&&f.name.endsWith(".csv")){applyFile(f);}});'
    'function onFile(inp){if(inp.files[0])applyFile(inp.files[0]);}'
    'function applyFile(f){'
    'file=f;'
    'document.getElementById("dz-idle").style.display="none";'
    'document.getElementById("dz-done").style.display="";'
    'document.getElementById("dz-fname").textContent=f.name;'
    'document.getElementById("dz-fsize").textContent=(f.size/1024).toFixed(1)+" KB";'
    'dz.classList.add("done");'
    'const stem=f.name.replace(/\\.csv$/i,"").replace(/[^a-z0-9]+/gi,"_").toLowerCase();'
    'const inp=document.getElementById("tbl-input");'
    'if(!inp.value)inp.value=stem;'
    'preview();checkReady();}'
    'function checkReady(){'
    'const t=document.getElementById("tbl-input").value.trim();'
    'document.getElementById("up-btn").disabled=!(file&&t);}'
    'async function preview(){'
    'const txt=await file.text();'
    'const lines=txt.trim().split("\\n");'
    'totalRows=lines.length-1;'
    'const preview=lines.slice(0,6);'
    'const hdrs=preview[0].split(",");'
    'let h=\'<table class="preview-tbl"><thead><tr>\';'
    'hdrs.forEach(c=>h+=\'<th>\'+c.trim()+\'</th>\');'
    'h+=\'</tr></thead><tbody>\';'
    'preview.slice(1).forEach(ln=>{'
    'h+=\'<tr>\';ln.split(",").forEach(v=>h+=\'<td>\'+v.trim()+\'</td>\');h+=\'</tr>\';});'
    'h+=\'</tbody></table>\';'
    'document.getElementById("preview-body").innerHTML=h;'
    'document.getElementById("row-count").textContent=totalRows+" rows total · "+hdrs.length+" columns";'
    'document.getElementById("preview-card").style.display="";}'
    'async function doUpload(){'
    'const tbl=document.getElementById("tbl-input").value.trim();'
    'const btn=document.getElementById("up-btn");'
    'const st=document.getElementById("status");'
    'btn.disabled=true;'
    'st.innerHTML=\'<div class="spin-wrap"><div class="spinner"></div>&nbsp;Loading…</div>\';'
    'const fd=new FormData();fd.append("table",tbl);fd.append("file",file);'
    'try{'
    'const r=await fetch("/api/upload/csv",{method:"POST",body:fd});'
    'const j=await r.json();'
    'if(r.ok){'
    'st.innerHTML=\'<span class="result-ok">✓ \'+j.rows_loaded+\' rows → raw.\'+j.table_name+\'</span>\';'
    '}else{'
    'st.innerHTML=\'<span class="result-err">✗ \'+(j.detail||"Upload failed")+\'</span>\';'
    'btn.disabled=false;}'
    '}catch(e){'
    'st.innerHTML=\'<span class="result-err">✗ Network error</span>\';'
    'btn.disabled=false;}}'
    '</script>'
    '<script>if(location.search.includes("embed=1"))document.querySelectorAll(\'a[href="/"]\').forEach(e=>e.style.display="none");</script>'
    '</body></html>'
)


@app.post("/api/upload/csv")
async def api_upload_csv(table: str = Form(...), file: UploadFile = File(...)) -> dict:
    table_name = _safe_name(table)
    if not table_name:
        raise HTTPException(status_code=400, detail="Invalid table name")
    content = await file.read()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data rows")
    cols = [_safe_name(c) for c in (reader.fieldnames or [])]
    if not cols:
        raise HTTPException(status_code=400, detail="Could not detect column headers")
    col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{table_name}"'))
        conn.execute(text(
            f'CREATE TABLE raw."{table_name}" ({col_defs}, '
            f'uploaded_at TIMESTAMPTZ DEFAULT now())'
        ))
        for row in rows:
            data = {_safe_name(k): (v or "").strip() or None for k, v in row.items()}
            conn.execute(text(f'INSERT INTO raw."{table_name}" ({col_list}) VALUES ({placeholders})'), data)
    return {"table_name": table_name, "rows_loaded": len(rows), "columns": cols}


@app.get("/upload", response_class=HTMLResponse)
def upload_page() -> str:
    """Demo data upload UI."""
    return _UPLOAD_HTML


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
