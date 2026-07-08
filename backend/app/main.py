"""DataFabrik FastAPI backend — pipeline health dashboard and metadata API."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import csv
import io
import re

import boto3
import requests as _requests
import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text

app = FastAPI(title="DataFabrik API", version="0.1.0")

DATABASE_URL  = os.environ["DATABASE_URL"]
AIRFLOW_URL   = os.environ.get("AIRFLOW_URL",      "http://airflow-webserver:8080")
AIRFLOW_USER  = os.environ.get("AIRFLOW_USER",     "admin")
AIRFLOW_PASS  = os.environ.get("AIRFLOW_PASSWORD", "admin")
S3_ENDPOINT   = os.environ.get("S3_ENDPOINT_URL",  "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID",     "minioadmin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def _latest_wizard_file(table_name: str) -> tuple[str, str] | None:
    """Return (key, filename) of the most recent wizard upload for this table, or None."""
    try:
        resp = _s3_client().list_objects_v2(
            Bucket="datafabrik-raw", Prefix=f"wizard/{table_name}/"
        )
        objects = resp.get("Contents", [])
        if not objects:
            return None
        latest = max(objects, key=lambda o: o["LastModified"])
        key = latest["Key"]
        return key, key.rsplit("/", 1)[-1]
    except Exception:
        return None

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


@app.patch("/api/pipelines/{dag_id}/pause")
def api_toggle_pause(dag_id: str, paused: bool) -> dict:
    """Pause or unpause an Airflow DAG."""
    resp = _requests.patch(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        json={"is_paused": paused},
        timeout=10,
    )
    if not resp.ok:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"dag_id": dag_id, "is_paused": paused}


@app.delete("/api/pipelines/{dag_id}/dag")
def api_delete_dag(dag_id: str) -> dict:
    """Delete a DAG from Airflow and remove any local config files if they exist."""
    resp = _requests.delete(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=10,
    )
    deleted_files = []
    for ext in (".yaml", ".sql"):
        p = _CONFIGS_DIR / f"{dag_id}{ext}"
        if p.exists():
            p.unlink()
            deleted_files.append(p.name)
    if not resp.ok and resp.status_code != 404:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"dag_id": dag_id, "deleted_files": deleted_files}


@app.post("/api/pipelines/{dag_id}/trigger")
def api_trigger_pipeline(dag_id: str) -> dict:
    """Unpause then trigger an Airflow DAG run, retrying while DagBag loads."""
    import time as _time
    # DAGs start paused — unpause before triggering (ignore errors, DAG may not exist yet)
    _requests.patch(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        json={"is_paused": False},
        timeout=10,
    )
    # Retry up to 15 times (75 s) while the scheduler re-scans the DAG file
    last: dict = {}
    for _ in range(15):
        resp = _requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            json={},
            timeout=10,
        )
        if resp.ok:
            return resp.json()
        try:
            last = resp.json()
        except Exception:
            last = {"status": resp.status_code}
        if resp.status_code == 404 or any(kw in str(last) for kw in ("DagBag", "not found", "does not exist")):
            _time.sleep(5)
            continue
        raise HTTPException(status_code=resp.status_code, detail=str(last))
    raise HTTPException(status_code=503, detail=f"DAG not loaded after retries: {last}")


# ── Admin API ──────────────────────────────────────────────────────────────────

_RESTART_CONTAINERS = [
    "datafabrik-airflow-webserver",
    "datafabrik-airflow-scheduler",
]

_HEALTH_CHECKS: dict[str, str] = {
    "airflow":  f"{AIRFLOW_URL}/health",
    "minio":    "http://minio:9000/minio/health/live",
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
/* ── Pipeline guide diagram ── */
.guide-wrap{display:flex;align-items:center;justify-content:center;flex:1;padding:40px 32px}
.guide-flow{display:flex;align-items:flex-start;gap:0}
.guide-node{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:20px 16px;width:160px;flex-shrink:0;position:relative}
.guide-node-num{position:absolute;top:-10px;left:50%;transform:translateX(-50%);background:#0f1117;border:1px solid #2d3748;border-radius:20px;padding:1px 10px;font-size:.65rem;font-weight:700;color:#4a5568;white-space:nowrap}
.guide-node-icon{font-size:2rem;text-align:center;margin:6px 0 10px}
.guide-node-title{font-size:.85rem;font-weight:700;text-align:center;margin-bottom:6px}
.guide-node-desc{font-size:.72rem;color:#718096;text-align:center;line-height:1.5}
.guide-node-tags{display:flex;flex-wrap:wrap;justify-content:center;gap:4px;margin-top:8px}
.guide-tag{background:#1e2535;border:1px solid #2d3748;border-radius:4px;padding:2px 7px;font-size:.63rem;color:#4a5568;font-family:monospace}
.guide-node.n1{border-color:#2b4c7e}.guide-node.n1 .guide-node-title{color:#90cdf4}
.guide-node.n2{border-color:#276749}.guide-node.n2 .guide-node-title{color:#68d391}
.guide-node.n3{border-color:#744210}.guide-node.n3 .guide-node-title{color:#f6ad55}
.guide-node.n4{border-color:#553c9a}.guide-node.n4 .guide-node-title{color:#b794f4}
.guide-arrow{display:flex;flex-direction:column;align-items:center;justify-content:center;width:48px;flex-shrink:0;padding-top:54px}
.guide-arrow-line{width:100%;height:2px;background:linear-gradient(90deg,#2d3748,#4a5568)}
.guide-arrow-head{width:0;height:0;border-top:5px solid transparent;border-bottom:5px solid transparent;border-left:7px solid #4a5568;margin-left:-1px}
.guide-arrow-label{font-size:.6rem;color:#4a5568;margin-top:6px;white-space:nowrap;text-align:center}
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
    <div class="sb-section">Tools</div>
    <button class="nav-btn" onclick="nav('airflow')" id="nav-airflow">
      <span class="icon">✈️</span> Airflow
    </button>
    <button class="nav-btn" onclick="nav('minio')" id="nav-minio">
      <span class="icon">🗄️</span> MinIO
    </button>
    <div class="sb-section">Build</div>
    <button class="nav-btn" onclick="nav('workflow')" id="nav-workflow">
      <span class="icon">🧹</span> Workflow Wizard
    </button>
    <div class="sb-section">Learn</div>
    <button class="nav-btn" onclick="nav('guide')" id="nav-guide">
      <span class="icon">🗺️</span> Pipeline Guide
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
      <button class="btn btn-danger btn-sm" onclick="restartServices()" id="restart-btn">🔄 Restart Airflow</button>
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
      </div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Pipelines
        <span style="font-weight:400;font-size:.75rem;color:#4a5568;margin-left:8px;text-transform:none;letter-spacing:0" id="pipes-ts"></span>
      </h3>
      <div id="pipes-content"><div class="loading"><span class="spinner"></span> Loading…</div></div>
      <h3 style="font-size:.85rem;color:#718096;text-transform:uppercase;letter-spacing:.5px;margin:20px 0 12px">Recent Runs</h3>
      <div id="home-runs"><div class="loading"><span class="spinner"></span> Loading…</div></div>
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

  <!-- PIPELINE WIZARD -->
  <div id="sec-workflow" class="section iframe-section">
    <div class="iframe-bar">
      <span>🧹 Workflow Wizard — upload CSV, clean data, build aggregation pipelines</span>
    </div>
    <div class="iframe-wrap">
      <iframe id="frame-workflow" title="Workflow Wizard" allowfullscreen></iframe>
    </div>
  </div>

  <!-- PIPELINE GUIDE -->
  <div id="sec-guide" class="section">
    <div class="sec-header">
      <h2>🗺️ Pipeline Guide</h2>
      <span class="sub">How local CSV data flows through the platform</span>
    </div>
    <div class="guide-wrap">
      <div class="guide-flow">

        <!-- Step 1: CSV Upload -->
        <div class="guide-node n1">
          <div class="guide-node-num">Step 1</div>
          <div class="guide-node-icon">📄</div>
          <div class="guide-node-title">Upload CSV</div>
          <div class="guide-node-desc">Drop a local CSV file in the Workflow Wizard to start a new pipeline</div>
          <div class="guide-node-tags">
            <span class="guide-tag">Workflow Wizard</span>
            <span class="guide-tag">local file</span>
          </div>
        </div>

        <div class="guide-arrow">
          <div class="guide-arrow-line"></div>
          <div class="guide-arrow-head"></div>
          <div class="guide-arrow-label">staged as raw</div>
        </div>

        <!-- Step 2: MinIO -->
        <div class="guide-node n2">
          <div class="guide-node-num">Step 2</div>
          <div class="guide-node-icon">🗄️</div>
          <div class="guide-node-title">MinIO Storage</div>
          <div class="guide-node-desc">CSV is written to the <code style="font-size:.65rem;background:#1e2535;padding:1px 4px;border-radius:3px">datafabrik-raw</code> bucket under <code style="font-size:.65rem;background:#1e2535;padding:1px 4px;border-radius:3px">wizard/</code></div>
          <div class="guide-node-tags">
            <span class="guide-tag">object store</span>
            <span class="guide-tag">:9001</span>
          </div>
        </div>

        <div class="guide-arrow">
          <div class="guide-arrow-line"></div>
          <div class="guide-arrow-head"></div>
          <div class="guide-arrow-label">triggers DAG</div>
        </div>

        <!-- Step 3: Airflow -->
        <div class="guide-node n3">
          <div class="guide-node-num">Step 3</div>
          <div class="guide-node-icon">✈️</div>
          <div class="guide-node-title">Airflow Pipeline</div>
          <div class="guide-node-desc">A DAG is generated and triggered. It reads raw data and runs the SQL transformation</div>
          <div class="guide-node-tags">
            <span class="guide-tag">DAG run</span>
            <span class="guide-tag">:8082</span>
          </div>
        </div>

        <div class="guide-arrow">
          <div class="guide-arrow-line"></div>
          <div class="guide-arrow-head"></div>
          <div class="guide-arrow-label">writes views</div>
        </div>

        <!-- Step 4: Postgres -->
        <div class="guide-node n4">
          <div class="guide-node-num">Step 4</div>
          <div class="guide-node-icon">🐘</div>
          <div class="guide-node-title">Postgres</div>
          <div class="guide-node-desc">Cleaned data lands in <code style="font-size:.65rem;background:#1e2535;padding:1px 4px;border-radius:3px">clean.</code> schema; aggregations in <code style="font-size:.65rem;background:#1e2535;padding:1px 4px;border-radius:3px">analytics.</code></div>
          <div class="guide-node-tags">
            <span class="guide-tag">clean schema</span>
            <span class="guide-tag">analytics schema</span>
          </div>
        </div>

      </div>
    </div>
  </div>

</main>
</div>

<script>
// ── Navigation ──────────────────────────────────────────────────────────
const SECTIONS = ['home','airflow','minio','workflow','guide'];
const IFRAMES  = {airflow:'/tools/airflow', minio:'http://localhost:9002',
                  workflow:'/workflow?embed=1'};
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
const _SVC_NAMES = ['postgres','airflow','minio'];

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
  if (!confirm('Restart Airflow webserver and scheduler?\\n\\nThis takes 60–90 seconds.')) return;
  const btn = document.getElementById('restart-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Restarting…'; }
  try {
    await fetch('/api/admin/restart', {method:'POST'}).then(r => r.json());
    toast('Airflow restarting — polling until ready…');
    pollAirflowReady(0);
  } catch(e) {
    toast('Restart failed: ' + e.message, 'err');
    if (btn) { btn.disabled = false; btn.textContent = '🔄 Restart Airflow'; }
  }
}
async function pollAirflowReady(attempt) {
  const btn = document.getElementById('restart-btn');
  if (attempt >= 18) {
    toast('Airflow still not ready after 3 min — check logs', 'err');
    if (btn) { btn.disabled = false; btn.textContent = '🔄 Restart Airflow'; }
    return;
  }
  await new Promise(r => setTimeout(r, 10000));
  if (btn) btn.textContent = '⏳ Waiting… (' + ((attempt+1)*10) + 's)';
  try {
    const j = await fetch('/api/admin/health').then(r => r.json());
    if (j.airflow === 'up') {
      toast('✓ Airflow is back online');
      checkHealth();
      if (btn) { btn.disabled = false; btn.textContent = '🔄 Restart Airflow'; }
      return;
    }
  } catch(_) {}
  pollAirflowReady(attempt + 1);
}

function openAllTools() {
  window.open('/tools/airflow', '_blank');
  window.open('/tools/minio', '_blank');
  toast('Opening tools — signing in automatically…');
}

// ── Home ────────────────────────────────────────────────────────────────
function loadHome() {
  fetch('/api/runs?limit=8')
    .then(r => r.json())
    .then(runs => renderHomeRuns(runs))
    .catch(() => {
      document.getElementById('home-runs').innerHTML = '<div class="empty">Could not load recent runs.</div>';
    });
  loadPipelines();
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
    document.getElementById('stat-total').textContent = pipes.length;
    document.getElementById('stat-ok').textContent = pipes.filter(p=>p.state==='success').length;
    document.getElementById('stat-fail').textContent = pipes.filter(p=>p.state==='failed').length;
    document.getElementById('stat-paused').textContent = pipes.filter(p=>p.state==='paused').length;
    renderPipelines(pipes);
    document.getElementById('pipes-ts').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('pipes-content').innerHTML = '<div class="empty">Failed to load pipelines: ' + e.message + '</div>';
    document.getElementById('pipes-ts').textContent = 'Could not reach Airflow';
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
    const isPaused = p.state === 'paused';
    const isRunning = p.state === 'running';
    return `<tr id="pipe-row-${p.id}">
      <td><strong>${p.id}</strong></td>
      <td>${badge(p.state)}</td>
      <td class="dim">${p.last_run}</td>
      <td class="dim">${p.duration}</td>
      <td>${pctHtml}</td>
      <td class="dim">${p.rows}</td>
      <td style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-trigger btn-sm" onclick="triggerPipeline('${p.id}', this)"
          ${isRunning ? 'disabled' : ''} title="Trigger a manual run">
          ▶ Run
        </button>
        <button class="btn btn-sm ${isPaused ? 'btn-success' : 'btn-ghost'}"
          onclick="togglePause('${p.id}', ${isPaused}, this)"
          title="${isPaused ? 'Resume scheduled runs' : 'Pause scheduled runs'}">
          ${isPaused ? '▶ Resume' : '⏸ Pause'}
        </button>
        <button class="btn btn-danger btn-sm" onclick="deleteDag('${p.id}', this)"
          title="Delete DAG from Airflow">
          🗑
        </button>
      </td>
    </tr>`;
  }).join('');

  document.getElementById('pipes-content').innerHTML = `
    <table>
      <thead><tr>
        <th>Pipeline</th><th>State</th><th>Last Run</th>
        <th>Duration</th><th>30-day Success</th><th>Rows</th><th>Actions</th>
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

async function togglePause(dagId, currentlyPaused, btn) {
  const willPause = !currentlyPaused;
  btn.disabled = true;
  btn.textContent = '…';
  try {
    await fetch('/api/pipelines/' + encodeURIComponent(dagId) + '/pause?paused=' + willPause, {method:'PATCH'});
    toast(dagId + (willPause ? ' paused' : ' resumed'));
    setTimeout(() => loadPipelines(), 800);
  } catch(e) {
    toast('Failed: ' + e.message, 'err');
    btn.disabled = false;
    btn.textContent = currentlyPaused ? '▶ Resume' : '⏸ Pause';
  }
}

async function deleteDag(dagId, btn) {
  if (!confirm('Delete DAG "' + dagId + '" from Airflow? This also removes any local config files for this pipeline.')) return;
  btn.disabled = true;
  btn.textContent = '…';
  const row = document.getElementById('pipe-row-' + dagId);
  if (row) row.style.opacity = '0.4';
  try {
    await fetch('/api/pipelines/' + encodeURIComponent(dagId) + '/dag', {method:'DELETE'});
    toast('Deleted ' + dagId);
    setTimeout(() => loadPipelines(), 800);
  } catch(e) {
    toast('Failed to delete ' + dagId + ': ' + e.message, 'err');
    btn.disabled = false;
    btn.textContent = '🗑';
    if (row) row.style.opacity = '1';
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

// ── Init ─────────────────────────────────────────────────────────────────
loadHome();
</script>
</body>
</html>'''


# ── Onboarding API ───────────────────────────────────────────────────────────

_CONFIGS_DIR = Path("/app/configs/pipelines")

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
            if t_type not in ("sql", "spark"):
                errors.append({"field": "stages.transformation.type",
                                "message": "Must be one of: sql, spark"})
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
    watermark_column: updated_at"""

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
    dest_key: my_api/{{ ds_nodash }}.json"""

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
    source_key: my-folder/*.csv"""

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
    '<div class="next-step">② Trigger a test run from the '
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
_DANGEROUS_SQL = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|TRUNCATE|CREATE|ALTER|GRANT|REVOKE|EXEC(?:UTE)?|CALL|COPY|pg_sleep|pg_read_file)\b",
    re.IGNORECASE,
)


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


def _generate_clean_sql(table: str, columns: list, filters: list, joins: list = [], computed_cols: list = []) -> str:
    _VALID_JOIN_TYPES = {"INNER", "LEFT", "RIGHT"}
    valid_joins = [
        j for j in joins
        if j.get("table") and j.get("left_col") and j.get("right_col")
        and j.get("join_type", "LEFT").upper() in _VALID_JOIN_TYPES
    ]
    has_joins = bool(valid_joins)

    select_parts = []
    for col in columns:
        if not col.get("include", True):
            continue
        src   = _safe_name(col["name"])          # actual column name in raw table
        out   = _safe_name(col.get("output_name") or col["name"])
        dtype = col.get("type", "TEXT")
        if dtype not in _VALID_TYPES:
            dtype = "TEXT"
        tq = f'"{table}".' if has_joins else ""
        if dtype == "TEXT":
            expr = f'trim({tq}"{src}")'
        elif dtype == "NUMERIC":
            expr = f'round({tq}"{src}"::NUMERIC, 2)'
        else:
            expr = f'{tq}"{src}"::{dtype}'
        select_parts.append(f'        {expr} AS "{out}"')

    where_parts = []
    for f in filters:
        op  = f.get("operator", "=")
        col = _safe_name(f.get("column", ""))   # match the sanitised column name
        val = f.get("value", "")
        if op not in _VALID_OPS or not col:
            continue
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f'"{col}" {op}')
        elif op == "LIKE":
            where_parts.append(f'"{col}" LIKE \'%{val}%\'')
        else:
            where_parts.append(f'"{col}" {op} \'{val}\'')

    for cc in computed_cols:
        name = _safe_name(cc.get("name", "") or "")
        expr = (cc.get("expression", "") or "").strip()
        if name and expr and not _DANGEROUS_SQL.search(expr):
            select_parts.append(f'        {expr} AS "{name}"')

    select_str = ",\n".join(select_parts) or "        *"
    where_str  = ""
    if where_parts:
        where_str = "WHERE " + "\n  AND ".join(where_parts) + "\n"

    if has_joins:
        join_lines = ""
        for j in valid_joins:
            jt     = j["join_type"].upper()
            jtable = _safe_name(j["table"])
            lcol   = _safe_name(j["left_col"])
            rcol   = _safe_name(j["right_col"])
            join_lines += f'    {jt} JOIN raw."{jtable}" ON "{table}"."{lcol}" = "{jtable}"."{rcol}"\n'
        return (
            f'with cleaned as (\n'
            f'    select\n'
            f'{select_str}\n'
            f'    from raw."{table}"\n'
            f'{join_lines}'
            f')\n'
            f'select * from cleaned\n'
            f'{where_str}'
        )
    return (
        f'with source as (\n'
        f'    select * from raw."{table}"\n'
        f'),\n'
        f'cleaned as (\n'
        f'    select\n'
        f'{select_str}\n'
        f'    from source\n'
        f')\n'
        f'select * from cleaned\n'
        f'{where_str}'
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


class _AggMetric(BaseModel):
    column: str
    fn: str
    output_name: str = ""


class _JoinConfig(BaseModel):
    table: str
    join_type: str = "LEFT"
    left_col: str
    right_col: str


class _ComputedCol(BaseModel):
    name: str
    expression: str


def _generate_agg_sql(clean_model: str, group_by: list[str], metrics: list[dict]) -> str:
    valid_fns = {"SUM", "COUNT", "AVG", "MIN", "MAX"}
    select_parts = []
    for c in group_by:
        select_parts.append(f'        "{_safe_name(c)}"')
    for m in metrics:
        fn  = m.get("fn", "SUM").upper()
        col = _safe_name(m.get("column", ""))
        out = _safe_name(m.get("output_name", "")) if m.get("output_name") else ""
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
        group_str = "    GROUP BY " + ", ".join(f'"{_safe_name(c)}"' for c in group_by) + "\n"
    return (
        f'with source as (\n'
        f'    select * from clean."{clean_model}"\n'  # clean_model == table name
        f'),\n'
        f'aggregated as (\n'
        f'    select\n'
        f'{select_str}\n'
        f'    from source\n'
        f'{group_str}'
        f')\n'
        f'select * from aggregated\n'
    )


@app.get("/api/workflow/tables")
def api_workflow_tables() -> dict:
    """List BASE TABLEs in raw schema with their column names (excluding metadata columns)."""
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                "SELECT c.table_name, c.column_name "
                "FROM information_schema.columns c "
                "JOIN information_schema.tables t "
                "  ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
                "WHERE c.table_schema = 'raw' "
                "  AND t.table_type = 'BASE TABLE' "
                "  AND c.column_name != 'uploaded_at' "
                "ORDER BY c.table_name, c.ordinal_position"
            ))
            tables: dict[str, list[str]] = {}
            for row in result:
                tname, cname = row[0], row[1]
                if tname not in tables:
                    tables[tname] = []
                tables[tname].append(cname)
    except Exception:
        tables = {}
    return {"tables": tables}


@app.post("/api/workflow/upload")
async def api_workflow_upload(table: str = Form(...), file: UploadFile = File(...)) -> dict:
    table_name = _safe_name(table)
    if not table_name:
        raise HTTPException(status_code=400, detail="Invalid table name")
    _SAMPLE_LIMIT = 1000
    content = await file.read()
    reader  = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    all_rows = list(reader)
    if not all_rows:
        raise HTTPException(status_code=400, detail="CSV has no data rows")
    total_rows = len(all_rows)
    rows       = all_rows[:_SAMPLE_LIMIT]
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
    # Step 1: insert sampled rows into Postgres raw schema
    safe_cols    = [_safe_name(c) for c in fieldnames]
    col_defs     = ", ".join(f'"{c}" TEXT' for c in safe_cols)
    col_list     = ", ".join(f'"{c}"' for c in safe_cols)
    placeholders = ", ".join(f":{c}" for c in safe_cols)
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
    # Step 2: upload sampled rows to MinIO
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    s3_bucket = "datafabrik-raw"
    s3_key    = f"wizard/{table_name}/{table_name}_{ts}.csv"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    _s3_client().put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return {
        "table_name":  table_name,
        "rows":        len(rows),
        "total_rows":  total_rows,
        "sampled":     total_rows > _SAMPLE_LIMIT,
        "columns":     columns,
        "postgres_table": f"raw.{table_name}",
        "s3_bucket":   s3_bucket,
        "s3_key":      s3_key,
    }


class _TableProcessConfig(BaseModel):
    table: str
    columns: list[_ColConfig]
    filters: list[_FilterConfig] = []
    joins: list[_JoinConfig] = []
    computed_cols: list[_ComputedCol] = []
    group_by: list[str] = []
    metrics: list[_AggMetric] = []


class _ProcessPayload(BaseModel):
    tables: list[_TableProcessConfig]
    custom_sql: str | None = None


@app.post("/api/workflow/preview-sql")
def api_preview_sql(payload: _ProcessPayload) -> dict:
    """Return generated SQL from the builder config without writing any files."""
    if not payload.tables:
        raise HTTPException(status_code=400, detail="No tables provided")

    sql_parts: list[str] = ["CREATE SCHEMA IF NOT EXISTS clean;"]
    if any(t.metrics for t in payload.tables):
        sql_parts.append("CREATE SCHEMA IF NOT EXISTS analytics;")

    for tbl_cfg in payload.tables:
        table = _safe_name(tbl_cfg.table)
        if not table:
            continue
        clean_sql = _generate_clean_sql(
            table,
            [c.model_dump() for c in tbl_cfg.columns],
            [f.model_dump() for f in tbl_cfg.filters],
            [j.model_dump() for j in tbl_cfg.joins],
            [cc.model_dump() for cc in tbl_cfg.computed_cols],
        )
        sql_parts.append(f'DROP VIEW IF EXISTS clean."{table}" CASCADE;')
        sql_parts.append(f'CREATE VIEW clean."{table}" AS\n{clean_sql};')
        if tbl_cfg.metrics:
            agg_sql = _generate_agg_sql(
                table, tbl_cfg.group_by, [m.model_dump() for m in tbl_cfg.metrics]
            )
            sql_parts.append(f'DROP VIEW IF EXISTS analytics."{table}" CASCADE;')
            sql_parts.append(
                f'CREATE VIEW analytics."{table}" AS\n{agg_sql};'
            )

    return {"sql": "\n".join(sql_parts) + "\n"}


@app.post("/api/workflow/process")
def api_process(payload: _ProcessPayload) -> dict:
    """Generate one pipeline with clean (+ optional analytics) views for all tables."""
    if not payload.tables:
        raise HTTPException(status_code=400, detail="No tables provided")

    ts    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    first = _safe_name(payload.tables[0].table)
    pid   = f"wiz_{'multi' if len(payload.tables) > 1 else first}_{ts}"

    sql_parts: list[str] = ["CREATE SCHEMA IF NOT EXISTS clean;"]
    if any(t.metrics for t in payload.tables):
        sql_parts.append("CREATE SCHEMA IF NOT EXISTS analytics;")

    result_views: list[dict] = []
    for tbl_cfg in payload.tables:
        table = _safe_name(tbl_cfg.table)
        if not table:
            continue
        clean_sql = _generate_clean_sql(
            table,
            [c.model_dump() for c in tbl_cfg.columns],
            [f.model_dump() for f in tbl_cfg.filters],
            [j.model_dump() for j in tbl_cfg.joins],
            [cc.model_dump() for cc in tbl_cfg.computed_cols],
        )
        sql_parts.append(f'DROP VIEW IF EXISTS clean."{table}" CASCADE;')
        sql_parts.append(f'CREATE VIEW clean."{table}" AS\n{clean_sql};')

        agg_view = None
        if tbl_cfg.metrics:
            agg_sql = _generate_agg_sql(
                table, tbl_cfg.group_by, [m.model_dump() for m in tbl_cfg.metrics]
            )
            sql_parts.append(f'DROP VIEW IF EXISTS analytics."{table}" CASCADE;')
            sql_parts.append(
                f'CREATE VIEW analytics."{table}" AS\n{agg_sql};'
            )
            agg_view = f'analytics."{table}"'

        result_views.append(
            {"table": table, "clean_view": f'clean."{table}"', "agg_view": agg_view}
        )

    table_names = ", ".join(_safe_name(t.table) for t in payload.tables)
    tags        = ["generated", "wizard"] + [_safe_name(t.table) for t in payload.tables]

    # Build optional ingestion block with source XCom info
    ingestion_block = ""
    s3_info = _latest_wizard_file(first)
    if s3_info:
        s3_key, s3_filename = s3_info
        ingestion_block = (
            f"  ingestion:\n"
            f"    type: wizard_csv\n"
            f"    bucket: datafabrik-raw\n"
            f"    key: {s3_key}\n"
            f"    filename: {s3_filename}\n"
            f"    table: {first}\n"
        )

    sql_content = payload.custom_sql if payload.custom_sql else "\n".join(sql_parts) + "\n"

    _CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    (_CONFIGS_DIR / f"{pid}.sql").write_text(sql_content)
    (_CONFIGS_DIR / f"{pid}.yaml").write_text(
        f"pipeline_id: {pid}\n"
        f"description: Wizard pipeline for {table_names}\n"
        f"tags: [{', '.join(tags)}]\n\n"
        f"schedule:\n  preset: \"@once\"\n  retries: 1\n\n"
        f"stages:\n"
        + ingestion_block +
        f"  transformation:\n"
        f"    type: sql\n"
        f"    sql_file: /opt/airflow/configs/pipelines/{pid}.sql\n"
    )
    api_trigger_pipeline(pid)

    return {
        "pipeline_id": pid,
        "views":       result_views,
        "airflow_url": f"http://localhost:8082/dags/{pid}/grid",
    }


# ── Pipeline management API ────────────────────────────────────────────────────

@app.get("/api/pipelines/list")
def api_pipelines_list() -> dict:
    """Return all generated pipeline configs with their metadata."""
    pipelines = []
    if _CONFIGS_DIR.is_dir():
        for yaml_path in sorted(_CONFIGS_DIR.glob("*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True):
            pid = yaml_path.stem
            sql_path = _CONFIGS_DIR / f"{pid}.sql"
            try:
                data = yaml.safe_load(yaml_path.read_text()) or {}
            except Exception:
                data = {}
            pipelines.append({
                "pipeline_id": pid,
                "description": data.get("description", ""),
                "tags": data.get("tags", []),
                "created_at": datetime.fromtimestamp(yaml_path.stat().st_mtime, tz=timezone.utc).isoformat(),
                "has_sql": sql_path.exists(),
                "airflow_url": f"http://localhost:8082/dags/{pid}/grid",
            })
    return {"pipelines": pipelines}


@app.delete("/api/pipelines/{pipeline_id}")
def api_pipeline_delete(pipeline_id: str) -> dict:
    """Delete pipeline config files and remove the DAG from Airflow."""
    deleted = []
    for ext in (".yaml", ".sql"):
        p = _CONFIGS_DIR / f"{pipeline_id}{ext}"
        if p.exists():
            p.unlink()
            deleted.append(str(p.name))
    # Best-effort: delete from Airflow (ignore errors)
    try:
        _requests.delete(
            f"{AIRFLOW_URL}/api/v1/dags/{pipeline_id}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5,
        )
    except Exception:
        pass
    return {"deleted": deleted, "pipeline_id": pipeline_id}


_MANAGE_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>DataFabrik — Manage Pipelines</title>'
    '<style>'
    '*{box-sizing:border-box;margin:0;padding:0}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;display:flex;align-items:center;gap:14px}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center}'
    '.logo svg{width:16px;height:16px;fill:none}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.page{max-width:900px;margin:0 auto;padding:32px 24px}'
    '.hero{margin-bottom:28px}'
    '.hero h2{font-size:1.4rem;font-weight:700;margin-bottom:6px}'
    '.hero p{color:#a0aec0;font-size:.88rem}'
    '.toolbar{display:flex;align-items:center;gap:12px;margin-bottom:16px}'
    '.search-inp{background:#1a1f2e;border:1px solid #2d3748;border-radius:6px;padding:8px 14px;'
    'color:#e2e8f0;font-size:.85rem;outline:none;flex:1;max-width:320px}'
    '.search-inp:focus{border-color:#3182ce}'
    '.count-label{font-size:.8rem;color:#4a5568;margin-left:auto}'
    '.pipe-list{display:flex;flex-direction:column;gap:10px}'
    '.pipe-card{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;'
    'padding:16px 20px;display:flex;align-items:center;gap:16px;transition:border-color .15s}'
    '.pipe-card:hover{border-color:#4a5568}'
    '.pipe-card.deleting{opacity:.4;pointer-events:none}'
    '.pipe-info{flex:1;min-width:0}'
    '.pipe-id{font-family:monospace;font-size:.85rem;font-weight:700;color:#63b3ed;'
    'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:3px}'
    '.pipe-desc{font-size:.78rem;color:#718096;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
    '.pipe-meta{font-size:.72rem;color:#4a5568;margin-top:4px}'
    '.tag{display:inline-flex;align-items:center;background:#232a3b;border-radius:4px;'
    'padding:2px 7px;font-size:.7rem;color:#718096;margin-right:4px}'
    '.pipe-actions{display:flex;align-items:center;gap:8px;flex-shrink:0}'
    '.btn{display:inline-flex;align-items:center;gap:5px;padding:7px 16px;border-radius:7px;'
    'font-size:.8rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s;text-decoration:none}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-ghost{background:#2d3748;color:#e2e8f0}.btn-ghost:hover{background:#3a4459}'
    '.btn-danger{background:#742a2a;color:#fed7d7}.btn-danger:hover{background:#9b2c2c}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover{background:#2b6cb0}'
    '.empty{text-align:center;padding:60px 0;color:#4a5568;font-size:.9rem}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#fc8181;'
    'border-radius:50%;animation:spin .6s linear infinite;display:inline-block}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.toast{position:fixed;bottom:24px;right:24px;background:#276749;color:#9ae6b4;'
    'padding:12px 20px;border-radius:8px;font-size:.85rem;font-weight:600;'
    'box-shadow:0 4px 12px rgba(0,0,0,.4);animation:fadein .2s;z-index:999}'
    '.toast.err{background:#742a2a;color:#fed7d7}'
    '@keyframes fadein{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik — Manage Pipelines</h1>'
    '<a href="/">← Back to Portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="hero">'
    '<h2>Generated Pipelines</h2>'
    '<p>All pipelines created by the Workflow Wizard. Delete removes the config files and the Airflow DAG.</p>'
    '</div>'
    '<div class="toolbar">'
    '<input class="search-inp" type="text" placeholder="Filter by pipeline ID…" oninput="applyFilter(this.value)">'
    '<span class="count-label" id="count-label"></span>'
    '</div>'
    '<div class="pipe-list" id="pipe-list"><div class="empty">Loading…</div></div>'
    '</div>'
    '<script>'
    'let ALL=[];'
    'async function load(){'
    'const r=await fetch("/api/pipelines/list");'
    'const j=await r.json();'
    'ALL=j.pipelines;render(ALL);}'
    'function render(list){'
    'const el=document.getElementById("pipe-list");'
    'const n=list.length;'
    'document.getElementById("count-label").textContent=n+" pipeline"+(n!==1?"s":"");'
    'if(!n){el.innerHTML=\'<div class="empty">No generated pipelines found.</div>\';return;}'
    'el.innerHTML=list.map(p=>{'
    'const tags=p.tags.map(t=>\'<span class="tag">\'+t+\'</span>\').join("");'
    'const created=new Date(p.created_at).toLocaleString();'
    'return \'<div class="pipe-card" id="card-\'+p.pipeline_id+\'" data-pid="\'+p.pipeline_id+\'">\''
    '+\'<div class="pipe-info">\''
    '+\'<div class="pipe-id">\'+p.pipeline_id+\'</div>\''
    '+\'<div class="pipe-desc">\'+( p.description||"—")+\'</div>\''
    '+\'<div class="pipe-meta">\'+tags+\' &nbsp;·&nbsp; Created \'+created+\'</div>\''
    '+\'</div>\''
    '+\'<div class="pipe-actions">\''
    '+\'<a class="btn btn-ghost" href="\'+p.airflow_url+\'" target="_blank">Airflow &#8599;</a>\''
    '+\'<button class="btn btn-danger delbtn">Delete</button>\''
    '+\'</div></div>\';}).join("");'
    'el.querySelectorAll(".delbtn").forEach(btn=>{'
    'btn.addEventListener("click",()=>deletePipeline(btn.closest(".pipe-card").dataset.pid));});}'
    'function applyFilter(q){'
    'const f=q.toLowerCase();'
    'render(f?ALL.filter(p=>p.pipeline_id.toLowerCase().includes(f)||p.description.toLowerCase().includes(f)):ALL);}'
    'async function deletePipeline(pid){'
    'if(!confirm("Delete pipeline \\""+pid+"\\"? This removes config files and the Airflow DAG."))return;'
    'const card=document.getElementById("card-"+pid);'
    'if(card)card.classList.add("deleting");'
    'const r=await fetch("/api/pipelines/"+pid,{method:"DELETE"});'
    'if(r.ok){ALL=ALL.filter(p=>p.pipeline_id!==pid);render(ALL);toast("Deleted "+pid);}'
    'else{if(card)card.classList.remove("deleting");toast("Delete failed",true);}}'
    'function toast(msg,err){'
    'const t=document.createElement("div");'
    't.className="toast"+(err?" err":"");t.textContent=msg;'
    'document.body.appendChild(t);setTimeout(()=>t.remove(),3000);}'
    'load();'
    '</script>'
    '<script>if(location.search.includes("embed=1"))document.querySelectorAll(\'a[href="/"]\').forEach(e=>e.style.display="none");</script>'
    '</body></html>'
)


@app.get("/manage", response_class=HTMLResponse)
def manage_page() -> str:
    """Pipeline management — list and delete generated pipelines."""
    return _MANAGE_HTML


_WORKFLOW_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>DataFabrik — Workflow Wizard</title>'
    '<style>'
    '*{box-sizing:border-box;margin:0;padding:0}'
    'html,body{height:100%;overflow:hidden}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;display:flex;flex-direction:column}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;display:flex;align-items:center;gap:14px;flex-shrink:0}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center}'
    '.logo svg{width:16px;height:16px}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.page{flex:1;min-height:0;overflow-y:auto;max-width:900px;margin:0 auto;padding:20px 24px;width:100%}'
    '.stepper{display:flex;align-items:center;margin-bottom:20px}'
    '.si{display:flex;align-items:center;gap:8px;font-size:.8rem;color:#4a5568}'
    '.si.active{color:#63b3ed;font-weight:600}'
    '.si.done{color:#48bb78}'
    '.si-circle{width:24px;height:24px;border-radius:50%;background:#2d3748;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;flex-shrink:0}'
    '.si.active .si-circle{background:#2b4c7e;color:#90cdf4}'
    '.si.done .si-circle{background:#276749;color:#9ae6b4}'
    '.si-sep{flex:1;height:1px;background:#2d3748;margin:0 12px}'
    '.panel{display:none}.panel.active{display:block}'
    '.panel-head{margin-bottom:20px}'
    '.panel-head h2{font-size:1.4rem;font-weight:700;margin-bottom:6px}'
    '.panel-head p{color:#a0aec0;font-size:.9rem;line-height:1.6}'
    '.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:20px 24px;margin-bottom:14px}'
    '.drop-zone{border:2px dashed #2d3748;border-radius:10px;padding:40px 24px;text-align:center;cursor:pointer;transition:border-color .2s,background .2s;position:relative}'
    '.drop-zone:hover,.drop-zone.drag{border-color:#3182ce;background:#0d1627}'
    '.drop-zone.done{border-color:#48bb78;background:#0d1f14;border-style:solid}'
    '.drop-zone input{position:absolute;inset:0;opacity:0;cursor:pointer}'
    '.dz-icon{font-size:2rem;margin-bottom:10px;display:block}'
    '.dz-main{font-size:.9rem;color:#a0aec0;margin-bottom:4px}'
    '.dz-sub{font-size:.75rem;color:#4a5568}'
    '.col-wrap{overflow-x:auto}'
    '.col-tbl{width:100%;border-collapse:collapse;font-size:.8rem}'
    '.col-tbl th{background:#232a3b;color:#4a5568;padding:7px 10px;text-align:left;border-bottom:1px solid #2d3748;white-space:nowrap;font-weight:600}'
    '.col-tbl td{padding:6px 10px;border-bottom:1px solid #151a27;vertical-align:middle}'
    '.col-tbl tr:last-child td{border-bottom:none}'
    '.col-tbl input[type=checkbox]{width:15px;height:15px;cursor:pointer;accent-color:#3182ce}'
    '.col-out{width:130px;background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:4px 8px;color:#e2e8f0;font-size:.78rem}'
    '.col-out:focus{border-color:#3182ce;outline:none}'
    'select.col-type{background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:4px 8px;color:#e2e8f0;font-size:.78rem;cursor:pointer}'
    '.col-sample{color:#4a5568;font-size:.72rem;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    '.filter-list,.join-list{display:flex;flex-direction:column;gap:8px}'
    '.filter-row,.join-row,.expr-row{display:flex;align-items:center;gap:6px;flex-wrap:wrap}'
    'select.filter-col,select.filter-op,select.join-type,select.join-tbl,select.join-lcol,select.join-rcol'
    '{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:6px 9px;color:#e2e8f0;font-size:.8rem;cursor:pointer}'
    'select.filter-col,select.join-lcol,select.join-rcol{min-width:110px}'
    'select.join-tbl{min-width:120px}'
    'select.filter-op,select.join-type{min-width:88px}'
    '.filter-val{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:6px 9px;color:#e2e8f0;font-size:.8rem;flex:1;min-width:70px}'
    '.filter-val:focus{border-color:#3182ce;outline:none}'
    '.join-eq{color:#4a5568;font-size:.85rem;padding:0 2px;flex-shrink:0}'
    '.filter-rm{background:none;border:none;color:#4a5568;cursor:pointer;font-size:1.1rem;padding:0 4px;line-height:1;flex-shrink:0}'
    '.filter-rm:hover{color:#fc8181}'
    '.no-msg{font-size:.78rem;color:#4a5568;padding:6px 0}'
    'input.expr-name{width:130px;background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:6px 9px;color:#e2e8f0;font-size:.8rem;outline:none}'
    'input.expr-name:focus{border-color:#3182ce}'
    'input.expr-val{background:#0d1117;border:1px solid #2d3748;border-radius:5px;padding:6px 9px;color:#68d391;font-size:.8rem;font-family:monospace;flex:1;min-width:150px;outline:none}'
    'input.expr-val:focus{border-color:#3182ce}'
    '.expr-eq{color:#4a5568;font-size:.85rem;padding:0 4px;flex-shrink:0}'
    'input[type=text]{background:#0d1117;border:1px solid #2d3748;border-radius:6px;padding:8px 12px;color:#e2e8f0;font-size:.875rem;outline:none;flex:1;transition:border-color .15s}'
    'input[type=text]:focus{border-color:#3182ce}'
    '.btn-link{background:none;border:none;color:#63b3ed;cursor:pointer;font-size:.78rem;padding:0}'
    '.btn-link:hover{text-decoration:underline}'
    '.btn-link:disabled{color:#4a5568;cursor:not-allowed}'
    '.panel-footer{display:flex;align-items:center;gap:12px;margin-top:14px}'
    '.btn{display:inline-flex;align-items:center;gap:6px;padding:10px 22px;border-radius:8px;font-size:.875rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s;text-decoration:none}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}'
    '.btn-ghost{background:#2d3748;color:#e2e8f0}.btn-ghost:hover{background:#3a4459}'
    '.btn-sm{padding:6px 14px!important;font-size:.8rem!important}'
    '.spin-wrap{display:flex;align-items:center;gap:8px;font-size:.85rem;color:#718096}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#63b3ed;border-radius:50%;animation:spin .6s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.err-msg{color:#fc8181;font-size:.85rem}'
    '.success-block{text-align:center;padding:28px 0 20px}'
    '.success-block .s-icon{font-size:3rem;margin-bottom:14px;display:block}'
    '.success-block h2{font-size:1.4rem;font-weight:700;margin-bottom:8px}'
    '.chk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px}'
    '.chk-item{display:flex;align-items:center;gap:8px;background:#0d1117;border:1px solid #2d3748;'
    'border-radius:6px;padding:8px 12px;font-size:.8rem;cursor:pointer;transition:border-color .2s}'
    '.chk-item:hover{border-color:#4a5568}'
    '.chk-item input{width:14px;height:14px;cursor:pointer;accent-color:#3182ce}'
    '.metric-row{display:grid;grid-template-columns:1fr 90px 1fr 28px;gap:8px;align-items:center;margin-bottom:8px}'
    '.metric-row select,.metric-row input[type=text]{background:#0d1117;border:1px solid #2d3748;'
    'border-radius:5px;padding:6px 9px;color:#e2e8f0;font-size:.79rem;outline:none}'
    '.metric-row select:focus,.metric-row input[type=text]:focus{border-color:#3182ce}'
    '.agg-toggle{display:flex;align-items:center;gap:10px;margin-bottom:12px;cursor:pointer;font-size:.88rem;color:#e2e8f0}'
    '.agg-toggle input[type=checkbox]{width:16px;height:16px;accent-color:#3182ce;cursor:pointer}'
    '.sub-label{font-size:.73rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#718096;margin:12px 0 6px}'
    '.ts-card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;margin-bottom:12px;overflow:hidden}'
    '.ts-head{display:flex;align-items:center;justify-content:space-between;padding:16px 20px;cursor:pointer;'
    'border-bottom:1px solid #2d3748;transition:background .15s}'
    '.ts-head:hover{background:#232a3b}'
    '.ts-head-info{display:flex;align-items:center;gap:14px}'
    '.ts-name{font-size:.9rem;font-weight:700;color:#63b3ed;font-family:monospace}'
    '.ts-meta{font-size:.72rem;color:#4a5568}'
    '.ts-chev{font-size:.8rem;color:#718096;flex-shrink:0}'
    '.ts-body{padding:20px}'
    '.ts-section{margin-bottom:18px;padding-bottom:16px;border-bottom:1px solid #151a27}'
    '.ts-section:last-child{margin-bottom:0;padding-bottom:0;border-bottom:none}'
    '.ts-stitle{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#718096;'
    'margin-bottom:8px;display:flex;align-items:center}'
    '.pb-summary{background:#151a27;border-radius:8px;padding:10px 16px;font-size:.82rem;color:#718096;margin-bottom:14px}'
    '.pb-summary b{color:#e2e8f0}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16" fill="none">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik — Workflow Wizard</h1>'
    '<a href="/">&#8592; Back to Portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="stepper">'
    '<div class="si active" id="si1"><div class="si-circle">1</div>Upload CSV</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si2"><div class="si-circle">2</div>Pipeline Builder</div>'
    '<div class="si-sep"></div>'
    '<div class="si" id="si3"><div class="si-circle">3</div>Results</div>'
    '</div>'
    '<div id="p1" class="panel active">'
    '<iframe id="upload-frame" src="/upload?embed=1"'
    ' style="width:100%;border:none;border-radius:8px;height:380px;display:block;transition:height .15s"></iframe>'
    '<div id="s1-continue" style="display:none"><div class="panel-footer">'
    '<button class="btn btn-primary" id="s1-btn"'
    ' onclick="enterPipelineBuilder(state.uploadedTables)">Continue to Pipeline Builder &#8594;</button>'
    '<span id="s1-hint" style="font-size:.78rem;color:#718096"></span>'
    '</div></div>'
    '</div>'
    '<div id="p2" class="panel">'
    '<div class="panel-head"><h2>Pipeline Builder</h2>'
    '<p>Configure each uploaded table — columns, filters, expressions, joins, and aggregation. All tables are processed together in one pipeline.</p></div>'
    '<div class="pb-summary" id="pb-summary"></div>'
    '<div id="tables-container"></div>'
    '<div class="panel-footer">'
    '<button class="btn btn-ghost" onclick="goStep(1)">&#8592; Back</button>'
    '<button class="btn btn-primary" id="s2-btn" onclick="doProcess()">Process &amp; Load &#8594;</button>'
    '<div id="s2-status"></div>'
    '</div>'
    '</div>'
    '<div id="p3" class="panel">'
    '<div class="card">'
    '<div class="success-block"><span class="s-icon">&#9989;</span><h2>Pipeline Triggered!</h2></div>'
    '<div id="r-pipelines"></div>'
    '</div>'
    '<div class="card" style="font-size:.82rem;color:#718096;line-height:1.8">'
    '<div style="font-weight:700;color:#e2e8f0;margin-bottom:10px">What happens next</div>'
    '<div>&#9312; Airflow creates a <code style="color:#63b3ed">clean.*</code> view for each table from <code style="color:#63b3ed">raw.*</code></div>'
    '<div>&#9313; If you enabled aggregation, <code style="color:#63b3ed">analytics.*</code> views are also created</div>'
    '<div style="margin-top:8px">&#9314; Query the views from any SQL client — connect to the <code style="color:#63b3ed">datafabrik</code> database on <code style="color:#63b3ed">localhost:5433</code></div>'
    '</div>'
    '<div class="panel-footer" style="justify-content:center;gap:12px;padding-top:16px;border-top:1px solid #2d3748;margin-top:16px">'
    '<button class="btn btn-ghost" onclick="goStep(2)">&#8592; Back</button>'
    '<button class="btn btn-primary" onclick="resetWizard()">&#10003; Complete</button>'
    '</div>'
    '</div>'
    '</div>'
    '</div>'
    '<script>'
    'const TYPES=["TEXT","INTEGER","NUMERIC","DATE","TIMESTAMP","BOOLEAN"];'
    'const OPS=[["=","equals"],["!=","not equals"],[">=",">="],["<=","<="],["LIKE","contains"],["IS NULL","is empty"],["IS NOT NULL","not empty"]];'
    'const JOIN_TYPES=["LEFT","INNER","RIGHT"];'
    'const FNS=["SUM","COUNT","AVG","MIN","MAX"];'
    'let state={uploadedTables:[],rawTables:{}};'
    'let seqs={};'
    'function nextSeq(i,t){if(!seqs[i])seqs[i]={};if(!seqs[i][t])seqs[i][t]=0;return++seqs[i][t];}'
    'window.addEventListener("message",function(e){'
    'if(!e.data)return;'
    'if(e.data.type==="datafabrik_height"){const fr=document.getElementById("upload-frame");if(fr)fr.style.height=e.data.h+"px";return;}'
    'if(e.data.type!=="datafabrik_upload")return;'
    'const tables=e.data.tables||[{table_name:e.data.table_name,rows:e.data.rows,columns:e.data.columns}];'
    'state.uploadedTables=tables;'
    'const hint=document.getElementById("s1-hint");'
    'if(hint)hint.textContent=tables.length>1?tables.length+" tables ready":"1 table ready";'
    'document.getElementById("s1-continue").style.display="";});'
    'async function enterPipelineBuilder(tables){'
    'try{'
    'const resp=await fetch("/api/workflow/tables");'
    'const jt=await resp.json();'
    'state.rawTables=jt.tables||{};'
    '}catch(_){state.rawTables={};}'
    'seqs={};'
    'document.getElementById("tables-container").innerHTML="";'
    'tables.forEach((t,i)=>buildTableSection(i,t,i===0));'
    'const n=tables.length;'
    'document.getElementById("pb-summary").innerHTML='
    '`<b>${n} table${n>1?"s":""} loaded</b> &nbsp;\xb7&nbsp; ${tables.map(t=>`<code style="color:#63b3ed">raw.${t.table_name}</code>`).join(" , ")}`;'
    'goStep(2);}'
    'function goStep(n){'
    'document.querySelectorAll(".panel").forEach(p=>p.classList.remove("active"));'
    'document.getElementById(`p${n}`).classList.add("active");'
    '["si1","si2","si3"].forEach((id,i)=>{'
    'const el=document.getElementById(id);'
    'if(el)el.className="si"+(i+1<n?" done":i+1===n?" active":"");});'
    'if(n===1){document.getElementById("s1-continue").style.display="none";const fr=document.getElementById("upload-frame");if(fr)fr.src="/upload?embed=1";}'
    'if(n===2){document.getElementById("s2-btn").disabled=false;document.getElementById("s2-status").innerHTML="";}}'
    'function buildTableSection(idx,t,expanded){'
    'const cols=t.columns||[];'
    'const colRows=cols.map((c,ci)=>'
    '`<tr>`+'
    '`<td><input type="checkbox" class="col-check" checked data-ci="${ci}"></td>`+'
    '`<td style="color:#718096;font-size:.78rem">${c.name}</td>`+'
    '`<td><input type="text" class="col-out" value="${c.name}" data-ci="${ci}"></td>`+'
    '`<td><select class="col-type" data-ci="${ci}">${TYPES.map(tp=>`<option${tp===c.type?" selected":""}>${tp}</option>`).join("")}</select></td>`+'
    '`<td class="col-sample">${(c.samples||[]).join(", ")}</td>`+'
    '`</tr>`'
    ').join("");'
    'const otherTbls=Object.keys(state.rawTables).filter(n=>n!==t.table_name);'
    'const joinBtnDis=otherTbls.length?"":"disabled";'
    'const noJoinMsg=otherTbls.length?"No joins":"No other tables available for joins";'
    'const div=document.createElement("div");'
    'div.className="ts-card";div.id=`ts-${idx}`;'
    'div.dataset.tsIdx=idx;div.dataset.tsTable=t.table_name;'
    'div.innerHTML='
    '`<div class="ts-head" onclick="toggleTs(${idx})">`+'
    '`<div class="ts-head-info"><span class="ts-name">raw.${t.table_name}</span>`+'
    '`<span class="ts-meta">${(t.rows||0).toLocaleString()} rows &nbsp;\xb7&nbsp; ${cols.length} cols</span></div>`+'
    '`<span class="ts-chev" id="chev-${idx}">${expanded?"▲":"▼"}</span>`+'
    '`</div>`+'
    '`<div class="ts-body" id="tsb-${idx}" style="${expanded?"":"display:none"}">`+'
    '`<div class="ts-section">`+'
    '`<div class="ts-stitle">Columns`+'
    '`<span style="font-weight:400;color:#4a5568;font-size:.7rem;text-transform:none;letter-spacing:0;margin-left:6px">— uncheck to exclude \xb7 rename \xb7 type</span>`+'
    '`</div>`+'
    '`<div class="col-wrap"><table class="col-tbl"><thead><tr>`+'
    '`<th>Keep</th><th>Source</th><th>Output name</th><th>Type</th><th>Samples</th>`+'
    '`</tr></thead><tbody>${colRows}</tbody></table></div>`+'
    '`</div>`+'
    '`<div class="ts-section">`+'
    '`<div class="ts-stitle">WHERE Filters`+'
    '`<button class="btn-link" style="margin-left:8px" onclick="addFilter(${idx})">+ Add filter</button>`+'
    '`</div>`+'
    '`<div style="font-size:.73rem;color:#4a5568;margin-bottom:6px">Only rows matching ALL conditions are included.</div>`+'
    '`<div class="filter-list" id="fl-${idx}"></div>`+'
    '`<div class="no-msg" id="nf-${idx}">No filters — all rows included</div>`+'
    '`</div>`+'
    '`<div class="ts-section">`+'
    '`<div class="ts-stitle">Computed Columns`+'
    '`<button class="btn-link" style="margin-left:8px" onclick="addExpr(${idx})">+ Add column</button>`+'
    '`</div>`+'
    '`<div style="font-size:.73rem;color:#4a5568;margin-bottom:6px">SQL expressions: <code style="color:#63b3ed">price*qty</code>, <code style="color:#63b3ed">upper(name)</code>, <code style="color:#63b3ed">CASE WHEN score&gt;80 THEN \'A\' ELSE \'B\' END</code></div>`+'
    '`<div id="el-${idx}" style="display:flex;flex-direction:column;gap:8px"></div>`+'
    '`<div class="no-msg" id="ne-${idx}">No computed columns</div>`+'
    '`</div>`+'
    '`<div class="ts-section">`+'
    '`<div class="ts-stitle">Join Tables`+'
    '`<button class="btn-link" style="margin-left:8px" ${joinBtnDis} onclick="addJoin(${idx})">+ Add join</button>`+'
    '`</div>`+'
    '`<div style="font-size:.73rem;color:#4a5568;margin-bottom:6px">Type &nbsp;\xb7&nbsp; Table &nbsp;\xb7&nbsp; Left col = Right col</div>`+'
    '`<div class="join-list" id="jl-${idx}"></div>`+'
    '`<div class="no-msg" id="nj-${idx}">${noJoinMsg}</div>`+'
    '`</div>`+'
    '`<div class="ts-section">`+'
    '`<div class="ts-stitle">Aggregation</div>`+'
    '`<label class="agg-toggle"><input type="checkbox" id="at-${idx}" onchange="toggleAgg(${idx},this.checked)">`+'
    '`&nbsp;Enable GROUP BY &amp; metrics (creates <code style="color:#63b3ed">analytics.*</code> view)</label>`+'
    '`<div id="ab-${idx}" style="display:none">`+'
    '`<div class="sub-label">Group By columns</div>`+'
    '`<div class="chk-grid" id="gb-${idx}"></div>`+'
    '`<div class="sub-label" style="margin-top:14px">Metrics`+'
    '`<button class="btn-link" style="margin-left:8px" onclick="addMetric(${idx})">+ Add metric</button></div>`+'
    '`<div style="display:none;grid-template-columns:1fr 90px 1fr 28px;gap:8px;margin-bottom:6px" id="mh-${idx}">`+'
    '`<span style="font-size:.7rem;color:#4a5568">Column</span>`+'
    '`<span style="font-size:.7rem;color:#4a5568">Function</span>`+'
    '`<span style="font-size:.7rem;color:#4a5568">Output name (optional)</span>`+'
    '`<span></span></div>`+'
    '`<div id="ml-${idx}"></div>`+'
    '`<div class="no-msg" id="nm-${idx}">No metrics</div>`+'
    '`</div>`+'
    '`</div>`+'
    '`</div>`;'
    'document.getElementById("tables-container").appendChild(div);}'
    'function toggleTs(idx){'
    'const b=document.getElementById(`tsb-${idx}`);'
    'const c=document.getElementById(`chev-${idx}`);'
    'const h=b.style.display==="none";'
    'b.style.display=h?"":"none";c.textContent=h?"▲":"▼";}'
    'function toggleFilterVal(sel){'
    'const v=sel.closest(".filter-row").querySelector(".filter-val");'
    'if(v)v.style.display=["IS NULL","IS NOT NULL"].includes(sel.value)?"none":"";}'
    'function checkEmpty(containerId,noMsgId){'
    'const c=document.getElementById(containerId);'
    'const m=document.getElementById(noMsgId);'
    'if(c&&m)m.style.display=c.children.length?"none":"";}'
    'function addFilter(idx){'
    'const t=state.uploadedTables[idx];'
    'const cols=(t.columns||[]).map(c=>c.name);'
    'const row=document.createElement("div");row.className="filter-row";'
    'row.innerHTML='
    '`<select class="filter-col">${cols.map(n=>`<option>${n}</option>`).join("")}</select>`+'
    '`<select class="filter-op" onchange="toggleFilterVal(this)">${OPS.map(([v,l])=>`<option value="${v}">${l}</option>`).join("")}</select>`+'
    '`<input type="text" class="filter-val" placeholder="value">`+'
    r'`<button class="filter-rm" onclick="this.closest(\'.filter-row\').remove();checkEmpty(\'fl-${idx}\',\'nf-${idx}\')">&#215;</button>`;'
    'document.getElementById(`fl-${idx}`).appendChild(row);'
    'document.getElementById(`nf-${idx}`).style.display="none";}'
    'function addExpr(idx){'
    'const row=document.createElement("div");row.className="expr-row";'
    'row.innerHTML='
    '`<input type="text" class="expr-name" placeholder="column name">`+'
    '`<span class="expr-eq">=</span>`+'
    '`<input type="text" class="expr-val" placeholder="e.g. price * qty">`+'
    r'`<button class="filter-rm" onclick="this.closest(\'.expr-row\').remove();checkEmpty(\'el-${idx}\',\'ne-${idx}\')">&#215;</button>`;'
    'document.getElementById(`el-${idx}`).appendChild(row);'
    'document.getElementById(`ne-${idx}`).style.display="none";}'
    'function addJoin(idx){'
    'const t=state.uploadedTables[idx];'
    'const others=Object.keys(state.rawTables).filter(n=>n!==t.table_name);'
    'if(!others.length)return;'
    'const firstTbl=others[0];'
    'const rightCols=state.rawTables[firstTbl]||[];'
    'const leftCols=(t.columns||[]).map(c=>c.name);'
    'const id=nextSeq(idx,"j");'
    'const row=document.createElement("div");row.className="join-row";'
    'row.innerHTML='
    '`<select class="join-type">${JOIN_TYPES.map(t=>`<option>${t}</option>`).join("")}</select>`+'
    '`<select class="join-tbl" onchange="updateJoinRcols(this,${idx},\'jrcol-${idx}-${id}\')">${others.map(t=>`<option>${t}</option>`).join("")}</select>`+'
    '`<select class="join-lcol">${leftCols.map(c=>`<option>${c}</option>`).join("")}</select>`+'
    '`<span class="join-eq">=</span>`+'
    '`<select class="join-rcol" id="jrcol-${idx}-${id}">${rightCols.map(c=>`<option>${c}</option>`).join("")}</select>`+'
    r'`<button class="filter-rm" onclick="this.closest(\'.join-row\').remove();checkEmpty(\'jl-${idx}\',\'nj-${idx}\')">&#215;</button>`;'
    'document.getElementById(`jl-${idx}`).appendChild(row);'
    'document.getElementById(`nj-${idx}`).style.display="none";}'
    'function updateJoinRcols(sel,idx,rcolId){'
    'const cols=state.rawTables[sel.value]||[];'
    'const rcol=document.getElementById(rcolId);'
    'if(rcol)rcol.innerHTML=cols.map(c=>`<option>${c}</option>`).join("");}'
    'function toggleAgg(idx,on){'
    'document.getElementById(`ab-${idx}`).style.display=on?"":"none";'
    'if(on)refreshGbCols(idx);}'
    'function refreshGbCols(idx){'
    'const t=state.uploadedTables[idx];'
    'const card=document.getElementById(`ts-${idx}`);'
    'const cols=[];'
    'card.querySelectorAll(".col-tbl tbody tr").forEach((tr,ci)=>{'
    'if(tr.querySelector(".col-check").checked){'
    'const c=t.columns[ci]||{};'
    'cols.push(tr.querySelector(".col-out").value.trim()||c.name);}});'
    'document.getElementById(`gb-${idx}`).innerHTML='
    'cols.map(c=>`<label class="chk-item"><input type="checkbox" data-col="${c}">&nbsp;${c}</label>`).join("");}'
    'function addMetric(idx){'
    'const t=state.uploadedTables[idx];'
    'const card=document.getElementById(`ts-${idx}`);'
    'const cols=[];'
    'card.querySelectorAll(".col-tbl tbody tr").forEach((tr,ci)=>{'
    'if(tr.querySelector(".col-check").checked){'
    'const c=t.columns[ci]||{};'
    'cols.push(tr.querySelector(".col-out").value.trim()||c.name);}});'
    'const row=document.createElement("div");row.className="metric-row";'
    'row.innerHTML='
    '`<select>${cols.map(c=>`<option>${c}</option>`).join("")}</select>`+'
    '`<select>${FNS.map(f=>`<option>${f}</option>`).join("")}</select>`+'
    '`<input type="text" placeholder="e.g. total_revenue">`+'
    '`<button style="background:none;border:none;color:#4a5568;cursor:pointer;font-size:1.1rem" onclick="syncMetrics(${idx},this)">&#215;</button>`;'
    'document.getElementById(`ml-${idx}`).appendChild(row);'
    'document.getElementById(`nm-${idx}`).style.display="none";'
    'document.getElementById(`mh-${idx}`).style.display="grid";}'
    'function syncMetrics(idx,btn){'
    'btn.closest(".metric-row").remove();'
    'const has=document.querySelectorAll(`#ml-${idx} .metric-row`).length>0;'
    'document.getElementById(`nm-${idx}`).style.display=has?"none":"";'
    'document.getElementById(`mh-${idx}`).style.display=has?"grid":"none";}'
    'async function doProcess(){'
    'const tables=[];'
    'document.querySelectorAll(".ts-card").forEach(card=>{'
    'const idx=parseInt(card.dataset.tsIdx);'
    'const tblName=card.dataset.tsTable;'
    'const tInfo=state.uploadedTables[idx];'
    'if(!tInfo)return;'
    'const cols=[];'
    'card.querySelectorAll(".col-tbl tbody tr").forEach((tr,ci)=>{'
    'const c=tInfo.columns[ci]||{};'
    'cols.push({name:c.name,output_name:tr.querySelector(".col-out").value.trim(),'
    'type:tr.querySelector(".col-type").value,include:tr.querySelector(".col-check").checked});});'
    'const filters=[];'
    'card.querySelectorAll(".filter-row").forEach(r=>{'
    'const op=r.querySelector(".filter-op").value;'
    'const fv=r.querySelector(".filter-val");'
    'filters.push({column:r.querySelector(".filter-col").value,operator:op,value:fv?fv.value.trim():""});});'
    'const computed_cols=[];'
    'card.querySelectorAll(".expr-row").forEach(r=>{'
    'const nm=r.querySelector(".expr-name").value.trim();'
    'const ex=r.querySelector(".expr-val").value.trim();'
    'if(nm&&ex)computed_cols.push({name:nm,expression:ex});});'
    'const joins=[];'
    'card.querySelectorAll(".join-row").forEach(r=>{'
    'joins.push({join_type:r.querySelector(".join-type").value,'
    'table:r.querySelector(".join-tbl").value,'
    'left_col:r.querySelector(".join-lcol").value,'
    'right_col:r.querySelector(".join-rcol").value});});'
    'const at=document.getElementById(`at-${idx}`);'
    'const gb=[],mx=[];'
    'if(at&&at.checked){'
    'card.querySelectorAll(`#gb-${idx} input:checked`).forEach(inp=>gb.push(inp.dataset.col));'
    'card.querySelectorAll(`#ml-${idx} .metric-row`).forEach(r=>{'
    'const ss=r.querySelectorAll("select");'
    'mx.push({column:ss[0].value,fn:ss[1].value,output_name:r.querySelector("input[type=text]").value.trim()});});}'
    'tables.push({table:tblName,columns:cols,filters,computed_cols,joins,group_by:gb,metrics:mx});});'
    'if(!tables.length){document.getElementById("s2-status").innerHTML=\'<span class="err-msg">No tables</span>\';return;}'
    'const btn=document.getElementById("s2-btn");'
    'const st=document.getElementById("s2-status");'
    'btn.disabled=true;'
    'st.innerHTML="<div class=\\"spin-wrap\\"><div class=\\"spinner\\"></div>&nbsp;Creating pipeline…</div>";'
    'try{'
    'const r=await fetch("/api/workflow/process",{method:"POST",headers:{"Content-Type":"application/json"},'
    'body:JSON.stringify({tables})});'
    'const j=await r.json();'
    'if(r.ok){renderResults(j);goStep(3);}'
    'else{'
    'const det=j&&j.detail;'
    'const msg=typeof det==="string"?det:det?JSON.stringify(det):"Processing failed";'
    'st.innerHTML=\'<span class="err-msg">\'+msg+\'</span>\';btn.disabled=false;}'
    '}catch(e){st.innerHTML="<span class=\\"err-msg\\">Network error</span>";btn.disabled=false;}}'
    'function renderResults(j){'
    'let html="";'
    'for(const v of(j.views||[])){'
    'html+=`<div style="background:#0d1627;border:1px solid #2b4c7e;border-radius:8px;padding:14px;margin-top:8px">`+'
    '`<div style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#63b3ed;margin-bottom:4px">Pipeline</div>`+'
    '`<div style="font-family:monospace;font-size:.8rem;color:#68d391;margin-bottom:8px">${j.pipeline_id}</div>`+'
    '`<div style="font-size:.78rem;color:#718096">`+'
    '`<div>Clean view: <code style="color:#a0aec0">${v.clean_view}</code></div>`;'
    'if(v.agg_view)html+=`<div>Analytics view: <code style="color:#a0aec0">${v.agg_view}</code></div>`;'
    'html+=`</div>`+'
    '`<div style="margin-top:10px"><a class="btn btn-primary btn-sm" href="${j.airflow_url}" target="_blank">View in Airflow &#8599;</a></div>`+'
    '`</div>`;}'
    'document.getElementById("r-pipelines").innerHTML=html;}'
    'function resetWizard(){'
    'state={uploadedTables:[],rawTables:{}};seqs={};'
    'document.getElementById("r-pipelines").innerHTML="";'
    'document.getElementById("tables-container").innerHTML="";'
    'goStep(1);}'
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
    'html,body{height:100%;overflow:hidden}'
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;display:flex;flex-direction:column}'
    '.topbar{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:14px 32px;display:flex;align-items:center;gap:14px;flex-shrink:0}'
    '.logo{width:28px;height:28px;background:#3182ce;border-radius:6px;display:flex;align-items:center;justify-content:center}'
    '.logo svg{width:16px;height:16px}'
    '.topbar h1{font-size:1rem;font-weight:700}'
    '.topbar a{margin-left:auto;font-size:.8rem;color:#63b3ed;text-decoration:none}'
    '.topbar a:hover{text-decoration:underline}'
    '.page{flex:1;min-height:0;overflow-y:auto;max-width:700px;margin:0 auto;padding:24px;width:100%}'
    '.panel-head{margin-bottom:24px}'
    '.panel-head h2{font-size:1.4rem;font-weight:700;margin-bottom:6px}'
    '.panel-head p{color:#a0aec0;font-size:.9rem;line-height:1.6}'
    '.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:20px 24px;margin-bottom:16px}'
    '.drop-zone{border:2px dashed #2d3748;border-radius:10px;padding:36px 24px;text-align:center;'
    'cursor:pointer;transition:border-color .2s,background .2s;position:relative}'
    '.drop-zone:hover,.drop-zone.drag{border-color:#3182ce;background:#0d1627}'
    '.drop-zone input{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}'
    '.dz-icon{font-size:2rem;margin-bottom:10px;display:block}'
    '.dz-main{font-size:.9rem;color:#a0aec0;margin-bottom:4px}'
    '.dz-sub{font-size:.75rem;color:#4a5568}'
    '.file-list{display:flex;flex-direction:column;gap:10px;margin-bottom:16px}'
    '.file-row{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:14px 18px;'
    'display:flex;align-items:center;gap:12px;transition:border-color .15s}'
    '.file-row:hover{border-color:#4a5568}'
    '.file-icon{font-size:1.3rem;flex-shrink:0}'
    '.file-info{flex:1;min-width:0}'
    '.file-name{font-size:.85rem;font-weight:600;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px}'
    '.file-size{font-size:.72rem;color:#4a5568}'
    '.file-tbl{display:flex;align-items:center;gap:4px;flex-shrink:0}'
    '.file-tbl span{font-size:.8rem;color:#4a5568;white-space:nowrap}'
    '.tbl-input{width:130px;background:#0d1117;border:1px solid #2d3748;border-radius:5px;'
    'padding:5px 8px;color:#e2e8f0;font-size:.8rem;outline:none}'
    '.tbl-input:focus{border-color:#3182ce}'
    '.file-status{font-size:.75rem;white-space:nowrap;flex-shrink:0;min-width:90px;text-align:right}'
    '.file-status.pending{color:#4a5568}'
    '.file-status.uploading{color:#63b3ed}'
    '.file-status.done{color:#48bb78}'
    '.file-status.error{color:#fc8181}'
    '.filter-rm{background:none;border:none;color:#4a5568;cursor:pointer;font-size:1.2rem;padding:0 2px;line-height:1;flex-shrink:0}'
    '.filter-rm:hover{color:#fc8181}'
    '.panel-footer{display:flex;align-items:center;gap:12px;margin-top:4px}'
    '.btn{display:inline-flex;align-items:center;gap:6px;padding:10px 22px;border-radius:8px;'
    'font-size:.875rem;font-weight:600;cursor:pointer;border:none;transition:opacity .15s,background .15s}'
    '.btn:disabled{opacity:.4;cursor:not-allowed}'
    '.btn-primary{background:#3182ce;color:#fff}.btn-primary:hover:not(:disabled){background:#2b6cb0}'
    '.btn-sm{padding:6px 14px!important;font-size:.8rem!important}'
    '.btn-ghost{background:#2d3748;color:#e2e8f0}.btn-ghost:hover{background:#3a4459}'
    '.spin-wrap{display:flex;align-items:center;gap:8px;font-size:.85rem;color:#718096}'
    '.spinner{width:14px;height:14px;border:2px solid #2d3748;border-top-color:#63b3ed;border-radius:50%;animation:spin .6s linear infinite}'
    '@keyframes spin{to{transform:rotate(360deg)}}'
    '.err-msg{color:#fc8181;font-size:.85rem}'
    '.success-card{background:#0d1f14;border:1px solid #276749;border-radius:10px;padding:16px 20px}'
    '.success-card .s-title{font-size:.85rem;font-weight:700;color:#68d391;margin-bottom:10px}'
    '.loc-row{font-size:.82rem;color:#68d391;display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;line-height:1.5}'
    '.loc-row code{font-family:monospace;color:#63b3ed;font-size:.78rem;word-break:break-all}'
    '.use-btn-wrap{margin-top:10px}'
    '</style></head><body>'
    '<div class="topbar">'
    '<div class="logo"><svg viewBox="0 0 16 16" fill="none">'
    '<rect width="16" height="16" rx="3" fill="#3182ce"/>'
    '<path d="M4 8h8M8 4v8" stroke="white" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg></div>'
    '<h1>DataFabrik — Upload Data</h1>'
    '<a href="/">&#8592; Back to Portal</a>'
    '</div>'
    '<div class="page">'
    '<div class="panel-head">'
    '<h2>Upload your data</h2>'
    '<p>Drop one or more CSV files. Up to <strong>1,000 rows</strong> per file are sampled into Postgres '
    '<code style="color:#63b3ed">raw</code> schema; the full files are stored in MinIO.</p>'
    '</div>'
    '<div class="card">'
    '<div class="drop-zone" id="dz">'
    '<input type="file" id="fi" accept=".csv" multiple>'
    '<span class="dz-icon">&#128194;</span>'
    '<div class="dz-main">Drop CSV files here, or click to browse</div>'
    '<div class="dz-sub">.csv files only &nbsp;\xb7&nbsp; multiple files supported</div>'
    '</div>'
    '</div>'
    '<div class="file-list" id="file-list"></div>'
    '<div class="panel-footer" id="up-footer" style="display:none">'
    '<button class="btn btn-primary" id="up-btn" onclick="uploadAll()">Upload All &#8599;</button>'
    '<div id="up-status"></div>'
    '</div>'
    '<div id="results"></div>'
    '</div>'
    '<script>'
    'let queue=[];let qseq=0;let allDone=[];'
    'const dz=document.getElementById("dz");'
    'dz.addEventListener("dragover",e=>{e.preventDefault();dz.classList.add("drag")});'
    'dz.addEventListener("dragleave",()=>dz.classList.remove("drag"));'
    'dz.addEventListener("drop",e=>{e.preventDefault();dz.classList.remove("drag");addFiles(e.dataTransfer.files);});'
    'document.getElementById("fi").addEventListener("change",e=>{addFiles(e.target.files);e.target.value="";});'
    'function addFiles(files){'
    'for(const f of files){'
    'if(!f.name.toLowerCase().endsWith(".csv"))continue;'
    'const id=++qseq;'
    'queue.push({id,file:f,status:"pending"});'
    'const stem=f.name.replace(/\\.csv$/i,"").replace(/[^a-z0-9]+/gi,"_").toLowerCase();'
    'const row=document.createElement("div");row.className="file-row";row.id=`fr${id}`;'
    'row.innerHTML='
    '`<span class="file-icon">&#128196;</span>`+'
    '`<div class="file-info"><div class="file-name">${f.name}</div><div class="file-size">${(f.size/1024).toFixed(1)} KB</div></div>`+'
    '`<div class="file-tbl"><span>raw.</span><input type="text" class="tbl-input" value="${stem}" placeholder="table_name"></div>`+'
    '`<div class="file-status pending" id="fs${id}">Pending</div>`+'
    '`<button class="filter-rm" onclick="removeRow(${id})">&#215;</button>`;'
    'document.getElementById("file-list").appendChild(row);}'
    'document.getElementById("up-footer").style.display=queue.length?"":"none";}'
    'function removeRow(id){'
    'queue=queue.filter(i=>i.id!==id);'
    'const r=document.getElementById(`fr${id}`);if(r)r.remove();'
    'document.getElementById("up-footer").style.display=queue.length?"":"none";}'
    'function setStatus(id,cls,text){'
    'const el=document.getElementById(`fs${id}`);'
    'if(el){el.className="file-status "+cls;el.textContent=text;}}'
    'async function uploadAll(){'
    'const pending=queue.filter(i=>i.status==="pending");'
    'if(!pending.length)return;'
    'const btn=document.getElementById("up-btn");'
    'btn.disabled=true;'
    'const done=[];'
    'for(const item of pending){'
    'const row=document.getElementById(`fr${item.id}`);'
    'if(!row)continue;'
    'const tbl=row.querySelector(".tbl-input").value.trim();'
    'if(!tbl){setStatus(item.id,"error","Need table name");continue;}'
    'setStatus(item.id,"uploading","Uploading…");'
    'try{'
    'const fd=new FormData();fd.append("table",tbl);fd.append("file",item.file);'
    'const r=await fetch("/api/workflow/upload",{method:"POST",body:fd});'
    'const j=await r.json();'
    'if(r.ok){'
    'item.status="done";item.result=j;done.push(j);'
    'const note=j.sampled?` (${j.rows.toLocaleString()} of ${j.total_rows.toLocaleString()} rows)`:`  (${j.rows.toLocaleString()} rows)`;'
    'setStatus(item.id,"done","✓ raw."+j.table_name+note);'
    '}else{'
    'item.status="error";'
    'setStatus(item.id,"error","✗ "+(j.detail||"Failed"));}}'
    'catch(e){item.status="error";setStatus(item.id,"error","✗ Network error");}}'
    'btn.disabled=false;'
    'if(done.length){allDone=done;showResults(done);}}'
    'const isEmbed=location.search.includes("embed=1");'
    'function showResults(results){'
    'const el=document.getElementById("results");'
    'let html="";'
    'for(const j of results){'
    'const note=j.sampled?` (sampled ${j.rows.toLocaleString()} of ${j.total_rows.toLocaleString()})`:` — ${j.rows.toLocaleString()} rows`;'
    'html+=`<div class="success-card" style="margin-bottom:12px">`+'
    '`<div class="s-title">✓ raw.${j.table_name}${note}</div>`+'
    '`<div class="loc-row">✓ Postgres: <code>raw.${j.table_name}</code></div>`+'
    '`<div class="loc-row">✓ MinIO: <code>${j.s3_bucket}/${j.s3_key}</code></div>`+'
    '`</div>`;}'
    'if(isEmbed&&results.length){'
    'html+=`<div style="margin-top:16px"><button class="btn btn-primary" onclick="useAllTables()">`+'
    '`Use ${results.length} table${results.length>1?"s":""} in Pipeline Builder &#8594;</button></div>`;}'
    'el.innerHTML=html;}'
    'function useAllTables(){'
    'window.parent.postMessage({type:"datafabrik_upload",tables:allDone},"*");}'
    '</script>'
    '<script>if(location.search.includes("embed=1")){'
    'document.documentElement.style.height="auto";'
    'document.body.style.cssText+="height:auto;overflow:visible;";'
    'document.querySelectorAll(\'.topbar\').forEach(e=>e.style.display="none");'
    'document.querySelector(\'.page\').style.cssText+="padding:8px 0;overflow:visible;";'
    'const ro=new ResizeObserver(()=>{'
    'window.parent.postMessage({type:"datafabrik_height",h:document.body.scrollHeight},"*");});'
    'ro.observe(document.body);}'
    '</script>'
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
def portal() -> HTMLResponse:
    """DataFabrik portal — unified UI for all platform tools."""
    return HTMLResponse(content=_PORTAL_HTML, headers={"Cache-Control": "no-store"})


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
