import { useState, useEffect, useCallback } from 'react'
import Badge from './Badge.jsx'
import { api } from '../api.js'

const SVCS = ['postgres', 'airflow', 'minio']

export default function Home({ onOpenDag, toast }) {
  const [pipes, setPipes]   = useState([])
  const [runs, setRuns]     = useState([])
  const [health, setHealth] = useState({})
  const [pipesTs, setPipesTs] = useState('')
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [p, r] = await Promise.all([api.pipelines(), api.runs(8)])
      setPipes(p)
      setRuns(r)
      setPipesTs('Updated ' + new Date().toLocaleTimeString())
    } catch { /* ignore */ }
    setLoading(false)
  }, [])

  const checkHealth = useCallback(async () => {
    setHealth(h => Object.fromEntries(SVCS.map(s => [s, { ...h[s], status: 'checking' }])))
    await Promise.all(SVCS.map(async svc => {
      try {
        const d = await api.health(svc)
        setHealth(h => ({ ...h, [svc]: { status: d.status === 'ok' ? 'ok' : 'error', latency: d.latency_ms } }))
      } catch {
        setHealth(h => ({ ...h, [svc]: { status: 'error' } }))
      }
    }))
  }, [])

  useEffect(() => { load(); checkHealth() }, [load, checkHealth])

  const stats = {
    total:  pipes.length,
    ok:     pipes.filter(p => p.state === 'success').length,
    fail:   pipes.filter(p => p.state === 'failed').length,
    paused: pipes.filter(p => p.state === 'paused').length,
  }

  async function trigger(id, btn) {
    btn.disabled = true
    try {
      await api.trigger(id)
      toast('✓ Triggered ' + id, 'ok')
      setTimeout(load, 2000)
    } catch (e) { toast('✗ ' + e.message, 'err') }
    btn.disabled = false
  }

  async function togglePause(id, isPaused) {
    try {
      await api.pause(id, !isPaused)
      toast(isPaused ? '▶ Resumed ' + id : '⏸ Paused ' + id, 'ok')
      load()
    } catch (e) { toast('✗ ' + e.message, 'err') }
  }

  async function deleteDag(id) {
    if (!confirm(`Delete pipeline "${id}"? This cannot be undone.`)) return
    try {
      await api.deleteDag(id)
      toast('🗑 Deleted ' + id, 'ok')
      load()
    } catch (e) { toast('✗ ' + e.message, 'err') }
  }

  return (
    <div className="section active" style={{ flexDirection: 'column' }}>
      <div className="sec-header">
        <h2>🏠 Home</h2>
      </div>
      <div className="sec-body">
        {/* Stat cards */}
        <div className="stat-cards">
          {[
            { label: 'Total', value: stats.total, color: '#90cdf4' },
            { label: 'Healthy', value: stats.ok,   color: '#68d391' },
            { label: 'Failed',  value: stats.fail,  color: '#fc8181' },
            { label: 'Paused',  value: stats.paused, color: '#f6ad55' },
          ].map(s => (
            <div key={s.label} className="stat-card">
              <div className="stat-label">{s.label}</div>
              <div className="stat-value" style={{ color: s.color }}>{s.value}</div>
            </div>
          ))}
        </div>

        {/* Service health */}
        <div className="sec-row" style={{ marginBottom: 10 }}>
          <h3>Platform Health</h3>
          <button className="btn btn-ghost btn-sm" onClick={checkHealth}>Refresh</button>
        </div>
        <div className="svc-grid">
          {SVCS.map(svc => {
            const h = health[svc] ?? {}
            return (
              <div key={svc} className="svc-card">
                <span className={`svc-dot ${h.status ?? ''}`} />
                <span className="svc-name">{svc}</span>
                <span className="svc-status-txt">
                  {h.status === 'ok' ? `${h.latency ?? '—'}ms` : h.status ?? '—'}
                </span>
              </div>
            )
          })}
        </div>

        {/* Pipelines */}
        <div className="sec-row">
          <h3>Pipelines</h3>
          <span className="sec-ts">{pipesTs}</span>
        </div>
        {loading ? (
          <div className="loading"><span className="spinner" /> Loading…</div>
        ) : pipes.length === 0 ? (
          <div className="empty">No pipelines found in Airflow.</div>
        ) : (
          <table>
            <thead><tr>
              <th>Pipeline</th><th>State</th><th>Last Run</th>
              <th>Duration</th><th>30d Success</th><th>Rows</th><th>Actions</th>
            </tr></thead>
            <tbody>
              {pipes.map(p => {
                const pct = p.pct_30d
                const tier = pct == null ? '' : pct >= 90 ? 'hi' : pct >= 70 ? 'mid' : 'lo'
                const isPaused  = p.state === 'paused'
                const isRunning = p.state === 'running'
                return (
                  <tr key={p.id}>
                    <td>
                      <a href="#" onClick={e => { e.preventDefault(); onOpenDag(p.id) }}
                        style={{ color: '#90cdf4', textDecoration: 'none', fontWeight: 700 }}>
                        {p.id}
                      </a>
                    </td>
                    <td><Badge state={p.state} /></td>
                    <td className="dim">{p.last_run}</td>
                    <td className="dim">{p.duration}</td>
                    <td>
                      {pct != null ? (
                        <>
                          <span style={{ fontWeight: 600, color: tier === 'hi' ? '#48bb78' : tier === 'mid' ? '#ecc94b' : '#fc8181' }}>{pct}%</span>
                          <div className="pct-bar"><div className={`pct-fill pct-${tier}`} style={{ width: pct + '%' }} /></div>
                          <span className="dim">{p.ok_30d}/{p.runs_30d}</span>
                        </>
                      ) : <span className="dim">no data</span>}
                    </td>
                    <td className="dim">{p.rows}</td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        <button className="btn btn-trigger btn-sm" disabled={isRunning}
                          onClick={e => trigger(p.id, e.currentTarget)}>▶ Run</button>
                        <button className={`btn btn-sm ${isPaused ? 'btn-success' : 'btn-ghost'}`}
                          onClick={() => togglePause(p.id, isPaused)}>
                          {isPaused ? '▶ Resume' : '⏸ Pause'}
                        </button>
                        <button className="btn btn-danger btn-sm"
                          onClick={() => deleteDag(p.id)}>🗑</button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}

        {/* Recent runs */}
        <div className="sec-row">
          <h3>Recent Runs</h3>
        </div>
        {runs.length === 0 ? (
          <div className="empty">No runs recorded yet.</div>
        ) : (
          <table>
            <thead><tr>
              <th>Pipeline</th><th>State</th><th>Started</th><th>Duration</th>
            </tr></thead>
            <tbody>
              {runs.map((r, i) => (
                <tr key={i}>
                  <td><strong>{r.pipeline_id}</strong></td>
                  <td><Badge state={r.state} /></td>
                  <td className="dim">{r.started_at}</td>
                  <td className="dim">{r.duration}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
