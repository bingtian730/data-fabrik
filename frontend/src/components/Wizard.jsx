import { useState, useRef } from 'react'
import { api } from '../api.js'

const TYPES = ['TEXT', 'INTEGER', 'NUMERIC', 'BOOLEAN', 'DATE', 'TIMESTAMPTZ']

/* ── Step indicator ── */
function StepBar({ step }) {
  const steps = ['Upload CSV', 'Configure', 'Results']
  return (
    <div className="step-indicator">
      {steps.map((label, i) => {
        const n = i + 1
        const cls = n < step ? 'si done' : n === step ? 'si active' : 'si'
        return (
          <div key={n} style={{ display: 'contents' }}>
            {i > 0 && <div className="si-sep" />}
            <div className={cls}>
              <div className="si-circle">{n < step ? '✓' : n}</div>
              {label}
            </div>
          </div>
        )
      })}
    </div>
  )
}

/* ── Step 1: Upload ── */
function UploadStep({ onDone, toast }) {
  const [queue, setQueue]   = useState([])
  const [seq, setSeq]       = useState(0)
  const [isDrag, setIsDrag] = useState(false)
  const inputRef = useRef()

  function addFiles(files) {
    const csvs = Array.from(files).filter(f => f.name.toLowerCase().endsWith('.csv'))
    if (!csvs.length) return
    setQueue(q => {
      let s = seq
      const newItems = csvs.map(f => {
        s++
        const stem = f.name.replace(/\.csv$/i, '').replace(/[^a-z0-9]+/gi, '_').toLowerCase()
        return { id: s, file: f, table: stem, status: 'pending' }
      })
      setSeq(s)
      return [...q, ...newItems]
    })
  }

  function removeRow(id) { setQueue(q => q.filter(i => i.id !== id)) }
  function setTable(id, val) { setQueue(q => q.map(i => i.id === id ? { ...i, table: val } : i)) }
  function setStatus(id, status, note) { setQueue(q => q.map(i => i.id === id ? { ...i, status, note } : i)) }

  async function uploadAll() {
    const pending = queue.filter(i => i.status === 'pending')
    const done = []
    for (const item of pending) {
      if (!item.table.trim()) { setStatus(item.id, 'error', 'Need table name'); continue }
      setStatus(item.id, 'uploading', 'Uploading…')
      try {
        const j = await api.upload(item.table.trim(), item.file)
        const note = j.sampled
          ? `✓ raw.${j.table_name} (${j.rows.toLocaleString()} of ${j.total_rows.toLocaleString()} rows)`
          : `✓ raw.${j.table_name} (${j.rows.toLocaleString()} rows)`
        setStatus(item.id, 'done', note)
        done.push(j)
      } catch (e) {
        setStatus(item.id, 'error', '✗ ' + e.message)
      }
    }
    if (done.length) onDone(done)
  }

  const pending = queue.filter(i => i.status === 'pending')

  return (
    <div>
      <div
        className={`drop-zone${isDrag ? ' drag' : ''}`}
        style={{ marginBottom: 16 }}
        onDragOver={e => { e.preventDefault(); setIsDrag(true) }}
        onDragLeave={() => setIsDrag(false)}
        onDrop={e => { e.preventDefault(); setIsDrag(false); addFiles(e.dataTransfer.files) }}
        onClick={() => inputRef.current.click()}
      >
        <input ref={inputRef} type="file" accept=".csv" multiple
          onChange={e => { addFiles(e.target.files); e.target.value = '' }} style={{ display: 'none' }} />
        <span className="dz-icon">📂</span>
        <div className="dz-main">Drop CSV files here, or click to browse</div>
        <div className="dz-sub">.csv only · multiple files supported</div>
      </div>

      {queue.map(item => (
        <div key={item.id} className="file-row">
          <span className="file-icon">📄</span>
          <div className="file-info">
            <div className="file-name">{item.file.name}</div>
            <div className="file-size">{(item.file.size / 1024).toFixed(1)} KB</div>
          </div>
          <div className="file-tbl">
            <span>raw.</span>
            <input className="tbl-input" value={item.table}
              onChange={e => setTable(item.id, e.target.value)}
              disabled={item.status !== 'pending'} />
          </div>
          <span className={`file-status ${item.status}`}>{item.note ?? item.status}</span>
          {item.status === 'pending' &&
            <button className="rm-btn" onClick={() => removeRow(item.id)}>×</button>}
        </div>
      ))}

      {queue.length > 0 && (
        <div className="panel-footer">
          <button className="btn btn-primary" onClick={uploadAll} disabled={!pending.length}>
            Upload All ↗
          </button>
        </div>
      )}
    </div>
  )
}

/* ── Step 2: Configure columns ── */
function ConfigStep({ tables, onDone }) {
  const [configs, setConfigs] = useState(() =>
    tables.map(t => ({
      table: t.table_name,
      columns: t.columns.map(c => ({ ...c, include: true })),
      filters: [],
    }))
  )

  function toggleCol(ti, ci) {
    setConfigs(cs => cs.map((c, i) => i !== ti ? c : {
      ...c, columns: c.columns.map((col, j) => j !== ci ? col : { ...col, include: !col.include })
    }))
  }

  function setType(ti, ci, type) {
    setConfigs(cs => cs.map((c, i) => i !== ti ? c : {
      ...c, columns: c.columns.map((col, j) => j !== ci ? col : { ...col, type })
    }))
  }

  async function submit() {
    const payload = {
      tables: configs.map(c => ({
        table: c.table,
        columns: c.columns.filter(col => col.include).map(col => ({ name: col.name, type: col.type })),
        filters: [],
        joins: [],
        computed_cols: [],
        group_by: [],
        metrics: [],
      }))
    }
    const result = await api.process(payload)
    onDone(result)
  }

  return (
    <div>
      {configs.map((cfg, ti) => (
        <div key={cfg.table} style={{ marginBottom: 24 }}>
          <div className="sec-row" style={{ marginBottom: 10 }}>
            <h3>raw.<strong>{cfg.table}</strong></h3>
          </div>
          {cfg.columns.map((col, ci) => (
            <div key={col.name} className="col-card">
              <div className="col-row">
                <div className="col-toggle">
                  <input type="checkbox" checked={col.include} onChange={() => toggleCol(ti, ci)} />
                </div>
                <span className="col-name">{col.name}</span>
                <select className="type-select" value={col.type} onChange={e => setType(ti, ci, e.target.value)}>
                  {TYPES.map(t => <option key={t}>{t}</option>)}
                </select>
                <span className="col-samples dim">{col.samples?.join(', ')}</span>
              </div>
            </div>
          ))}
        </div>
      ))}
      <div className="panel-footer">
        <button className="btn btn-primary" onClick={submit}>Build Pipeline →</button>
      </div>
    </div>
  )
}

/* ── Step 3: Results ── */
function ResultsStep({ result, onReset }) {
  return (
    <div>
      {result.pipelines?.map(p => (
        <div key={p.pipeline_id} className="success-card">
          <div className="s-title">✓ Pipeline created: {p.pipeline_id}</div>
          <div className="loc-row">Postgres view: <code>clean.{p.pipeline_id.replace(/^wiz_/, '').replace(/_\d+$/, '')}</code></div>
          {p.dag_run_id && <div className="loc-row">Airflow run: <code>{p.dag_run_id}</code></div>}
        </div>
      ))}
      {result.error && <div className="err-msg">✗ {result.error}</div>}
      <div className="panel-footer">
        <button className="btn btn-ghost" onClick={onReset}>← Start over</button>
      </div>
    </div>
  )
}

/* ── Main Wizard ── */
export default function Wizard({ toast }) {
  const [step, setStep]       = useState(1)
  const [uploads, setUploads] = useState([])
  const [result, setResult]   = useState(null)

  function handleUploaded(done) {
    setUploads(done)
    if (done.length) setStep(2)
  }

  async function handleConfig(payload) {
    try {
      const r = await api.process(payload)
      setResult(r)
      setStep(3)
    } catch (e) {
      toast('✗ ' + e.message, 'err')
    }
  }

  function reset() { setStep(1); setUploads([]); setResult(null) }

  return (
    <div className="section active" style={{ flexDirection: 'column' }}>
      <div className="sec-header">
        <h2>🧹 Workflow Wizard</h2>
        <span className="sub">Upload CSV, clean data, build pipelines</span>
      </div>
      <div className="sec-body">
        <div className="wizard-wrap">
          <StepBar step={step} />
          {step === 1 && <UploadStep onDone={handleUploaded} toast={toast} />}
          {step === 2 && <ConfigStep tables={uploads} onDone={handleConfig} />}
          {step === 3 && <ResultsStep result={result ?? {}} onReset={reset} />}
        </div>
      </div>
    </div>
  )
}
