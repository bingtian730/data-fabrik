import { useState, useCallback, useRef } from 'react'
import Home from './components/Home.jsx'
import Wizard from './components/Wizard.jsx'
import Guide from './components/Guide.jsx'
import Toast from './components/Toast.jsx'

const AIRFLOW_URL  = import.meta.env.VITE_AIRFLOW_URL  ?? 'http://localhost:8082'
const MINIO_URL    = import.meta.env.VITE_MINIO_URL    ?? 'http://localhost:9002'

const NAV = [
  { section: 'platform', items: [
    { id: 'home',    icon: '🏠', label: 'Home' },
  ]},
  { section: 'tools', items: [
    { id: 'airflow', icon: '✈️', label: 'Airflow' },
    { id: 'minio',   icon: '🗄️', label: 'MinIO' },
    { id: 'wizard',  icon: '🧹', label: 'Workflow Wizard' },
  ]},
  { section: 'help', items: [
    { id: 'guide',   icon: '🗺️', label: 'Pipeline Guide' },
  ]},
]

const IFRAMES = { airflow: AIRFLOW_URL, minio: MINIO_URL }

export default function App() {
  const [active, setActive]     = useState('home')
  const [toasts, setToasts]     = useState([])
  const [toastSeq, setToastSeq] = useState(0)
  const frameLoaded = useRef({})

  const toast = useCallback((msg, type = 'ok') => {
    const id = toastSeq + 1
    setToastSeq(id)
    setToasts(ts => [...ts, { id, msg, type }])
  }, [toastSeq])

  const dismiss = useCallback(id => {
    setToasts(ts => ts.filter(t => t.id !== id))
  }, [])

  function nav(id) {
    setActive(id)
    if (IFRAMES[id] && !frameLoaded.current[id]) {
      const fr = document.getElementById('frame-' + id)
      if (fr) { fr.src = IFRAMES[id]; frameLoaded.current[id] = true }
    }
  }

  function openAirflowDag(dagId) {
    const dagUrl = AIRFLOW_URL + '/dags/' + dagId + '/grid'
    const fr = document.getElementById('frame-airflow')
    if (fr) fr.src = dagUrl
    frameLoaded.current['airflow'] = true
    setActive('airflow')
  }

  return (
    <div className="app">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sb-logo">
          <div className="sb-logo-icon">
            <svg viewBox="0 0 16 16" fill="none">
              <rect width="16" height="16" rx="3" fill="#3182ce" />
              <path d="M4 8h8M8 4v8" stroke="white" strokeWidth="1.8" strokeLinecap="round" />
            </svg>
          </div>
          <span className="sb-logo-text">DataFabrik</span>
        </div>
        <nav className="sb-nav">
          {NAV.map(group => (
            <div key={group.section}>
              <div className="sb-section">{group.section}</div>
              {group.items.map(item => (
                <button
                  key={item.id}
                  className={`nav-btn${active === item.id ? ' active' : ''}`}
                  onClick={() => nav(item.id)}
                >
                  <span className="icon">{item.icon}</span>
                  {item.label}
                </button>
              ))}
            </div>
          ))}
        </nav>
      </aside>

      {/* Main content */}
      <main className="main">
        {/* Home */}
        {active === 'home' && (
          <Home onOpenDag={openAirflowDag} toast={toast} />
        )}

        {/* Wizard */}
        {active === 'wizard' && (
          <Wizard toast={toast} />
        )}

        {/* Guide */}
        {active === 'guide' && <Guide />}

        {/* Airflow iframe */}
        <div className={`section iframe-section${active === 'airflow' ? ' active' : ''}`}
          style={{ display: active === 'airflow' ? 'flex' : 'none' }}>
          <div className="iframe-wrap">
            <div className="iframe-bar">
              <span>✈️ Airflow — Pipeline Orchestration</span>
              <a href={AIRFLOW_URL} target="_blank" rel="noreferrer">Open in new tab ↗</a>
            </div>
            <iframe id="frame-airflow" title="Airflow" allowFullScreen />
          </div>
        </div>

        {/* MinIO iframe */}
        <div className={`section iframe-section${active === 'minio' ? ' active' : ''}`}
          style={{ display: active === 'minio' ? 'flex' : 'none' }}>
          <div className="iframe-wrap">
            <div className="iframe-bar">
              <span>🗄️ MinIO — Object Storage &nbsp;·&nbsp; <code>minioadmin / minioadmin</code></span>
              <a href={MINIO_URL} target="_blank" rel="noreferrer">Open in new tab ↗</a>
            </div>
            <iframe id="frame-minio" title="MinIO" allowFullScreen />
          </div>
        </div>
      </main>

      <Toast toasts={toasts} dismiss={dismiss} />
    </div>
  )
}
