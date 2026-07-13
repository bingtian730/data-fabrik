function Arrow({ label }) {
  return (
    <div className="guide-arrow">
      <div className="guide-arrow-line" />
      <div className="guide-arrow-head" />
      {label && <div className="guide-arrow-label">{label}</div>}
    </div>
  )
}

function Node({ n, step, icon, title, desc, tags }) {
  return (
    <div className={`guide-node n${n}`}>
      <div className="guide-node-num">Step {step}</div>
      <div className="guide-node-icon">{icon}</div>
      <div className="guide-node-title">{title}</div>
      <div className="guide-node-desc">{desc}</div>
      <div className="guide-node-tags">
        {tags.map(t => <span key={t} className="guide-tag">{t}</span>)}
      </div>
    </div>
  )
}

export default function Guide() {
  return (
    <div className="section active" style={{ flexDirection: 'column' }}>
      <div className="sec-header">
        <h2>🗺️ Pipeline Guide</h2>
        <span className="sub">How local CSV data flows through the platform</span>
      </div>
      <div className="guide-wrap">
        <div className="guide-flow">
          <Node n={1} step={1} icon="📄" title="Upload CSV"
            desc="Drop a local CSV file in the Workflow Wizard to start a new pipeline"
            tags={['Workflow Wizard', 'local file']} />

          <Arrow label="staged as raw" />

          <Node n={2} step={2} icon="🗄️" title="MinIO Storage"
            desc={<>CSV is written to the <code>datafabrik-raw</code> bucket under <code>wizard/</code></>}
            tags={['object store', ':9001']} />

          <Arrow label="triggers DAG" />

          <Node n={3} step={3} icon="✈️" title="Airflow Pipeline"
            desc="A DAG is generated and triggered. It reads raw data and runs the SQL transformation"
            tags={['DAG run', ':8082']} />

          <Arrow label="writes views" />

          <Node n={4} step={4} icon="🐘" title="Postgres"
            desc={<>Cleaned data lands as a view in <code>clean.</code> schema for querying</>}
            tags={['clean schema', 'view']} />

          <Arrow label="exports CSV" />

          <Node n={5} step={5} icon="🗄️" title="MinIO Clean"
            desc={<>Transformed CSV snapshot written to <code>datafabrik-clean</code> bucket</>}
            tags={['datafabrik-clean', 'wizard/']} />
        </div>
      </div>
    </div>
  )
}
