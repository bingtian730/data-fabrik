export default function Badge({ state }) {
  const map = {
    success: ['badge-success', '✓ success'],
    failed:  ['badge-failed',  '✗ failed'],
    running: ['badge-running', '⟳ running'],
    paused:  ['badge-paused',  '⏸ paused'],
  }
  const [cls, label] = map[state] ?? ['badge-default', state ?? '—']
  return <span className={`badge ${cls}`}>{label}</span>
}
