const BASE = import.meta.env.VITE_API_BASE ?? ''

async function req(path, opts = {}) {
  const res = await fetch(BASE + path, opts)
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail ?? `HTTP ${res.status}`)
  }
  return res.json()
}

export const api = {
  pipelines:    () => req('/api/pipelines'),
  runs:         (limit = 8) => req(`/api/runs?limit=${limit}`),
  health:       (svc) => req(`/api/admin/health/${svc}`),
  trigger:      (id) => req(`/api/pipelines/${id}/trigger`, { method: 'POST' }),
  pause:        (id, paused) => req(`/api/pipelines/${id}/pause?paused=${paused}`, { method: 'PATCH' }),
  deleteDag:    (id) => req(`/api/pipelines/${id}/dag`, { method: 'DELETE' }),
  upload:       (table, file) => {
    const fd = new FormData()
    fd.append('table', table)
    fd.append('file', file)
    return req('/api/workflow/upload', { method: 'POST', body: fd })
  },
  process:      (payload) => req('/api/workflow/process', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }),
}
