import { useEffect } from 'react'

export default function Toast({ toasts, dismiss }) {
  return (
    <div className="toast-container">
      {toasts.map(t => <ToastItem key={t.id} toast={t} dismiss={dismiss} />)}
    </div>
  )
}

function ToastItem({ toast, dismiss }) {
  useEffect(() => {
    const id = setTimeout(() => dismiss(toast.id), 4000)
    return () => clearTimeout(id)
  }, [toast.id, dismiss])
  return <div className={`toast ${toast.type}`}>{toast.msg}</div>
}
