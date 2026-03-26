import { useEffect, useState } from 'react'
import { createMigration, fetchMigrationJobs } from '../api/migrations'
import { fetchUsers } from '../api/users'
import StatusBadge from '../components/StatusBadge'
import { useAuth } from '../contexts/AuthContext'

function MigrationsPage() {
  const { user } = useAuth()
  const isSuperAdmin = user?.role === 'SUPER_ADMIN'
  const [items, setItems] = useState([])
  const [users, setUsers] = useState([])
  const [selectedUserId, setSelectedUserId] = useState('')
  const [form, setForm] = useState({ vm_name: '', source: '', destination: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (isSuperAdmin) {
      fetchUsers().then(setUsers).catch(() => {})
    }
  }, [isSuperAdmin])

  useEffect(() => {
    loadMigrations()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedUserId])

  async function loadMigrations() {
    try {
      const data = await fetchMigrationJobs({ userId: selectedUserId || undefined })
      setItems(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load migrations.')
    }
  }

  async function handleCreate(event) {
    event.preventDefault()
    setSubmitting(true)
    try {
      await createMigration(form)
      setForm({ vm_name: '', source: '', destination: '' })
      await loadMigrations()
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to create migration.')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>Migrations</h2>
          <p>Create migrations and review scoped job history.</p>
        </div>
        {isSuperAdmin ? (
          <div className="header-actions">
            <select value={selectedUserId} onChange={(event) => setSelectedUserId(event.target.value)}>
              <option value="">All users</option>
              {users.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.username}
                </option>
              ))}
            </select>
          </div>
        ) : null}
      </div>

      {error ? <div className="alert error">{error}</div> : null}

      <div className="panel migration-form-panel">
        <h3>Create migration</h3>
        <form className="inline-form" onSubmit={handleCreate}>
          <input
            placeholder="VM name"
            value={form.vm_name}
            onChange={(event) => setForm((current) => ({ ...current, vm_name: event.target.value }))}
            required
          />
          <input
            placeholder="Source"
            value={form.source}
            onChange={(event) => setForm((current) => ({ ...current, source: event.target.value }))}
            required
          />
          <input
            placeholder="Destination"
            value={form.destination}
            onChange={(event) => setForm((current) => ({ ...current, destination: event.target.value }))}
            required
          />
          <button className="primary-btn" disabled={submitting} type="submit">
            {submitting ? 'Creating...' : 'Create'}
          </button>
        </form>
      </div>

      <div className="panel">
        <div className="toolbar">
          <h3>Migration list</h3>
          <button className="secondary-btn" onClick={loadMigrations}>
            Refresh
          </button>
        </div>
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>VM</th>
                <th>User</th>
                <th>Source</th>
                <th>Destination</th>
                <th>Status</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.id}>
                  <td>{item.vm_name}</td>
                  <td>{item.user?.username || '-'}</td>
                  <td>{item.source || '-'}</td>
                  <td>{item.destination || '-'}</td>
                  <td><StatusBadge status={item.status} /></td>
                  <td>{new Date(item.created_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}

export default MigrationsPage
