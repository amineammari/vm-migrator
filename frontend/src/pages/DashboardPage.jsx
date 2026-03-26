import { useEffect, useState } from 'react'
import { fetchDashboard } from '../api/dashboard'
import { fetchUsers } from '../api/users'
import StatusBadge from '../components/StatusBadge'
import { useAuth } from '../contexts/AuthContext'

function DashboardPage() {
  const { user } = useAuth()
  const isSuperAdmin = user?.role === 'SUPER_ADMIN'
  const [dashboard, setDashboard] = useState(null)
  const [users, setUsers] = useState([])
  const [selectedUserId, setSelectedUserId] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (isSuperAdmin) {
      fetchUsers().then(setUsers).catch(() => {})
    }
  }, [isSuperAdmin])

  useEffect(() => {
    loadDashboard()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedUserId])

  async function loadDashboard() {
    try {
      const data = await fetchDashboard({ userId: selectedUserId || undefined })
      setDashboard(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load dashboard.')
    }
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>Dashboard</h2>
          <p>{isSuperAdmin ? 'Global migration visibility with optional user filtering.' : 'Your migration activity overview.'}</p>
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

      <div className="stats-grid">
        <StatCard label="Total migrations" value={dashboard?.total_migrations ?? '-'} />
        <StatCard label="Completed" value={dashboard?.stats_by_status?.completed ?? '-'} />
        <StatCard label="Running" value={dashboard?.stats_by_status?.running ?? '-'} />
        <StatCard label="Failed" value={dashboard?.stats_by_status?.failed ?? '-'} />
      </div>

      <div className="panel">
        <div className="toolbar">
          <h3>Recent migrations</h3>
          <button className="secondary-btn" onClick={loadDashboard}>
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
              </tr>
            </thead>
            <tbody>
              {(dashboard?.migrations || []).map((item) => (
                <tr key={item.id}>
                  <td>{item.vm_name}</td>
                  <td>{item.user?.username || '-'}</td>
                  <td>{item.source || '-'}</td>
                  <td>{item.destination || '-'}</td>
                  <td><StatusBadge status={item.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <p>{label}</p>
      <strong>{value}</strong>
    </div>
  )
}

export default DashboardPage
