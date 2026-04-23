import { useEffect, useState } from 'react'
import { fetchDashboard } from '../api/dashboard'
import { fetchUsers } from '../api/users'
import StatusBadge from '../components/StatusBadge'
import { Alert, Button, Card, PageHeader, Skeleton, Table } from '../components/ui'
import { useAuth } from '../contexts/useAuth'

function DashboardPage() {
  const { user } = useAuth()
  const isSuperAdmin = user?.role === 'SUPER_ADMIN'
  const [dashboard, setDashboard] = useState(null)
  const [users, setUsers] = useState([])
  const [selectedUserId, setSelectedUserId] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)
  const [tablePage, setTablePage] = useState(1)
  const [tablePageSize, setTablePageSize] = useState(10)

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
    setLoading(true)
    try {
      const data = await fetchDashboard({ userId: selectedUserId || undefined })
      setDashboard(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load dashboard.')
    } finally {
      setLoading(false)
    }
  }

  const stats = dashboard?.stats_by_status || {}
  const migrations = dashboard?.migrations || []
  const completed = stats.completed ?? stats.verified ?? 0
  const running = stats.running ?? 0
  const failed = stats.failed ?? 0
  const total = dashboard?.total_migrations ?? 0
  const healthPercent = total ? Math.round((completed / total) * 100) : 0
  const tableTotalPages = Math.max(1, Math.ceil(migrations.length / Math.max(1, tablePageSize)))
  const pagedMigrations = migrations.slice((tablePage - 1) * tablePageSize, tablePage * tablePageSize)

  useEffect(() => {
    setTablePage(1)
  }, [selectedUserId, migrations.length])

  useEffect(() => {
    if (tablePage > tableTotalPages) {
      setTablePage(tableTotalPages)
    }
  }, [tablePage, tableTotalPages])

  return (
    <section>
      <PageHeader
        eyebrow="Dashboard"
        title="Dashboard"
        description={isSuperAdmin ? 'Global migration visibility with optional user filtering.' : 'Your migration activity overview.'}
        actions={
          isSuperAdmin ? (
            <select value={selectedUserId} onChange={(event) => setSelectedUserId(event.target.value)}>
              <option value="">All users</option>
              {users.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.username}
                </option>
              ))}
            </select>
          ) : null
        }
      />

      {error ? <Alert>{error}</Alert> : null}

      <div className="stats-grid">
        <StatCard label="Total VMs" value={total || '-'} trend="Migration records in scope" />
        <StatCard label="Completed" value={completed || '-'} trend={`${healthPercent}% completion`} />
        <StatCard label="In progress" value={running || '-'} trend="Actively orchestrating" />
        <StatCard label="Errors" value={failed || '-'} trend={failed ? 'Review monitoring logs' : 'No active failures'} tone={failed ? 'danger' : 'success'} />
      </div>

      <div className="dashboard-grid">
        <Card className="chart-card">
          <div className="toolbar">
            <div>
              <h3>Migration throughput</h3>
              <p>Last 7 workflow windows</p>
            </div>
          </div>
          <div className="chart-placeholder" role="img" aria-label="Migration throughput chart placeholder">
            {(dashboard?.throughput || [42, 58, 36, 72, 66, 84, 61]).map((height, index) => (
              <span key={index} style={{ '--bar-height': `${height}%` }} />
            ))}
          </div>
        </Card>

        <Card className="chart-card">
          <div className="toolbar">
            <div>
              <h3>Workflow health</h3>
              <p>Completion ratio in current scope</p>
            </div>
          </div>
          <div
            className="donut-meter"
            style={
              healthPercent > 0
                ? { '--meter': `${healthPercent * 3.6}deg` }
                : { '--meter': '0deg', background: 'conic-gradient(var(--color-neutral-300) 360deg, var(--color-neutral-100) 0)' }
            }
          >
            <div style={{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 2,
              pointerEvents: 'none',
            }}>
              <strong>{healthPercent}%</strong>
              <span>verified</span>
            </div>
          </div>
        </Card>
      </div>

      <Card>
        <div className="toolbar">
          <h3>Recent migrations</h3>
          <Button variant="secondary" onClick={loadDashboard}>
            Refresh
          </Button>
        </div>
        {loading ? (
          <Skeleton rows={5} />
        ) : (
          <Table
            pagination={{
              page: tablePage,
              pageSize: tablePageSize,
              totalItems: migrations.length,
              onPageChange: setTablePage,
              onPageSizeChange: (nextPageSize) => {
                setTablePageSize(nextPageSize)
                setTablePage(1)
              },
              label: 'migrations',
            }}
          >
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
              {pagedMigrations.map((item) => (
                <tr key={item.id}>
                  <td>{item.vm_name}</td>
                  <td>{item.user?.username || '-'}</td>
                  <td>{item.source || '-'}</td>
                  <td>{item.destination || '-'}</td>
                  <td><StatusBadge status={item.status} /></td>
                </tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>
    </section>
  )
}

function StatCard({ label, value, trend, tone = '' }) {
  return (
    <div className={`stat-card ${tone}`.trim()}>
      <p>{label}</p>
      <strong>{value}</strong>
      {trend ? <div className="stat-subtext">{trend}</div> : null}
    </div>
  )
}

export default DashboardPage
