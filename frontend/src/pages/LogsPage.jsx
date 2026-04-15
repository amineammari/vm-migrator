import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchMigrationJobs } from '../api/migrations'
import PanelState from '../components/PanelState'
import StatusBadge from '../components/StatusBadge'
import { Alert, Button, Card, Field, PageHeader } from '../components/ui'

const LOG_LEVELS = ['all', 'info', 'success', 'warning', 'error']

function LogsPage() {
  const [jobs, setJobs] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [level, setLevel] = useState('all')
  const [query, setQuery] = useState('')

  useEffect(() => {
    loadJobs()
  }, [])

  async function loadJobs() {
    setLoading(true)
    try {
      const data = await fetchMigrationJobs({ ordering: '-updated_at' })
      setJobs(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load monitoring logs.')
    } finally {
      setLoading(false)
    }
  }

  const entries = useMemo(() => {
    return jobs.flatMap((job) => buildLogEntries(job)).filter((entry) => {
      const matchesLevel = level === 'all' || entry.level === level
      const text = `${entry.vm} ${entry.message} ${entry.status}`.toLowerCase()
      return matchesLevel && text.includes(query.trim().toLowerCase())
    })
  }, [jobs, level, query])

  return (
    <section>
      <PageHeader
        eyebrow="Observability"
        title="Logs and Monitoring"
        description="Track workflow events, errors, and recent job activity."
        actions={
          <Button variant="secondary" onClick={loadJobs} disabled={loading}>
            Refresh
          </Button>
        }
      />

      {error ? <Alert>{error}</Alert> : null}

      <Card className="toolbar-panel">
        <div className="filter-bar" role="search">
          <Field label="Search logs">
            <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="vm-prod-02, failed, uploaded" />
          </Field>
          <Field label="Level">
            <select value={level} onChange={(event) => setLevel(event.target.value)}>
              {LOG_LEVELS.map((item) => (
                <option key={item} value={item}>{item.toUpperCase()}</option>
              ))}
            </select>
          </Field>
        </div>
      </Card>

      <Card className="log-panel">
        {loading ? (
          <PanelState title="Loading logs" message="Reading migration job activity..." />
        ) : entries.length === 0 ? (
          <PanelState title="No logs match" message="Adjust the search or level filter." />
        ) : (
          <ol className="log-list" aria-label="Migration logs">
            {entries.map((entry) => (
              <li key={entry.id} className={`log-entry ${entry.level}`}>
                <time>{entry.time}</time>
                <span className="log-level">{entry.level}</span>
                <div>
                  <strong>{entry.vm}</strong>
                  <p>{entry.message}</p>
                </div>
                <StatusBadge status={entry.status} />
                <Link className="text-link" to={`/migrations/${entry.jobId}`}>Open</Link>
              </li>
            ))}
          </ol>
        )}
      </Card>
    </section>
  )
}

function buildLogEntries(job) {
  const status = job.status || 'PENDING'
  const level = status.includes('FAILED') || status.includes('ROLL') ? 'error' : status.includes('VERIFIED') ? 'success' : 'info'
  const updated = job.updated_at || job.created_at
  return [
    {
      id: `${job.id}-status`,
      jobId: job.id,
      vm: job.vm_name || `Job ${job.id}`,
      status,
      level,
      time: formatDate(updated),
      message: `Workflow state changed to ${status}.`,
    },
  ]
}

function formatDate(value) {
  if (!value) return '-'
  return new Date(value).toLocaleString()
}

export default LogsPage
