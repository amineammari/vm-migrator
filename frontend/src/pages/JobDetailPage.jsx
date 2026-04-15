import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { fetchMigrationJob } from '../api/migrations'
import PanelState from '../components/PanelState'
import StatusBadge from '../components/StatusBadge'
import { Alert, Button, Card, PageHeader } from '../components/ui'

function JobDetailPage() {
  const { id } = useParams()
  const [job, setJob] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    loadJob()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id])

  async function loadJob() {
    setLoading(true)
    setError('')
    try {
      const data = await fetchMigrationJob(id)
      setJob(data)
    } catch (err) {
      setError(err.message || 'Unable to load job details.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <section>
      <PageHeader
        eyebrow="Job detail"
        title="Migration Job Detail"
        description={`Job #${id}`}
        actions={
          <>
          <Button variant="secondary" onClick={loadJob} disabled={loading}>
            Refresh
          </Button>
          <Button as={Link} variant="secondary" to="/migration-jobs">
            Back
          </Button>
          </>
        }
      />

      {error && <Alert>{error}</Alert>}

      <Card>
        {loading ? (
          <PanelState title="Loading job" message="Fetching migration metadata..." />
        ) : !job ? (
          <PanelState title="Not found" message="This migration job is unavailable." />
        ) : (
          <>
            <section className="detail-block">
              <h3>Execution Summary</h3>
              <div className="detail-grid">
                <Detail label="ID" value={job.id} />
                <Detail label="VM name" value={job.vm_name} />
                <Detail label="Status" value={<StatusBadge status={job.status} />} />
                <Detail label="Created" value={formatDate(job.created_at)} />
                <Detail label="Updated" value={formatDate(job.updated_at)} />
                <Detail
                  label="Source"
                  value={job.conversion_metadata?.selected_source || '-'}
                />
              </div>
            </section>

            <section className="detail-block">
              <h3>Conversion Plan</h3>
              <pre className="json-block">{prettyJson(job.conversion_metadata?.conversion)}</pre>
            </section>

            <section className="detail-block">
              <h3>Disk Analysis</h3>
              <pre className="json-block">{prettyJson(job.disk_analysis)}</pre>
            </section>

            <section className="detail-block">
              <h3>OpenStack Deployment</h3>
              <pre className="json-block">{prettyJson(job.conversion_metadata?.openstack)}</pre>
            </section>

            <section className="detail-block">
              <h3>Rollback</h3>
              <pre className="json-block">
                {prettyJson({
                  rollback_at: job.conversion_metadata?.rollback_at,
                  rollback_reason: job.conversion_metadata?.rollback_reason,
                  rollback_actions: job.conversion_metadata?.rollback_actions,
                })}
              </pre>
            </section>
          </>
        )}
      </Card>
    </section>
  )
}

function Detail({ label, value }) {
  return (
    <div className="detail-item">
      <span>{label}</span>
      <strong className="detail-value">{value || '-'}</strong>
    </div>
  )
}

function prettyJson(data) {
  if (!data) return 'No data available.'
  return JSON.stringify(data, null, 2)
}

function formatDate(value) {
  if (!value) return '-'
  return new Date(value).toLocaleString()
}

export default JobDetailPage
