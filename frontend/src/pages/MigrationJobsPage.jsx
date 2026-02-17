import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchMigrationJobs } from '../api/migrations'
import {
  fetchOpenStackHealth,
  fetchOpenStackProvisionStatus,
  triggerOpenStackProvision,
} from '../api/openstack'
import PanelState from '../components/PanelState'
import StatusBadge from '../components/StatusBadge'

const POLL_INTERVAL_MS = 5000

function MigrationJobsPage() {
  const [jobs, setJobs] = useState([])
  const [health, setHealth] = useState(null)
  const [provisioning, setProvisioning] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [provisioningBusy, setProvisioningBusy] = useState(false)

  useEffect(() => {
    let mounted = true

    async function load() {
      try {
        const [jobItems, healthData, provisionData] = await Promise.all([
          fetchMigrationJobs(),
          fetchOpenStackHealth(),
          fetchOpenStackProvisionStatus(),
        ])
        if (!mounted) return
        setJobs(jobItems)
        setHealth(healthData)
        setProvisioning(provisionData)
        setError('')
      } catch (err) {
        if (!mounted) return
        setError(err.message || 'Failed to load migration dashboard data.')
      } finally {
        if (mounted) setLoading(false)
      }
    }

    load()
    const timer = setInterval(load, POLL_INTERVAL_MS)
    return () => {
      mounted = false
      clearInterval(timer)
    }
  }, [])

  async function handleProvision() {
    setProvisioningBusy(true)
    setError('')
    try {
      await triggerOpenStackProvision()
      const provisionData = await fetchOpenStackProvisionStatus()
      setProvisioning(provisionData)
    } catch (err) {
      setError(err.message || 'Failed to trigger OpenStack provisioning.')
    } finally {
      setProvisioningBusy(false)
    }
  }

  const provisioningActive = ['QUEUED', 'RUNNING'].includes(provisioning?.state)
  const jobStats = useMemo(() => {
    const total = jobs.length
    const active = jobs.filter((job) =>
      ['PENDING', 'DISCOVERED', 'CONVERTING', 'UPLOADING', 'DEPLOYED'].includes(job.status),
    ).length
    const failed = jobs.filter((job) => ['FAILED', 'ROLLED_BACK'].includes(job.status)).length
    const verified = jobs.filter((job) => job.status === 'VERIFIED').length
    return { total, active, failed, verified }
  }, [jobs])

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>Migration Jobs Dashboard</h2>
          <p>Live status of migration workflow states with auto-refresh.</p>
        </div>
        <div className="header-actions">
          <button
            className="primary-btn"
            onClick={handleProvision}
            disabled={provisioningBusy || provisioningActive}
          >
            Provision OpenStack Infra
          </button>
        </div>
      </div>

      {error && <div className="alert error">{error}</div>}

      <div className="stats-grid">
        <div className="stat-card">
          <p>OpenStack project</p>
          <strong className="mono">{health?.project_id || '-'}</strong>
        </div>
        <div className="stat-card">
          <p>Images</p>
          <strong>{health?.image_count ?? '-'}</strong>
        </div>
        <div className="stat-card">
          <p>Flavors</p>
          <strong>{health?.flavor_count ?? '-'}</strong>
        </div>
        <div className="stat-card">
          <p>Networks</p>
          <strong>{health?.network_count ?? '-'}</strong>
        </div>
        <div className="stat-card">
          <p>Infra provisioning</p>
          <div className="stat-inline">
            <StatusBadge status={provisioning?.state || 'IDLE'} />
          </div>
          <div className="stat-subtext">
            {provisioning?.message || 'No provisioning runs yet.'}
          </div>
        </div>
        <div className="stat-card">
          <p>Jobs total</p>
          <strong>{jobStats.total}</strong>
          <div className="stat-subtext">Active: {jobStats.active} • Verified: {jobStats.verified} • Failed: {jobStats.failed}</div>
        </div>
      </div>

      <div className="panel">
        {loading ? (
          <PanelState title="Loading jobs" message="Fetching migration job queue..." />
        ) : jobs.length === 0 ? (
          <PanelState title="No jobs yet" message="Trigger migrations from VMware inventory to populate this table." />
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Created at</th>
                  <th>Updated at</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id} className="vm-row">
                    <td>
                      <div className="vm-name-cell">
                        <strong>{job.vm_name}</strong>
                        <span>Job #{job.id}</span>
                      </div>
                    </td>
                    <td>
                      <StatusBadge status={job.status} />
                    </td>
                    <td>{formatDate(job.created_at)}</td>
                    <td>{formatDate(job.updated_at)}</td>
                    <td>
                      <Link className="text-link" to={`/migrations/${job.id}`}>
                        Open
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  )
}

function formatDate(value) {
  if (!value) return '-'
  return new Date(value).toLocaleString()
}

export default MigrationJobsPage
