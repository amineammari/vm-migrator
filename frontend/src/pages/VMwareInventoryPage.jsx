import { useEffect, useMemo, useState } from 'react'
import {
  discoverVMwareNow,
  fetchTaskStatus,
  fetchVMwareVMs,
  triggerMigrations,
} from '../api/vmware'
import PanelState from '../components/PanelState'

function VMwareInventoryPage() {
  const [vms, setVMs] = useState([])
  const [selectedKeys, setSelectedKeys] = useState(new Set())
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(null)

  useEffect(() => {
    loadVMs()
  }, [])

  const selectedVMs = useMemo(
    () => vms.filter((vm) => selectedKeys.has(makeKey(vm))),
    [vms, selectedKeys],
  )

  async function loadVMs() {
    setLoading(true)
    setError('')
    try {
      const items = await fetchVMwareVMs()
      setVMs(items)
    } catch (err) {
      setError(err.message || 'Unable to load VMware inventory.')
    } finally {
      setLoading(false)
    }
  }

  async function refreshFromESXi() {
    setRefreshing(true)
    setError('')
    try {
      const discovery = await discoverVMwareNow({
        include_workstation: false,
        include_esxi: true,
      })
      const taskId = discovery?.task_id
      if (!taskId) throw new Error('Discovery did not return a task id.')

      const final = await waitForTaskCompletion(taskId)
      if (final?.state !== 'SUCCESS') {
        const reason =
          typeof final?.result === 'string'
            ? final.result
            : final?.result?.error || `Discovery task failed with state ${final?.state}.`
        throw new Error(reason)
      }

      const esxiErrors = final?.result?.esxi?.errors
      if (Array.isArray(esxiErrors) && esxiErrors.length > 0) {
        throw new Error(esxiErrors[0])
      }

      await loadVMs()
    } catch (err) {
      setError(err.message || 'Unable to refresh ESXi inventory.')
    } finally {
      setRefreshing(false)
    }
  }

  function toggleVM(vm) {
    const key = makeKey(vm)
    setSelectedKeys((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  async function migrateSelected() {
    if (!selectedVMs.length) return
    setSubmitting(true)
    setError('')
    setResult(null)

    try {
      const payload = selectedVMs.map((vm) => ({ name: vm.name, source: vm.source }))
      const response = await triggerMigrations(payload)
      setResult(response)
      setSelectedKeys(new Set())
    } catch (err) {
      setError(err.message || 'Migration request failed.')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>VMware Inventory</h2>
          <p>Select discovered VMs and start migration jobs.</p>
        </div>
        <button className="secondary-btn" onClick={refreshFromESXi} disabled={loading || refreshing || submitting}>
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      {error && <div className="alert error">{error}</div>}
      {result && (
        <div className="alert success">
          Created: {result.created_jobs?.length || 0}, Skipped: {result.skipped_jobs?.length || 0}
        </div>
      )}

      <div className="panel">
        {loading ? (
          <PanelState title="Loading inventory" message="Fetching discovered VMware VMs..." />
        ) : vms.length === 0 ? (
          <PanelState title="No discovered VMs" message="Run discovery and refresh this page." />
        ) : (
          <>
            <div className="toolbar">
              <p>{selectedVMs.length} selected</p>
              <button
                className="primary-btn"
                onClick={migrateSelected}
                disabled={!selectedVMs.length || submitting}
              >
                {submitting ? 'Submitting...' : 'Migrate selected VMs'}
              </button>
            </div>

            <table className="data-table">
              <thead>
                <tr>
                  <th></th>
                  <th>Name</th>
                  <th>Source</th>
                  <th>CPU</th>
                  <th>RAM (MB)</th>
                  <th>Power state</th>
                </tr>
              </thead>
              <tbody>
                {vms.map((vm) => {
                  const key = makeKey(vm)
                  const checked = selectedKeys.has(key)
                  return (
                    <tr key={key}>
                      <td>
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => toggleVM(vm)}
                          aria-label={`Select ${vm.name}`}
                        />
                      </td>
                      <td>{vm.name}</td>
                      <td>{vm.source}</td>
                      <td>{vm.cpu ?? '-'}</td>
                      <td>{vm.ram ?? '-'}</td>
                      <td>{vm.power_state || '-'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </>
        )}
      </div>
    </section>
  )
}

function makeKey(vm) {
  return `${vm.source}::${vm.name}`
}

async function waitForTaskCompletion(taskId, timeoutMs = 60000, intervalMs = 1200) {
  const startedAt = Date.now()
  // Poll Celery task status until it reaches a terminal state.
  while (Date.now() - startedAt < timeoutMs) {
    const status = await fetchTaskStatus(taskId)
    if (status?.ready) return status
    await sleep(intervalMs)
  }
  throw new Error('Discovery timed out. Please try refresh again.')
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export default VMwareInventoryPage
