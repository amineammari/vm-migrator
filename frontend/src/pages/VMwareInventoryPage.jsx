import { useEffect, useMemo, useState } from 'react'
import { fetchVMwareVMs, triggerMigrations } from '../api/vmware'
import PanelState from '../components/PanelState'

function VMwareInventoryPage() {
  const [vms, setVMs] = useState([])
  const [selectedKeys, setSelectedKeys] = useState(new Set())
  const [loading, setLoading] = useState(true)
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
        <button className="secondary-btn" onClick={loadVMs} disabled={loading || submitting}>
          Refresh
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

export default VMwareInventoryPage
