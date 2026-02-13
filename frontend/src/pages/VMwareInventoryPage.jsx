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
  const [specByKey, setSpecByKey] = useState({})
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
      if (next.has(key)) {
        next.delete(key)
        setSpecByKey((prev) => {
          const copy = { ...prev }
          delete copy[key]
          return copy
        })
      } else {
        next.add(key)
        setSpecByKey((prev) => {
          if (prev[key]) return prev
          return {
            ...prev,
            [key]: buildDefaultSpec(vm),
          }
        })
      }
      return next
    })
  }

  function updateSpec(vm, field, value) {
    const key = makeKey(vm)
    setSpecByKey((current) => ({
      ...current,
      [key]: {
        ...buildDefaultSpec(vm),
        ...(current[key] || {}),
        [field]: value,
      },
    }))
  }

  async function migrateSelected() {
    if (!selectedVMs.length) return
    setSubmitting(true)
    setError('')
    setResult(null)

    try {
      const payload = selectedVMs.map((vm) => {
        const key = makeKey(vm)
        const spec = specByKey[key] || buildDefaultSpec(vm)
        const overrides = buildOverrides(spec)
        const base = { name: vm.name, source: vm.source }
        if (Object.keys(overrides).length) base.overrides = overrides
        return base
      })
      const response = await triggerMigrations(payload)
      setResult(response)
      setSelectedKeys(new Set())
      setSpecByKey({})
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

            {!!selectedVMs.length && (
              <div className="spec-form-grid">
                {selectedVMs.map((vm) => {
                  const key = makeKey(vm)
                  const spec = specByKey[key] || buildDefaultSpec(vm)
                  return (
                    <article className="spec-card" key={`spec-${key}`}>
                      <h4>{vm.name}</h4>
                      <p>Adjust OpenStack target specs before starting migration.</p>
                      <div className="spec-fields">
                        <label>
                          <span>CPU</span>
                          <input
                            type="number"
                            min="1"
                            value={spec.cpu}
                            onChange={(e) => updateSpec(vm, 'cpu', e.target.value)}
                          />
                        </label>
                        <label>
                          <span>RAM (MB)</span>
                          <input
                            type="number"
                            min="1"
                            value={spec.ram}
                            onChange={(e) => updateSpec(vm, 'ram', e.target.value)}
                          />
                        </label>
                        <label>
                          <span>Network name</span>
                          <input
                            type="text"
                            value={spec.network_name}
                            onChange={(e) => updateSpec(vm, 'network_name', e.target.value)}
                            placeholder="private"
                          />
                        </label>
                        <label>
                          <span>Fixed IP (optional)</span>
                          <input
                            type="text"
                            value={spec.fixed_ip}
                            onChange={(e) => updateSpec(vm, 'fixed_ip', e.target.value)}
                            placeholder="192.168.1.20"
                          />
                        </label>
                        <label className="span-2">
                          <span>Extra disks (GB, comma-separated)</span>
                          <input
                            type="text"
                            value={spec.extra_disks_gb}
                            onChange={(e) => updateSpec(vm, 'extra_disks_gb', e.target.value)}
                            placeholder="20, 50"
                          />
                        </label>
                      </div>
                    </article>
                  )
                })}
              </div>
            )}

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

function buildDefaultSpec(vm) {
  const metadata = vm?.metadata || {}
  return {
    cpu: vm?.cpu ?? '',
    ram: vm?.ram ?? '',
    network_name: inferNetworkName(metadata),
    fixed_ip: inferFixedIp(metadata),
    extra_disks_gb: '',
  }
}

function buildOverrides(spec) {
  const overrides = {}

  const cpu = parsePositiveInteger(spec?.cpu)
  if (cpu) overrides.cpu = cpu

  const ram = parsePositiveInteger(spec?.ram)
  if (ram) overrides.ram = ram

  const extraDisks = parseDiskList(spec?.extra_disks_gb)
  if (extraDisks.length) overrides.extra_disks_gb = extraDisks

  const network = {}
  if (typeof spec?.network_name === 'string' && spec.network_name.trim()) {
    network.network_name = spec.network_name.trim()
  }
  if (typeof spec?.fixed_ip === 'string' && spec.fixed_ip.trim()) {
    network.fixed_ip = spec.fixed_ip.trim()
  }
  if (Object.keys(network).length) overrides.network = network

  return overrides
}

function parsePositiveInteger(value) {
  const parsed = Number.parseInt(String(value), 10)
  if (!Number.isFinite(parsed) || parsed <= 0) return null
  return parsed
}

function parseDiskList(value) {
  if (typeof value !== 'string') return []
  return value
    .split(',')
    .map((part) => Number.parseInt(part.trim(), 10))
    .filter((n) => Number.isFinite(n) && n > 0)
}

function inferNetworkName(metadata) {
  if (!metadata || typeof metadata !== 'object') return ''

  const candidates = [
    metadata.network_name,
    metadata.portgroup,
    metadata.primary_network,
    metadata.network?.name,
    metadata.network,
  ]
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim()
  }
  return ''
}

function inferFixedIp(metadata) {
  if (!metadata || typeof metadata !== 'object') return ''

  const candidates = [
    metadata.ip_address,
    metadata.ip,
    metadata.ipv4,
    metadata.guest_ip,
    metadata.primary_ip,
  ]
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim()
  }
  return ''
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
