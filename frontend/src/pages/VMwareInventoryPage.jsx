import { Fragment, useMemo, useState } from 'react'
import {
  connectVMwareEndpoint,
  discoverVMwareNow,
  fetchTaskStatus,
  fetchVMwareVMs,
  testVMwareEndpoint,
  triggerMigrations,
} from '../api/vmware'
import { connectOpenstackEndpoint, testOpenstackEndpoint } from '../api/openstack'
import PanelState from '../components/PanelState'

function VMwareInventoryPage() {
  const [vms, setVMs] = useState([])
  const [selectedKeys, setSelectedKeys] = useState(new Set())
  const [specByKey, setSpecByKey] = useState({})
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [openstackError, setOpenstackError] = useState('')
  const [result, setResult] = useState(null)
  const [flavors, setFlavors] = useState([])
  const [networks, setNetworks] = useState([])

  const [activeVmwareEndpoint, setActiveVmwareEndpoint] = useState(null)
  const [activeOpenstackEndpoint, setActiveOpenstackEndpoint] = useState(null)

  const [showVmwareModal, setShowVmwareModal] = useState(false)
  const [showOpenstackModal, setShowOpenstackModal] = useState(false)
  const [expandedVmKey, setExpandedVmKey] = useState('')

  const [vmwareForm, setVmwareForm] = useState({
    label: '',
    host: '',
    port: 443,
    username: '',
    password: '',
    insecure: true,
  })
  const [vmwareTesting, setVmwareTesting] = useState(false)
  const [vmwareConnecting, setVmwareConnecting] = useState(false)
  const [vmwareTestPassed, setVmwareTestPassed] = useState(false)
  const [vmwareTestMessage, setVmwareTestMessage] = useState('')

  const [openstackForm, setOpenstackForm] = useState({
    label: '',
    auth_url: '',
    username: '',
    password: '',
    project_name: '',
    user_domain_name: 'Default',
    project_domain_name: 'Default',
    region_name: '',
    interface: '',
    identity_api_version: '',
    verify: false,
    image_endpoint_override: '',
  })
  const [openstackTesting, setOpenstackTesting] = useState(false)
  const [openstackConnecting, setOpenstackConnecting] = useState(false)
  const [openstackTestPassed, setOpenstackTestPassed] = useState(false)
  const [openstackTestMessage, setOpenstackTestMessage] = useState('')

  const selectedVMs = useMemo(
    () => vms.filter((vm) => selectedKeys.has(makeKey(vm))),
    [vms, selectedKeys],
  )

  async function loadVMs(endpointSessionId) {
    if (typeof endpointSessionId !== 'number') return
    setLoading(true)
    setError('')
    try {
      const items = await fetchVMwareVMs({ endpointSessionId })
      setVMs(items)
    } catch (err) {
      setError(err.message || 'Unable to load VMware inventory.')
    } finally {
      setLoading(false)
    }
  }

  async function refreshFromESXi() {
    if (!activeVmwareEndpoint?.id) {
      setError('Connectez-vous d\'abord a un endpoint VMware ESXi.')
      return
    }
    setRefreshing(true)
    setError('')
    try {
      const discovery = await discoverVMwareNow({
        include_workstation: false,
        include_esxi: true,
        vmware_endpoint_session_id: activeVmwareEndpoint.id,
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

      await loadVMs(activeVmwareEndpoint.id)
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

  function updateSpecValues(vm, nextValues) {
    const key = makeKey(vm)
    setSpecByKey((current) => ({
      ...current,
      [key]: {
        ...buildDefaultSpec(vm),
        ...(current[key] || {}),
        ...nextValues,
      },
    }))
  }

  async function migrateSelected() {
    if (!selectedVMs.length) return
    if (!activeVmwareEndpoint?.id) {
      setError('Veuillez connecter un endpoint VMware ESXi.')
      return
    }
    if (!activeOpenstackEndpoint?.id) {
      setOpenstackError('Veuillez connecter un endpoint OpenStack.')
      return
    }

    setSubmitting(true)
    setError('')
    setOpenstackError('')
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
      const response = await triggerMigrations({
        vms: payload,
        vmware_endpoint_session_id: activeVmwareEndpoint.id,
        openstack_endpoint_session_id: activeOpenstackEndpoint.id,
      })
      setResult(response)
      setSelectedKeys(new Set())
      setSpecByKey({})
    } catch (err) {
      setError(err.message || 'Migration request failed.')
    } finally {
      setSubmitting(false)
    }
  }

  async function handleVmwareTest() {
    setVmwareTesting(true)
    setVmwareTestPassed(false)
    setVmwareTestMessage('')
    try {
      const res = await testVMwareEndpoint(vmwareForm)
      setVmwareTestPassed(Boolean(res?.ok))
      setVmwareTestMessage(res?.message || 'Test reussi.')
    } catch (err) {
      setVmwareTestPassed(false)
      setVmwareTestMessage(err.message || 'Echec du test VMware ESXi.')
    } finally {
      setVmwareTesting(false)
    }
  }

  async function handleVmwareConnect() {
    setVmwareConnecting(true)
    setError('')
    try {
      const res = await connectVMwareEndpoint(vmwareForm)
      setActiveVmwareEndpoint(res?.vmware_endpoint_session || null)
      setVMs(Array.isArray(res?.items) ? res.items : [])
      setSelectedKeys(new Set())
      setSpecByKey({})
      setExpandedVmKey('')
      setShowVmwareModal(false)
      setVmwareTestPassed(false)
      setVmwareTestMessage('')
    } catch (err) {
      setError(err.message || 'Connexion VMware impossible.')
    } finally {
      setVmwareConnecting(false)
    }
  }

  async function handleOpenstackTest() {
    setOpenstackTesting(true)
    setOpenstackTestPassed(false)
    setOpenstackTestMessage('')
    try {
      const res = await testOpenstackEndpoint(openstackForm)
      setOpenstackTestPassed(Boolean(res?.ok))
      setOpenstackTestMessage(res?.message || 'Test reussi.')
    } catch (err) {
      setOpenstackTestPassed(false)
      setOpenstackTestMessage(err.message || 'Echec du test OpenStack.')
    } finally {
      setOpenstackTesting(false)
    }
  }

  async function handleOpenstackConnect() {
    setOpenstackConnecting(true)
    setOpenstackError('')
    try {
      const res = await connectOpenstackEndpoint(openstackForm)
      setActiveOpenstackEndpoint(res?.openstack_endpoint_session || null)
      setFlavors(Array.isArray(res?.flavors) ? res.flavors : [])
      setNetworks(Array.isArray(res?.networks) ? res.networks : [])
      setShowOpenstackModal(false)
      setOpenstackTestPassed(false)
      setOpenstackTestMessage('')
    } catch (err) {
      setOpenstackError(err.message || 'Connexion OpenStack impossible.')
    } finally {
      setOpenstackConnecting(false)
    }
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>VMware Inventory</h2>
          <p>Select discovered VMs and start migration jobs.</p>
          <div className="endpoint-summary">
            <span>
              VMware: {activeVmwareEndpoint ? `${activeVmwareEndpoint.host}:${activeVmwareEndpoint.port}` : 'Non connecte'}
            </span>
            <span>
              OpenStack: {activeOpenstackEndpoint ? `${activeOpenstackEndpoint.project_name} @ ${activeOpenstackEndpoint.auth_url}` : 'Non connecte'}
            </span>
          </div>
        </div>
        <div className="header-actions">
          <button className="secondary-btn" onClick={() => setShowVmwareModal(true)} disabled={submitting}>
            Connect ESXi
          </button>
          <button className="secondary-btn" onClick={() => setShowOpenstackModal(true)} disabled={submitting}>
            Connect OpenStack
          </button>
          <button className="secondary-btn" onClick={refreshFromESXi} disabled={loading || refreshing || submitting || !activeVmwareEndpoint}>
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </div>

      {error && <div className="alert error">{error}</div>}
      {openstackError && <div className="alert error">{openstackError}</div>}
      {result && (
        <div className="alert success">
          Created: {result.created_jobs?.length || 0}, Skipped: {result.skipped_jobs?.length || 0}
        </div>
      )}

      <div className="panel">
        {!activeVmwareEndpoint ? (
          <PanelState title="No ESXi connection" message="Open Connect ESXi and test credentials before loading inventory." />
        ) : loading ? (
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
                disabled={!selectedVMs.length || submitting || !activeOpenstackEndpoint}
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
                        <label className="span-2">
                          <span>Flavor</span>
                          <select
                            value={spec.flavor_id}
                            onChange={(e) => {
                              const nextFlavorId = e.target.value
                              const nextFlavor = flavors.find((item) => item.id === nextFlavorId)
                              updateSpecValues(vm, {
                                flavor_id: nextFlavorId,
                                cpu: nextFlavor ? String(nextFlavor.vcpus ?? '') : spec.cpu,
                                ram: nextFlavor ? String(nextFlavor.ram ?? '') : spec.ram,
                              })
                            }}
                            disabled={!activeOpenstackEndpoint}
                          >
                            <option value="">Auto (map from CPU/RAM)</option>
                            {flavors.map((flavor) => (
                              <option key={flavor.id} value={flavor.id}>
                                {flavor.name} - {flavor.vcpus ?? '?'} vCPU, {flavor.ram ?? '?'} MB, {flavor.disk ?? 0} GB
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>CPU</span>
                          <input
                            type="number"
                            min="1"
                            value={spec.cpu}
                            onChange={(e) => updateSpec(vm, 'cpu', e.target.value)}
                            disabled={Boolean(spec.flavor_id)}
                          />
                        </label>
                        <label>
                          <span>RAM (MB)</span>
                          <input
                            type="number"
                            min="1"
                            value={spec.ram}
                            onChange={(e) => updateSpec(vm, 'ram', e.target.value)}
                            disabled={Boolean(spec.flavor_id)}
                          />
                        </label>
                        <label>
                          <span>Network</span>
                          {networks.length ? (
                            <select
                              value={spec.network_id}
                              onChange={(e) => {
                                const nextNetworkId = e.target.value
                                const nextNetwork = networks.find((item) => item.id === nextNetworkId)
                                updateSpecValues(vm, {
                                  network_id: nextNetworkId,
                                  network_name: nextNetwork?.name || '',
                                  fixed_ip: '',
                                })
                              }}
                              disabled={!activeOpenstackEndpoint}
                            >
                              <option value="">Select a network</option>
                              {networks.map((network) => (
                                <option key={network.id} value={network.id}>
                                  {formatNetworkLabel(network)}
                                </option>
                              ))}
                            </select>
                          ) : (
                            <input
                              type="text"
                              value={spec.network_name}
                              onChange={(e) => updateSpec(vm, 'network_name', e.target.value)}
                              placeholder="private"
                            />
                          )}
                        </label>
                        <label>
                          <span>Fixed IP (optional)</span>
                          {renderFixedIpField({
                            spec,
                            network: networks.find((item) => item.id === spec.network_id),
                            onChange: (value) => updateSpec(vm, 'fixed_ip', value),
                          })}
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

            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th></th>
                    <th>Name</th>
                    <th>Source</th>
                    <th>Guest OS</th>
                    <th>IP</th>
                    <th>CPU</th>
                    <th>RAM (MB)</th>
                    <th>Storage</th>
                    <th>Host</th>
                    <th>Power state</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {vms.map((vm) => {
                  const key = makeKey(vm)
                  const checked = selectedKeys.has(key)
                  const expanded = expandedVmKey === key
                  const metadata = vm?.metadata || {}
                  const guest = metadata?.guest || {}
                  const storage = metadata?.storage || {}
                  const guestOs = metadata?.guest_full_name || metadata?.summary?.guest_full_name || '-'
                  const ip = vm?.guest_ip || guest?.ip_address || metadata?.summary?.guest_ip_address || '-'
                  const storageValue =
                    typeof storage?.provisioned_bytes === 'number' && storage.provisioned_bytes > 0
                      ? formatBytes(storage.provisioned_bytes)
                      : '-'
                  const hostValue = metadata?.host_name || '-'
                  const clusterValue = metadata?.cluster_name ? ` (${metadata.cluster_name})` : ''
                  return (
                    <Fragment key={key}>
                      <tr key={key} className={expanded ? 'vm-row expanded' : 'vm-row'}>
                        <td>
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => toggleVM(vm)}
                            aria-label={`Select ${vm.name}`}
                          />
                        </td>
                        <td>
                          <div className="vm-name-cell">
                            <strong>{vm.name}</strong>
                            <span>{metadata?.vmx_datastore_path || metadata?.instance_uuid || '-'}</span>
                          </div>
                        </td>
                        <td><span className="pill neutral">{vm.source}</span></td>
                        <td>{guestOs}</td>
                        <td>{ip}</td>
                        <td>{vm.cpu ?? '-'}</td>
                        <td>{vm.ram ?? '-'}</td>
                        <td>{storageValue}</td>
                        <td>{`${hostValue}${clusterValue}`}</td>
                        <td><span className={`pill ${powerClass(vm.power_state)}`}>{vm.power_state || '-'}</span></td>
                        <td>
                          <button
                            className="secondary-btn slim-btn"
                            onClick={() => setExpandedVmKey((current) => (current === key ? '' : key))}
                          >
                            {expanded ? 'Hide' : 'View'}
                          </button>
                        </td>
                      </tr>
                      {expanded && (
                        <tr className="vm-details-row">
                          <td colSpan={11}>
                            <VmSpecsPanel vm={vm} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>

      {showVmwareModal && (
        <div className="modal-backdrop" onClick={() => setShowVmwareModal(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <h3>Connect VMware ESXi</h3>
            <div className="modal-grid">
              <label>
                <span>Label</span>
                <input value={vmwareForm.label} onChange={(e) => setVmwareForm((v) => ({ ...v, label: e.target.value }))} />
              </label>
              <label>
                <span>Host / IP</span>
                <input value={vmwareForm.host} onChange={(e) => setVmwareForm((v) => ({ ...v, host: e.target.value }))} />
              </label>
              <label>
                <span>Port</span>
                <input
                  type="number"
                  min="1"
                  max="65535"
                  value={vmwareForm.port}
                  onChange={(e) => setVmwareForm((v) => ({ ...v, port: Number.parseInt(e.target.value || '443', 10) || 443 }))}
                />
              </label>
              <label>
                <span>Username</span>
                <input value={vmwareForm.username} onChange={(e) => setVmwareForm((v) => ({ ...v, username: e.target.value }))} />
              </label>
              <label className="span-2">
                <span>Password</span>
                <input type="password" value={vmwareForm.password} onChange={(e) => setVmwareForm((v) => ({ ...v, password: e.target.value }))} />
              </label>
              <label className="checkbox-line span-2">
                <input
                  type="checkbox"
                  checked={vmwareForm.insecure}
                  onChange={(e) => setVmwareForm((v) => ({ ...v, insecure: e.target.checked }))}
                />
                <span>Disable SSL verification (insecure)</span>
              </label>
            </div>
            {vmwareTestMessage && (
              <div className={`alert ${vmwareTestPassed ? 'success' : 'error'}`}>{vmwareTestMessage}</div>
            )}
            <div className="modal-actions">
              <button className="secondary-btn" onClick={handleVmwareTest} disabled={vmwareTesting || vmwareConnecting}>
                {vmwareTesting ? 'Testing...' : 'Test'}
              </button>
              <button
                className="primary-btn"
                onClick={handleVmwareConnect}
                disabled={!vmwareTestPassed || vmwareTesting || vmwareConnecting}
              >
                {vmwareConnecting ? 'Connecting...' : 'Connect'}
              </button>
            </div>
          </div>
        </div>
      )}

      {showOpenstackModal && (
        <div className="modal-backdrop" onClick={() => setShowOpenstackModal(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <h3>Connect OpenStack</h3>
            <div className="modal-grid">
              <label>
                <span>Label</span>
                <input value={openstackForm.label} onChange={(e) => setOpenstackForm((v) => ({ ...v, label: e.target.value }))} />
              </label>
              <label>
                <span>Auth URL</span>
                <input value={openstackForm.auth_url} onChange={(e) => setOpenstackForm((v) => ({ ...v, auth_url: e.target.value }))} />
              </label>
              <label>
                <span>Username</span>
                <input value={openstackForm.username} onChange={(e) => setOpenstackForm((v) => ({ ...v, username: e.target.value }))} />
              </label>
              <label>
                <span>Password</span>
                <input type="password" value={openstackForm.password} onChange={(e) => setOpenstackForm((v) => ({ ...v, password: e.target.value }))} />
              </label>
              <label>
                <span>Project</span>
                <input value={openstackForm.project_name} onChange={(e) => setOpenstackForm((v) => ({ ...v, project_name: e.target.value }))} />
              </label>
              <label>
                <span>Region</span>
                <input value={openstackForm.region_name} onChange={(e) => setOpenstackForm((v) => ({ ...v, region_name: e.target.value }))} />
              </label>
              <label>
                <span>User domain</span>
                <input value={openstackForm.user_domain_name} onChange={(e) => setOpenstackForm((v) => ({ ...v, user_domain_name: e.target.value }))} />
              </label>
              <label>
                <span>Project domain</span>
                <input value={openstackForm.project_domain_name} onChange={(e) => setOpenstackForm((v) => ({ ...v, project_domain_name: e.target.value }))} />
              </label>
              <label>
                <span>Interface</span>
                <input value={openstackForm.interface} onChange={(e) => setOpenstackForm((v) => ({ ...v, interface: e.target.value }))} />
              </label>
              <label>
                <span>Identity API version</span>
                <input value={openstackForm.identity_api_version} onChange={(e) => setOpenstackForm((v) => ({ ...v, identity_api_version: e.target.value }))} />
              </label>
              <label className="span-2">
                <span>Image endpoint override (optional)</span>
                <input
                  value={openstackForm.image_endpoint_override}
                  onChange={(e) => setOpenstackForm((v) => ({ ...v, image_endpoint_override: e.target.value }))}
                />
              </label>
              <label className="checkbox-line span-2">
                <input
                  type="checkbox"
                  checked={openstackForm.verify}
                  onChange={(e) => setOpenstackForm((v) => ({ ...v, verify: e.target.checked }))}
                />
                <span>Enable SSL verification</span>
              </label>
            </div>
            {openstackTestMessage && (
              <div className={`alert ${openstackTestPassed ? 'success' : 'error'}`}>{openstackTestMessage}</div>
            )}
            <div className="modal-actions">
              <button className="secondary-btn" onClick={handleOpenstackTest} disabled={openstackTesting || openstackConnecting}>
                {openstackTesting ? 'Testing...' : 'Test'}
              </button>
              <button
                className="primary-btn"
                onClick={handleOpenstackConnect}
                disabled={!openstackTestPassed || openstackTesting || openstackConnecting}
              >
                {openstackConnecting ? 'Connecting...' : 'Connect'}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}

function makeKey(vm) {
  return `${vm.source}::${vm.name}`
}

function buildDefaultSpec(vm) {
  const metadata = vm?.metadata || {}
  return {
    flavor_id: '',
    cpu: vm?.cpu ?? '',
    ram: vm?.ram ?? '',
    network_id: '',
    network_name: inferNetworkName(metadata),
    fixed_ip: inferFixedIp(metadata),
    extra_disks_gb: '',
  }
}

function buildOverrides(spec) {
  const overrides = {}

  if (typeof spec?.flavor_id === 'string' && spec.flavor_id.trim()) {
    overrides.flavor_id = spec.flavor_id.trim()
  }

  const cpu = parsePositiveInteger(spec?.cpu)
  if (cpu) overrides.cpu = cpu

  const ram = parsePositiveInteger(spec?.ram)
  if (ram) overrides.ram = ram

  const extraDisks = parseDiskList(spec?.extra_disks_gb)
  if (extraDisks.length) overrides.extra_disks_gb = extraDisks

  const network = {}
  if (typeof spec?.network_id === 'string' && spec.network_id.trim()) {
    network.network_id = spec.network_id.trim()
  }
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

function formatNetworkLabel(network) {
  const name = network?.name || network?.id || 'network'
  const external = network?.is_router_external ? 'external' : 'tenant'
  const subnets = Array.isArray(network?.subnets) ? network.subnets : []
  const subnetLabels = subnets
    .map((subnet) => {
      const cidr = subnet?.cidr
      const pool = Array.isArray(subnet?.allocation_pools) ? subnet.allocation_pools[0] : null
      const poolLabel = pool?.start && pool?.end ? ` [${pool.start}-${pool.end}]` : ''
      return cidr ? `${cidr}${poolLabel}` : null
    })
    .filter(Boolean)
  const subnetSuffix = subnetLabels.length ? ` - ${subnetLabels.slice(0, 1).join(', ')}` : ''
  const extra = subnetLabels.length > 2 ? ' +more' : ''
  const label = `${name} (${external})${subnetSuffix}${extra}`
  return truncateMiddle(label, 90)
}

function formatBytes(value) {
  const num = Number(value)
  if (!Number.isFinite(num) || num <= 0) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  let size = num
  let idx = 0
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024
    idx += 1
  }
  return `${size.toFixed(size >= 100 ? 0 : 1)} ${units[idx]}`
}

function getAvailableIps(network) {
  const subnets = Array.isArray(network?.subnets) ? network.subnets : []
  const items = []
  let totalCount = 0
  let truncated = false

  for (const subnet of subnets) {
    const available = Array.isArray(subnet?.available_ips) ? subnet.available_ips : []
    if (Number.isFinite(subnet?.available_ip_count)) {
      totalCount += subnet.available_ip_count
    } else {
      totalCount += available.length
    }
    if (subnet?.available_ips_truncated) truncated = true
    for (const ip of available) {
      items.push({ ip, subnet })
    }
  }

  return { items, totalCount, truncated }
}

function renderFixedIpField({ spec, network, onChange }) {
  const { items, totalCount, truncated } = getAvailableIps(network)
  const showSelect = items.length > 0
  const showManual = !showSelect || truncated
  const displayCount = formatCount(totalCount)

  return (
    <div className="fixed-ip-field">
      {showSelect && (
        <select value={spec.fixed_ip} onChange={(e) => onChange(e.target.value)}>
          <option value="">Auto-assign</option>
          {items.map(({ ip, subnet }) => (
            <option key={`${subnet?.id || 'subnet'}-${ip}`} value={ip}>
              {ip} {subnet?.cidr ? `(${subnet.cidr})` : ''}
            </option>
          ))}
        </select>
      )}
      {showManual && (
        <input
          type="text"
          value={spec.fixed_ip}
          onChange={(e) => onChange(e.target.value)}
          placeholder="192.168.1.20"
        />
      )}
      {showSelect && (
        <span className="helper-text">
          {truncated
            ? `Showing ${formatCount(items.length)} of ${displayCount} available IPs. Use manual entry if needed.`
            : `${displayCount} IPs available in selected network.`}
        </span>
      )}
    </div>
  )
}

function formatCount(value) {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return '-'
  if (n > 1000000000) return 'many'
  return n.toLocaleString()
}

function truncateMiddle(value, maxLen) {
  const text = String(value || '')
  if (text.length <= maxLen) return text
  const side = Math.max(8, Math.floor((maxLen - 3) / 2))
  return `${text.slice(0, side)}...${text.slice(-side)}`
}

function VmSpecsPanel({ vm }) {
  const metadata = vm?.metadata || {}
  const guest = metadata?.guest || {}
  const storage = metadata?.storage || {}
  const disks = Array.isArray(vm?.disks) ? vm.disks : []
  const nics = Array.isArray(vm?.nics) ? vm.nics : []
  const guestNics = Array.isArray(guest?.nics) ? guest.nics : []
  const datastores = Array.isArray(metadata?.datastores) ? metadata.datastores : []
  const networks = Array.isArray(metadata?.networks) ? metadata.networks : []

  return (
    <div className="vm-specs-panel">
      <div className="vm-specs-grid">
        <section className="vm-spec-block">
          <h4>Compute</h4>
          <dl>
            <div><dt>vCPU</dt><dd>{vm?.cpu ?? '-'}</dd></div>
            <div><dt>RAM</dt><dd>{vm?.ram ? `${vm.ram} MB` : '-'}</dd></div>
            <div><dt>Firmware</dt><dd>{metadata?.firmware || '-'}</dd></div>
            <div><dt>HW version</dt><dd>{metadata?.vm_hw_version || '-'}</dd></div>
            <div><dt>Guest tools</dt><dd>{guest?.tools_running_status || '-'}</dd></div>
            <div><dt>Boot time</dt><dd>{formatDateTime(metadata?.boot_time)}</dd></div>
          </dl>
        </section>

        <section className="vm-spec-block">
          <h4>Storage</h4>
          <dl>
            <div><dt>Disk count</dt><dd>{disks.length}</dd></div>
            <div><dt>Provisioned</dt><dd>{formatBytes(storage?.provisioned_bytes)}</dd></div>
            <div><dt>Committed</dt><dd>{formatBytes(storage?.committed_bytes)}</dd></div>
            <div><dt>Snapshot count</dt><dd>{metadata?.snapshot_count ?? 0}</dd></div>
            <div><dt>Datastores</dt><dd>{datastores.length ? datastores.join(', ') : '-'}</dd></div>
          </dl>
          <div className="subtable">
            {disks.slice(0, 6).map((disk, idx) => (
              <div key={`${disk?.label || 'disk'}-${idx}`} className="subrow">
                <span>{disk?.label || `disk${idx}`}</span>
                <strong>{formatBytes(disk?.size_bytes)}</strong>
              </div>
            ))}
            {disks.length > 6 && <div className="subrow muted">+{disks.length - 6} more disks</div>}
          </div>
        </section>

        <section className="vm-spec-block">
          <h4>Network</h4>
          <dl>
            <div><dt>Primary IP</dt><dd>{vm?.guest_ip || guest?.ip_address || '-'}</dd></div>
            <div><dt>NIC count</dt><dd>{nics.length}</dd></div>
            <div><dt>Networks</dt><dd>{networks.length ? networks.join(', ') : '-'}</dd></div>
            <div><dt>Host</dt><dd>{metadata?.host_name || '-'}</dd></div>
            <div><dt>Cluster</dt><dd>{metadata?.cluster_name || '-'}</dd></div>
          </dl>
          <div className="subtable">
            {nics.slice(0, 6).map((nic, idx) => (
              <div key={`${nic?.mac_address || 'nic'}-${idx}`} className="subrow">
                <span>{nic?.network || nic?.label || `nic${idx}`}</span>
                <strong>{nic?.mac_address || '-'}</strong>
              </div>
            ))}
            {guestNics.slice(0, 3).map((nic, idx) => (
              <div key={`guest-nic-${idx}`} className="subrow muted">
                <span>{nic?.network || 'guest nic'}</span>
                <strong>{Array.isArray(nic?.ips) && nic.ips.length ? nic.ips.join(', ') : '-'}</strong>
              </div>
            ))}
          </div>
        </section>

        <section className="vm-spec-block">
          <h4>Identity</h4>
          <dl>
            <div><dt>Power</dt><dd>{vm?.power_state || '-'}</dd></div>
            <div><dt>Connection</dt><dd>{metadata?.connection_state || '-'}</dd></div>
            <div><dt>Instance UUID</dt><dd className="mono">{metadata?.instance_uuid || '-'}</dd></div>
            <div><dt>BIOS UUID</dt><dd className="mono">{metadata?.bios_uuid || '-'}</dd></div>
            <div><dt>MOID</dt><dd className="mono">{metadata?.moid || '-'}</dd></div>
          </dl>
        </section>
      </div>

      <details className="raw-specs">
        <summary>Raw JSON</summary>
        <pre>{JSON.stringify(vm, null, 2)}</pre>
      </details>
    </div>
  )
}

function formatDateTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString()
}

function powerClass(value) {
  const normalized = String(value || '').toLowerCase()
  if (normalized.includes('on')) return 'success'
  if (normalized.includes('off')) return 'neutral'
  return 'warning'
}

async function waitForTaskCompletion(taskId, timeoutMs = 60000, intervalMs = 1200) {
  const startedAt = Date.now()
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
