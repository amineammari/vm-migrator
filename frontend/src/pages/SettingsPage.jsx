import { useEffect, useState } from 'react'
import { createOpenStackNetwork, fetchOpenStackNetworkCatalog } from '../api/openstack'
import {
  fetchOpenStackRouters,
  createOpenStackRouter,
  attachSubnetToRouter,
} from '../api/openstack'
import PanelState from '../components/PanelState'
import { Alert, Badge, Button, Card, Field, PageHeader, Table, Toggle } from '../components/ui'

function SettingsPage() {
  const [networks, setNetworks] = useState([])
  const [externalNetworks, setExternalNetworks] = useState([])
  const [floatingIps, setFloatingIps] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [networkCreateForm, setNetworkCreateForm] = useState({
    name: '',
    subnet_name: '',
    cidr: '192.168.100.0/24',
    gateway_ip: '',
    allocation_pool_start: '',
    allocation_pool_end: '',
    dns_nameservers: '8.8.8.8, 1.1.1.1',
    enable_dhcp: true,
  })
  const [networkCreateBusy, setNetworkCreateBusy] = useState(false)
  const [networkCreateMessage, setNetworkCreateMessage] = useState('')
  const [networkCreateError, setNetworkCreateError] = useState('')
  const [tablePage, setTablePage] = useState(1)
  const [tablePageSize, setTablePageSize] = useState(10)
  const [defaults, setDefaults] = useState({
    networkProfile: 'production',
    dhcp: true,
    floatingIp: false,
    dns: '8.8.8.8, 1.1.1.1',
  })

  const openstackEndpointSessionId = Number(localStorage.getItem('active_openstack_endpoint_session_id')) || null

  useEffect(() => {
    loadNetworkCatalog()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openstackEndpointSessionId])

  // Remove duplicate declarations of tableTotalPages and pagedNetworks

  // (Suppression des redéfinitions, une seule version de chaque fonction et état doit rester)

  useEffect(() => {
    loadRouters()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openstackEndpointSessionId])

  useEffect(() => {
    setTablePage(1)
  }, [networks.length])

  const tableTotalPages = Math.max(1, Math.ceil(networks.length / Math.max(1, tablePageSize)))
  const pagedNetworks = networks.slice((tablePage - 1) * tablePageSize, tablePage * tablePageSize)

  useEffect(() => {
    if (tablePage > tableTotalPages) {
      setTablePage(tableTotalPages)
    }
  }, [tablePage, tableTotalPages])

  async function loadNetworkCatalog() {
    setLoading(true)
    try {
      const data = await fetchOpenStackNetworkCatalog(openstackEndpointSessionId)
      setNetworks(data?.items || [])
      setExternalNetworks(data?.external_networks || [])
      setFloatingIps(data?.available_floating_ips || [])
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load network settings.')
    } finally {
      setLoading(false)
    }
  }

  async function handleCreateNetwork(event) {
    event.preventDefault()

    if (!openstackEndpointSessionId) {
      setNetworkCreateError('Connect an OpenStack endpoint from VM List first.')
      return
    }

    setNetworkCreateBusy(true)
    setNetworkCreateMessage('')
    setNetworkCreateError('')
    try {
      const res = await createOpenStackNetwork({
        openstack_endpoint_session_id: openstackEndpointSessionId,
        name: networkCreateForm.name,
        subnet_name: networkCreateForm.subnet_name,
        cidr: networkCreateForm.cidr,
        gateway_ip: networkCreateForm.gateway_ip,
        allocation_pool_start: networkCreateForm.allocation_pool_start,
        allocation_pool_end: networkCreateForm.allocation_pool_end,
        enable_dhcp: networkCreateForm.enable_dhcp,
        dns_nameservers: String(networkCreateForm.dns_nameservers || '')
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
      })
      const refreshed = await fetchOpenStackNetworkCatalog(openstackEndpointSessionId)
      setNetworks(refreshed?.items || [])
      setExternalNetworks(refreshed?.external_networks || [])
      setFloatingIps(refreshed?.available_floating_ips || [])
      setNetworkCreateMessage(res?.message || 'Network created successfully.')
      setNetworkCreateForm((current) => ({
        ...current,
        name: '',
        subnet_name: '',
      }))
    } catch (err) {
      setNetworkCreateError(err.message || 'Failed to create OpenStack network.')
    } finally {
      setNetworkCreateBusy(false)
    }
  }

  async function loadRouters() {
    if (!openstackEndpointSessionId) return
    setRouters(await fetchOpenStackRouters(openstackEndpointSessionId))
  }

  async function handleCreateRouter(e) {
    e.preventDefault()
    setRouterCreateMsg('')
    try {
      await createOpenStackRouter({
        openstack_endpoint_session_id: openstackEndpointSessionId,
        name: routerForm.name,
        external_network_id: routerForm.external_network_id,
      })
      setRouterCreateMsg('Router created!')
      setRouterForm({ name: '', external_network_id: '' })
      loadRouters()
    } catch (err) {
      setRouterCreateMsg(err.message || 'Router creation failed')
    }
  }

  async function handleAttachSubnet(e) {
    e.preventDefault()
    setRouterAttachMsg('')
    try {
      await attachSubnetToRouter({
        openstack_endpoint_session_id: openstackEndpointSessionId,
        router_id: routerAttachForm.router_id,
        subnet_id: routerAttachForm.subnet_id,
      })
      setRouterAttachMsg('Subnet attached!')
      setRouterAttachForm({ router_id: '', subnet_id: '' })
      loadRouters()
    } catch (err) {
      setRouterAttachMsg(err.message || 'Attach failed')
    }
  }

  // --- Router management state ---
  const [routers, setRouters] = useState([])
  const [routerForm, setRouterForm] = useState({ name: '', external_network_id: '' })
  const [routerCreateMsg, setRouterCreateMsg] = useState('')
  const [routerAttachMsg, setRouterAttachMsg] = useState('')
  const [routerAttachForm, setRouterAttachForm] = useState({ router_id: '', subnet_id: '' })

  return (
    <section>
      <PageHeader
        eyebrow="Settings"
        title="Network Configuration"
        description="Review OpenStack network inventory and target migration defaults."
        actions={
          <Button variant="secondary" onClick={loadNetworkCatalog} disabled={loading}>
            Refresh Catalog
          </Button>
        }
      />

      {error ? <Alert>{error}</Alert> : null}

      <div className="grid-12">
        <Card className="span-5">
          <h3>Migration defaults</h3>
          <div className="settings-stack">
            <Field label="Network profile">
              <select
                value={defaults.networkProfile}
                onChange={(event) => setDefaults((current) => ({ ...current, networkProfile: event.target.value }))}
              >
                <option value="production">Production VLAN</option>
                <option value="staging">Staging</option>
                <option value="isolated">Isolated migration subnet</option>
              </select>
            </Field>
            <Field label="DNS nameservers">
              <input
                value={defaults.dns}
                onChange={(event) => setDefaults((current) => ({ ...current, dns: event.target.value }))}
              />
            </Field>
            <Toggle
              label="Enable DHCP by default"
              checked={defaults.dhcp}
              onChange={(event) => setDefaults((current) => ({ ...current, dhcp: event.target.checked }))}
            />
            <Toggle
              label="Attach Floating IP when available"
              checked={defaults.floatingIp}
              onChange={(event) => setDefaults((current) => ({ ...current, floatingIp: event.target.checked }))}
            />
          </div>
        </Card>

        <Card className="span-7">
          <h3>Create OpenStack network</h3>
          <p className="section-copy">Create a tenant network and subnet in the active OpenStack project.</p>
          {!openstackEndpointSessionId ? (
            <PanelState title="No OpenStack connection" message="Connect OpenStack from VM List before creating networks." />
          ) : (
            <form className="spec-fields" onSubmit={handleCreateNetwork}>
              <Field label="Network name">
                <input
                  value={networkCreateForm.name}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, name: event.target.value }))}
                  placeholder="private-migration"
                  required
                />
              </Field>
              <Field label="Subnet name">
                <input
                  value={networkCreateForm.subnet_name}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, subnet_name: event.target.value }))}
                  placeholder="private-migration-subnet"
                  required
                />
              </Field>
              <Field label="CIDR">
                <input
                  value={networkCreateForm.cidr}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, cidr: event.target.value }))}
                  placeholder="192.168.100.0/24"
                  required
                />
              </Field>
              <Field label="Gateway IP">
                <input
                  value={networkCreateForm.gateway_ip}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, gateway_ip: event.target.value }))}
                  placeholder="192.168.100.1"
                />
              </Field>
              <Field label="Allocation start">
                <input
                  value={networkCreateForm.allocation_pool_start}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, allocation_pool_start: event.target.value }))}
                  placeholder="192.168.100.50"
                />
              </Field>
              <Field label="Allocation end">
                <input
                  value={networkCreateForm.allocation_pool_end}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, allocation_pool_end: event.target.value }))}
                  placeholder="192.168.100.200"
                />
              </Field>
              <Field label="DNS nameservers" className="span-2">
                <input
                  value={networkCreateForm.dns_nameservers}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, dns_nameservers: event.target.value }))}
                  placeholder="8.8.8.8, 1.1.1.1"
                />
              </Field>
              <label className="checkbox-line span-2">
                <input
                  type="checkbox"
                  checked={networkCreateForm.enable_dhcp}
                  onChange={(event) => setNetworkCreateForm((current) => ({ ...current, enable_dhcp: event.target.checked }))}
                />
                <span>Enable DHCP on subnet</span>
              </label>
              <div className="form-actions span-2">
                <Button type="submit" disabled={networkCreateBusy}>
                  {networkCreateBusy ? 'Creating...' : 'Create Network'}
                </Button>
              </div>
              {networkCreateError ? <Alert className="span-2">{networkCreateError}</Alert> : null}
              {networkCreateMessage ? <Alert tone="success" className="span-2">{networkCreateMessage}</Alert> : null}
            </form>
          )}
        </Card>

        <Card className="span-12">
          <h3>OpenStack network catalog</h3>
          {loading ? (
            <PanelState title="Loading networks" message="Reading tenant networks and Floating IP pool..." />
          ) : networks.length === 0 ? (
            <PanelState title="No networks loaded" message="Connect OpenStack from VM List, then refresh the catalog." />
          ) : (
            <Table
              pagination={{
                page: tablePage,
                pageSize: tablePageSize,
                totalItems: networks.length,
                onPageChange: setTablePage,
                onPageSizeChange: (nextPageSize) => {
                  setTablePageSize(nextPageSize)
                  setTablePage(1)
                },
                label: 'networks',
              }}
            >
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Subnets</th>
                    <th>Admin</th>
                  </tr>
                </thead>
                <tbody>
                  {pagedNetworks.map((network) => (
                    <tr key={network.id}>
                      <td>
                        <div className="vm-name-cell">
                          <strong>{network.name || network.id}</strong>
                          <span>{network.id}</span>
                        </div>
                      </td>
                      <td><Badge tone={network.is_router_external ? 'warning' : 'info'}>{network.is_router_external ? 'External' : 'Tenant'}</Badge></td>
                      <td>{formatSubnets(network.subnets)}</td>
                      <td>{network.admin_state_up === false ? 'Down' : 'Up'}</td>
                    </tr>
                  ))}
                </tbody>
            </Table>
          )}
        </Card>

        <Card className="span-12">
          <h3>OpenStack Routers</h3>
          <Button onClick={loadRouters}>Refresh Routers</Button>
          <ul>
            {routers.map(r => (
              <li key={r.id}>{r.name} ({r.id})</li>
            ))}
          </ul>
          <form onSubmit={handleCreateRouter} style={{ marginTop: 16 }}>
            <Field label="Router name">
              <input value={routerForm.name} onChange={e => setRouterForm(f => ({ ...f, name: e.target.value }))} required />
            </Field>
            <Field label="External network">
              <select value={routerForm.external_network_id} onChange={e => setRouterForm(f => ({ ...f, external_network_id: e.target.value }))} required>
                <option value="">Select external network</option>
                {externalNetworks.map(n => (
                  <option key={n.id} value={n.id}>{n.name || n.id}</option>
                ))}
              </select>
            </Field>
            <Button type="submit">Create Router</Button>
            {routerCreateMsg && <Alert>{routerCreateMsg}</Alert>}
          </form>
          <form onSubmit={handleAttachSubnet} style={{ marginTop: 16 }}>
            <Field label="Router">
              <select value={routerAttachForm.router_id} onChange={e => setRouterAttachForm(f => ({ ...f, router_id: e.target.value }))} required>
                <option value="">Select router</option>
                {routers.map(r => (
                  <option key={r.id} value={r.id}>{r.name || r.id}</option>
                ))}
              </select>
            </Field>
            <Field label="Subnet">
              <select value={routerAttachForm.subnet_id} onChange={e => setRouterAttachForm(f => ({ ...f, subnet_id: e.target.value }))} required>
                <option value="">Select subnet</option>
                {networks.flatMap(n => (n.subnets || [])).map(s => (
                  <option key={s.id} value={s.id}>{s.name || s.cidr}</option>
                ))}
              </select>
            </Field>
            <Button type="submit">Attach Subnet</Button>
            {routerAttachMsg && <Alert>{routerAttachMsg}</Alert>}
          </form>
        </Card>
      </div>

      <div className="stats-grid settings-stats">
        <div className="stat-card"><p>Tenant networks</p><strong>{networks.length}</strong></div>
        <div className="stat-card"><p>External networks</p><strong>{externalNetworks.length}</strong></div>
        <div className="stat-card"><p>Available Floating IPs</p><strong>{floatingIps.length}</strong></div>
      </div>
    </section>
  )
}

function formatSubnets(subnets) {
  const items = Array.isArray(subnets) ? subnets : []
  if (!items.length) return '-'
  return items.map((item) => item.cidr).filter(Boolean).join(', ') || '-'
}

export default SettingsPage
