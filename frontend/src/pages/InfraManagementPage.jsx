import { useEffect, useState } from 'react'
import {
  closeOpenstackEndpointSession,
  connectOpenstackEndpoint,
  fetchOpenstackEndpointSessions,
  testOpenstackEndpoint,
} from '../api/openstack'
import {
  closeVMwareEndpointSession,
  connectVMwareEndpoint,
  fetchVMwareEndpointSessions,
  testVMwareEndpoint,
} from '../api/vmware'
import { Alert, Button, Card, PageHeader, Table } from '../components/ui'

const defaultVmwareForm = {
  label: '',
  type: 'esxi',
  host: '',
  port: 443,
  username: '',
  password: '',
  insecure: true,
  datacenter: '',
}

const defaultOpenstackForm = {
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
}

function InfraManagementPage() {
  const [vmwareEndpoints, setVmwareEndpoints] = useState([])
  const [openstackEndpoints, setOpenstackEndpoints] = useState([])
  const [vmwareForm, setVmwareForm] = useState(defaultVmwareForm)
  const [openstackForm, setOpenstackForm] = useState(defaultOpenstackForm)
  const [vmwareMessage, setVmwareMessage] = useState('')
  const [openstackMessage, setOpenstackMessage] = useState('')
  const [vmwareError, setVmwareError] = useState('')
  const [openstackError, setOpenstackError] = useState('')
  const [vmwareTesting, setVmwareTesting] = useState(false)
  const [openstackTesting, setOpenstackTesting] = useState(false)
  const [vmwareAdding, setVmwareAdding] = useState(false)
  const [openstackAdding, setOpenstackAdding] = useState(false)

  useEffect(() => {
    loadEndpoints()
  }, [])

  async function loadEndpoints() {
    const [vmwareItems, openstackItems] = await Promise.all([
      fetchVMwareEndpointSessions(),
      fetchOpenstackEndpointSessions(),
    ])
    setVmwareEndpoints(vmwareItems)
    setOpenstackEndpoints(openstackItems)
  }

  async function handleVmwareTest() {
    setVmwareTesting(true)
    setVmwareError('')
    setVmwareMessage('')
    try {
      const res = await testVMwareEndpoint(vmwareForm)
      setVmwareMessage(res?.message || 'Test reussi.')
    } catch (err) {
      setVmwareError(err.message || 'Echec du test VMware ESXi.')
    } finally {
      setVmwareTesting(false)
    }
  }

  async function handleVmwareAdd() {
    setVmwareAdding(true)
    setVmwareError('')
    setVmwareMessage('')
    try {
      await connectVMwareEndpoint(vmwareForm)
      setVmwareForm(defaultVmwareForm)
      setVmwareMessage('ESXi ajoute.')
      await loadEndpoints()
    } catch (err) {
      setVmwareError(err.message || 'Ajout ESXi impossible.')
    } finally {
      setVmwareAdding(false)
    }
  }

  async function handleOpenstackTest() {
    setOpenstackTesting(true)
    setOpenstackError('')
    setOpenstackMessage('')
    try {
      const res = await testOpenstackEndpoint(openstackForm)
      setOpenstackMessage(res?.message || 'Test reussi.')
    } catch (err) {
      setOpenstackError(err.message || 'Echec du test OpenStack.')
    } finally {
      setOpenstackTesting(false)
    }
  }

  async function handleOpenstackAdd() {
    setOpenstackAdding(true)
    setOpenstackError('')
    setOpenstackMessage('')
    try {
      await connectOpenstackEndpoint(openstackForm)
      setOpenstackForm(defaultOpenstackForm)
      setOpenstackMessage('OpenStack ajoute.')
      await loadEndpoints()
    } catch (err) {
      setOpenstackError(err.message || 'Ajout OpenStack impossible.')
    } finally {
      setOpenstackAdding(false)
    }
  }

  async function removeVmwareEndpoint(id) {
    await closeVMwareEndpointSession(id)
    await loadEndpoints()
  }

  async function removeOpenstackEndpoint(id) {
    await closeOpenstackEndpointSession(id)
    await loadEndpoints()
  }

  return (
    <section>
      <PageHeader
        eyebrow="Infrastructure"
        title="Environnements"
        description="Ajoutez les sources ESXi et les cibles OpenStack utilisees par les migrations."
      />

      <div className="grid-12">
        <Card className="span-6">
          <div className="toolbar">
            <h3>Ajouter ESXi</h3>
          </div>
          {vmwareError ? <Alert>{vmwareError}</Alert> : null}
          {vmwareMessage ? <Alert tone="success">{vmwareMessage}</Alert> : null}
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
            <label>
              <span>Source Type</span>
              <select value={vmwareForm.type} onChange={(e) => setVmwareForm((v) => ({ ...v, type: e.target.value }))}>
                <option value="esxi">ESXi</option>
                <option value="vcenter">vCenter</option>
              </select>
            </label>
            {vmwareForm.type === 'vcenter' && (
              <label>
                <span>Datacenter (optional)</span>
                <input
                  value={vmwareForm.datacenter}
                  onChange={(e) => setVmwareForm((v) => ({ ...v, datacenter: e.target.value }))}
                  placeholder="ha-datacenter"
                />
              </label>
            )}
            <label className="checkbox-line span-2">
              <input
                type="checkbox"
                checked={vmwareForm.insecure}
                onChange={(e) => setVmwareForm((v) => ({ ...v, insecure: e.target.checked }))}
              />
              <span>Disable SSL verification</span>
            </label>
          </div>
          <div className="form-actions">
            <Button variant="secondary" onClick={handleVmwareTest} disabled={vmwareTesting || vmwareAdding}>
              {vmwareTesting ? 'Testing...' : 'Test'}
            </Button>
            <Button onClick={handleVmwareAdd} disabled={vmwareTesting || vmwareAdding}>
              {vmwareAdding ? 'Adding...' : 'Ajouter'}
            </Button>
          </div>
        </Card>

        <Card className="span-6">
          <div className="toolbar">
            <h3>Ajouter OpenStack</h3>
          </div>
          {openstackError ? <Alert>{openstackError}</Alert> : null}
          {openstackMessage ? <Alert tone="success">{openstackMessage}</Alert> : null}
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
              <span>Image endpoint override</span>
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
          <div className="form-actions">
            <Button variant="secondary" onClick={handleOpenstackTest} disabled={openstackTesting || openstackAdding}>
              {openstackTesting ? 'Testing...' : 'Test'}
            </Button>
            <Button onClick={handleOpenstackAdd} disabled={openstackTesting || openstackAdding}>
              {openstackAdding ? 'Adding...' : 'Ajouter'}
            </Button>
          </div>
        </Card>
      </div>

      <Card>
        <div className="toolbar">
          <h3>ESXi ajoutes</h3>
          <Button variant="secondary" onClick={loadEndpoints}>Refresh</Button>
        </div>
        <Table>
          <thead>
            <tr>
              <th>Label</th>
              <th>Host</th>
              <th>Username</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {vmwareEndpoints.map((item) => (
              <tr key={item.id}>
                <td>{item.label || '-'}</td>
                <td>{item.host}:{item.port}</td>
                <td>{item.username}</td>
                <td>{item.last_test_status || '-'}</td>
                <td>
                  <Button variant="danger" className="slim-btn" onClick={() => removeVmwareEndpoint(item.id)}>
                    Remove
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      </Card>

      <Card>
        <div className="toolbar">
          <h3>OpenStack ajoutes</h3>
          <Button variant="secondary" onClick={loadEndpoints}>Refresh</Button>
        </div>
        <Table>
          <thead>
            <tr>
              <th>Label</th>
              <th>Auth URL</th>
              <th>Project</th>
              <th>Region</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {openstackEndpoints.map((item) => (
              <tr key={item.id}>
                <td>{item.label || '-'}</td>
                <td>{item.auth_url}</td>
                <td>{item.project_name}</td>
                <td>{item.region_name || '-'}</td>
                <td>{item.last_test_status || '-'}</td>
                <td>
                  <Button variant="danger" className="slim-btn" onClick={() => removeOpenstackEndpoint(item.id)}>
                    Remove
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      </Card>
    </section>
  )
}

export default InfraManagementPage
