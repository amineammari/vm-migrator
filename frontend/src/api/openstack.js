import { apiFetch } from './client'

function withSessionQuery(path, openstackEndpointSessionId = null, params = {}) {
  const query = new URLSearchParams()
  if (openstackEndpointSessionId) {
    query.set('openstack_endpoint_session_id', String(openstackEndpointSessionId))
  }
  if (params.projectName) {
    query.set('project_name', String(params.projectName))
  }
  const suffix = query.toString()
  return suffix ? `${path}?${suffix}` : path
}

export async function fetchOpenStackHealth(openstackEndpointSessionId = null, params = {}) {
  return apiFetch(withSessionQuery('/api/openstack/health', openstackEndpointSessionId, params))
}

export async function fetchOpenStackFlavors(openstackEndpointSessionId = null, params = {}) {
  const data = await apiFetch(withSessionQuery('/api/openstack/flavors', openstackEndpointSessionId, params))
  return data?.items || []
}

export async function fetchOpenStackNetworks(openstackEndpointSessionId = null, params = {}) {
  const data = await apiFetch(withSessionQuery('/api/openstack/networks', openstackEndpointSessionId, params))
  return data?.items || []
}

export async function fetchOpenStackNetworkCatalog(openstackEndpointSessionId = null, params = {}) {
  const data = await apiFetch(withSessionQuery('/api/openstack/networks', openstackEndpointSessionId, params))
  return {
    items: data?.items || [],
    external_networks: data?.external_networks || [],
    available_floating_ips: data?.available_floating_ips || [],
  }
}

export async function fetchOpenstackEndpointSessions() {
  const data = await apiFetch('/api/openstack/endpoints')
  return data?.items || []
}

export async function fetchOpenstackEndpointProjects(sessionId) {
  const data = await apiFetch(`/api/openstack/endpoints/${encodeURIComponent(String(sessionId))}/projects`)
  return data?.items || []
}

export async function createOpenStackNetwork(payload) {
  return apiFetch('/api/openstack/networks/create', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function testOpenstackEndpoint(payload) {
  return apiFetch('/api/openstack/endpoints/test', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function connectOpenstackEndpoint(payload) {
  return apiFetch('/api/openstack/endpoints/connect', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function fetchOpenstackEndpointSession(sessionId) {
  const data = await apiFetch(`/api/openstack/endpoints/${encodeURIComponent(String(sessionId))}`)
  return data?.openstack_endpoint_session || null
}

export async function closeOpenstackEndpointSession(sessionId) {
  return apiFetch('/api/openstack/endpoints/close', {
    method: 'POST',
    body: JSON.stringify({ openstack_endpoint_session_id: sessionId }),
  })
}

export async function triggerOpenStackProvision({
  var_overrides = {},
  openstack_endpoint_session_id = null,
} = {}) {
  return apiFetch('/api/openstack/provision', {
    method: 'POST',
    body: JSON.stringify({ var_overrides, openstack_endpoint_session_id }),
  })
}

export async function fetchOpenStackProvisionStatus() {
  return apiFetch('/api/openstack/provision/status')
}

// --- ROUTER MANAGEMENT ---

export async function fetchOpenStackRouters(openstackEndpointSessionId = null, params = {}) {
  const data = await apiFetch(withSessionQuery('/api/openstack/routers', openstackEndpointSessionId, params))
  return data?.items || []
}

export async function createOpenStackRouter(payload) {
  return apiFetch('/api/openstack/routers/create', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function attachSubnetToRouter(payload) {
  return apiFetch('/api/openstack/routers/attach-subnet', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}
