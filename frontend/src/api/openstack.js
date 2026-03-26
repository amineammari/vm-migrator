import { apiFetch } from './client'

function withSessionQuery(path, openstackEndpointSessionId = null) {
  if (!openstackEndpointSessionId) return path
  const encoded = encodeURIComponent(String(openstackEndpointSessionId))
  return `${path}?openstack_endpoint_session_id=${encoded}`
}

export async function fetchOpenStackHealth(openstackEndpointSessionId = null) {
  return apiFetch(withSessionQuery('/api/openstack/health', openstackEndpointSessionId))
}

export async function fetchOpenStackFlavors(openstackEndpointSessionId = null) {
  const data = await apiFetch(withSessionQuery('/api/openstack/flavors', openstackEndpointSessionId))
  return data?.items || []
}

export async function fetchOpenStackNetworks(openstackEndpointSessionId = null) {
  const data = await apiFetch(withSessionQuery('/api/openstack/networks', openstackEndpointSessionId))
  return data?.items || []
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
