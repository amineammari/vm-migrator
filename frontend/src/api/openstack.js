import { apiFetch } from './client'

export async function fetchOpenStackHealth() {
  return apiFetch('/api/openstack/health')
}

export async function fetchOpenStackFlavors() {
  const data = await apiFetch('/api/openstack/flavors')
  return data?.items || []
}

export async function fetchOpenStackNetworks() {
  const data = await apiFetch('/api/openstack/networks')
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

export async function triggerOpenStackProvision(var_overrides = {}) {
  return apiFetch('/api/openstack/provision', {
    method: 'POST',
    body: JSON.stringify({ var_overrides }),
  })
}

export async function fetchOpenStackProvisionStatus() {
  return apiFetch('/api/openstack/provision/status')
}
