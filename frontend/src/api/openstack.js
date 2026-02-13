import { apiFetch } from './client'

export async function fetchOpenStackHealth() {
  return apiFetch('/api/openstack/health')
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
