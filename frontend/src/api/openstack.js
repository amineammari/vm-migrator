import { apiFetch } from './client'

export async function fetchOpenStackHealth() {
  return apiFetch('/api/openstack/health')
}
