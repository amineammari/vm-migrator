import { apiFetch } from './client'

export async function fetchVMwareVMs() {
  const data = await apiFetch('/api/vmware/vms')
  return data?.items || []
}

export async function triggerMigrations(vms) {
  return apiFetch('/api/migrations/from-vmware', {
    method: 'POST',
    body: JSON.stringify({ vms }),
  })
}
