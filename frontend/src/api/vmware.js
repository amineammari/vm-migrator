import { apiFetch } from './client'

export async function fetchVMwareVMs() {
  const data = await apiFetch('/api/vmware/vms')
  return data?.items || []
}

export async function discoverVMwareNow({
  include_workstation = false,
  include_esxi = true,
} = {}) {
  return apiFetch('/api/vmware/discover-now', {
    method: 'POST',
    body: JSON.stringify({ include_workstation, include_esxi }),
  })
}

export async function fetchTaskStatus(taskId) {
  return apiFetch(`/api/tasks/${taskId}`)
}

export async function triggerMigrations(vms) {
  return apiFetch('/api/migrations/from-vmware', {
    method: 'POST',
    body: JSON.stringify({ vms }),
  })
}
