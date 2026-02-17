import { apiFetch } from './client'

export async function fetchVMwareVMs({ endpointSessionId } = {}) {
  const qs =
    typeof endpointSessionId === 'number'
      ? `?endpoint_session_id=${encodeURIComponent(String(endpointSessionId))}`
      : ''
  const data = await apiFetch(`/api/vmware/vms${qs}`)
  return data?.items || []
}

export async function discoverVMwareNow({
  include_workstation = false,
  include_esxi = true,
  vmware_endpoint_session_id = null,
} = {}) {
  return apiFetch('/api/vmware/discover-now', {
    method: 'POST',
    body: JSON.stringify({ include_workstation, include_esxi, vmware_endpoint_session_id }),
  })
}

export async function fetchTaskStatus(taskId) {
  return apiFetch(`/api/tasks/${taskId}`)
}

export async function triggerMigrations({
  vms,
  vmware_endpoint_session_id,
  openstack_endpoint_session_id,
}) {
  return apiFetch('/api/migrations/from-vmware', {
    method: 'POST',
    body: JSON.stringify({ vms, vmware_endpoint_session_id, openstack_endpoint_session_id }),
  })
}

export async function testVMwareEndpoint(payload) {
  return apiFetch('/api/vmware/endpoints/test', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function connectVMwareEndpoint(payload) {
  return apiFetch('/api/vmware/endpoints/connect', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}
