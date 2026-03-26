import { apiFetch } from './client'

export function fetchDashboard(params = {}) {
  const query = new URLSearchParams()
  if (params.userId) query.set('user_id', String(params.userId))
  const suffix = query.toString() ? `?${query.toString()}` : ''
  return apiFetch(`/api/dashboard${suffix}`)
}
