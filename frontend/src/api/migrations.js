import { apiFetch } from './client'

export async function fetchMigrationJobs(params = {}) {
  const query = new URLSearchParams()
  if (params.userId) query.set('user_id', String(params.userId))
  if (params.username) query.set('username', params.username)
  if (params.ordering) query.set('ordering', params.ordering)
  const suffix = query.toString() ? `?${query.toString()}` : ''
  return apiFetch(`/api/migrations${suffix}`)
}

export function createMigration(payload) {
  return apiFetch('/api/migrations', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function fetchMigrationJob(id) {
  return apiFetch(`/api/migrations/${id}`)
}
