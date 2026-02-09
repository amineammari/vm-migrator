import { apiFetch } from './client'

export async function fetchMigrationJobs() {
  const data = await apiFetch('/api/migrations')
  if (Array.isArray(data)) return data
  return data?.items || []
}

export async function fetchMigrationJob(id) {
  return apiFetch(`/api/migrations/${id}`)
}
