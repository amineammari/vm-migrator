import { apiFetch } from './client'

export function fetchUsers() {
  return apiFetch('/api/users/')
}

export function createUser(payload) {
  return apiFetch('/api/users/', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function updateUser(id, payload) {
  return apiFetch(`/api/users/${id}/`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export function deleteUser(id) {
  return apiFetch(`/api/users/${id}/`, {
    method: 'DELETE',
  })
}
