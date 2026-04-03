import { clearAuthStorage, getAccessToken, getRefreshToken, setAuthStorage } from '../auth/storage'

const BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')

export async function apiFetch(path, options = {}) {
  return request(path, options, true)
}

async function request(path, options, canRetry) {
  const url = `${BASE_URL}${path}`
  const accessToken = getAccessToken()
  const headers = {
    'Content-Type': 'application/json',
    ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
    ...(options.headers || {}),
  }

  const response = await fetch(url, { ...options, headers })
  const raw = await response.text()
  const data = raw ? safeParseJson(raw) : null

  if (response.status === 401 && canRetry && getRefreshToken()) {
    const refreshed = await tryRefresh()
    if (refreshed) {
      return request(path, options, false)
    }
  }

  if (!response.ok) {
    const error = new Error(extractErrorMessage(data) || `Request failed with status ${response.status}`)
    error.status = response.status
    error.payload = data
    throw error
  }

  return data
}

async function tryRefresh() {
  const refresh = getRefreshToken()
  if (!refresh) return false

  const response = await fetch(`${BASE_URL}/api/auth/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refresh }),
  })

  const raw = await response.text()
  const data = raw ? safeParseJson(raw) : null
  if (!response.ok || !data?.access) {
    clearAuthStorage()
    return false
  }

  setAuthStorage({
    access: data.access,
    refresh,
    user: data.user || null,
  })
  return true
}

function safeParseJson(raw) {
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

function extractErrorMessage(payload) {
  if (!payload) return null
  if (typeof payload === 'string') return payload
  if (payload.error) return payload.error
  if (payload.detail) return payload.detail
  if (payload.message) return payload.message
  if (Array.isArray(payload)) {
    const items = payload
      .map((item) => extractErrorMessage(item))
      .filter(Boolean)
    return items.length ? items.join(' ') : null
  }
  if (typeof payload === 'object') {
    const parts = Object.entries(payload)
      .flatMap(([key, value]) => {
        const message = extractErrorMessage(value)
        if (!message) return []
        if (/^\d+$/.test(key)) return [message]
        return [`${key}: ${message}`]
      })
      .filter(Boolean)
    return parts.length ? parts.join(' ') : null
  }
  return null
}
