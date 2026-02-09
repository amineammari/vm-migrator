const BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')

export async function apiFetch(path, options = {}) {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  })

  const raw = await response.text()
  const data = raw ? safeParseJson(raw) : null

  if (!response.ok) {
    const error = new Error(extractErrorMessage(data) || `Request failed with status ${response.status}`)
    error.status = response.status
    error.payload = data
    throw error
  }

  return data
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
  return null
}
