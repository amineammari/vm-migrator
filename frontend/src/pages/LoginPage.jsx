import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

function LoginPage() {
  const navigate = useNavigate()
  const { login } = useAuth()
  const [form, setForm] = useState({ username: '', password: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  async function handleSubmit(event) {
    event.preventDefault()
    setSubmitting(true)
    setError('')
    try {
      await login(form)
      navigate('/dashboard', { replace: true })
    } catch (err) {
      setError(err.message || 'Login failed.')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit}>
        <p className="brand-kicker">Authentication</p>
        <h2>Sign In</h2>
        <p className="auth-copy">Use your VM Migrator account to access dashboards and migrations.</p>

        <label>
          <span>Username</span>
          <input
            value={form.username}
            onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
            required
          />
        </label>

        <label>
          <span>Password</span>
          <input
            type="password"
            value={form.password}
            onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
            required
          />
        </label>

        {error ? <div className="alert error">{error}</div> : null}

        <button className="primary-btn auth-submit" disabled={submitting} type="submit">
          {submitting ? 'Signing in...' : 'Login'}
        </button>
      </form>
    </div>
  )
}

export default LoginPage
