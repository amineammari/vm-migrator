import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Alert, Button, Field } from '../components/ui'
import { useAuth } from '../contexts/useAuth'

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

        <Field label="Username">
          <input
            value={form.username}
            onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
            required
          />
        </Field>

        <Field label="Password">
          <input
            type="password"
            value={form.password}
            onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
            required
          />
        </Field>

        {error ? <Alert>{error}</Alert> : null}

        <Button className="auth-submit" disabled={submitting} type="submit">
          {submitting ? 'Signing in...' : 'Login'}
        </Button>
      </form>
    </div>
  )
}

export default LoginPage
