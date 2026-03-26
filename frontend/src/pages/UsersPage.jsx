import { useEffect, useState } from 'react'
import { createUser, deleteUser, fetchUsers } from '../api/users'

function UsersPage() {
  const [users, setUsers] = useState([])
  const [form, setForm] = useState({ username: '', email: '', password: '', role: 'USER' })
  const [error, setError] = useState('')

  useEffect(() => {
    loadUsers()
  }, [])

  async function loadUsers() {
    try {
      const data = await fetchUsers()
      setUsers(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load users.')
    }
  }

  async function handleCreate(event) {
    event.preventDefault()
    try {
      await createUser(form)
      setForm({ username: '', email: '', password: '', role: 'USER' })
      await loadUsers()
    } catch (err) {
      setError(err.message || 'Unable to create user.')
    }
  }

  async function handleDelete(id) {
    try {
      await deleteUser(id)
      await loadUsers()
    } catch (err) {
      setError(err.message || 'Unable to delete user.')
    }
  }

  return (
    <section>
      <div className="page-header">
        <div>
          <h2>User Management</h2>
          <p>Create and manage platform users as a super admin.</p>
        </div>
      </div>

      {error ? <div className="alert error">{error}</div> : null}

      <div className="panel migration-form-panel">
        <h3>Create user</h3>
        <form className="inline-form" onSubmit={handleCreate}>
          <input
            placeholder="Username"
            value={form.username}
            onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
            required
          />
          <input
            placeholder="Email"
            type="email"
            value={form.email}
            onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))}
            required
          />
          <input
            placeholder="Password"
            type="password"
            value={form.password}
            onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
            required
          />
          <select
            value={form.role}
            onChange={(event) => setForm((current) => ({ ...current, role: event.target.value }))}
          >
            <option value="USER">USER</option>
            <option value="SUPER_ADMIN">SUPER_ADMIN</option>
          </select>
          <button className="primary-btn" type="submit">
            Create
          </button>
        </form>
      </div>

      <div className="panel">
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Username</th>
                <th>Email</th>
                <th>Role</th>
                <th>Created</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {users.map((item) => (
                <tr key={item.id}>
                  <td>{item.username}</td>
                  <td>{item.email}</td>
                  <td>{item.role}</td>
                  <td>{new Date(item.created_at).toLocaleString()}</td>
                  <td>
                    <button className="secondary-btn" onClick={() => handleDelete(item.id)}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}

export default UsersPage
