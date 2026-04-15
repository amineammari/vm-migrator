import { useCallback, useEffect, useState } from 'react'
import { createUser, deleteUser, fetchUsers } from '../api/users'
import { Alert, Button, Card, Field, PageHeader, Table } from '../components/ui'

function UsersPage() {
  const [users, setUsers] = useState([])
  const [form, setForm] = useState({ username: '', email: '', password: '', role: 'USER' })
  const [error, setError] = useState('')
  const [tablePage, setTablePage] = useState(1)
  const [tablePageSize, setTablePageSize] = useState(10)

  const loadUsers = useCallback(async function loadUsers() {
    try {
      const data = await fetchUsers()
      setUsers(data)
      setError('')
    } catch (err) {
      setError(err.message || 'Unable to load users.')
    }
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    loadUsers()
  }, [loadUsers])

  useEffect(() => {
    setTablePage(1)
  }, [users.length])

  const tableTotalPages = Math.max(1, Math.ceil(users.length / Math.max(1, tablePageSize)))
  const pagedUsers = users.slice((tablePage - 1) * tablePageSize, tablePage * tablePageSize)

  useEffect(() => {
    if (tablePage > tableTotalPages) {
      setTablePage(tableTotalPages)
    }
  }, [tablePage, tableTotalPages])

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
      <PageHeader
        eyebrow="Administration"
        title="User Management"
        description="Create and manage platform users as a super admin."
      />

      {error ? <Alert>{error}</Alert> : null}

      <Card className="migration-form-panel">
        <h3>Create user</h3>
        <form className="inline-form" onSubmit={handleCreate}>
          <Field label="Username">
            <input
              placeholder="j.smith"
              value={form.username}
              onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
              required
            />
          </Field>
          <Field label="Email">
            <input
              placeholder="j.smith@example.com"
              type="email"
              value={form.email}
              onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))}
              required
            />
          </Field>
          <Field label="Password">
            <input
              placeholder="Temporary password"
              type="password"
              value={form.password}
              onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
              required
            />
          </Field>
          <Field label="Role">
            <select
              value={form.role}
              onChange={(event) => setForm((current) => ({ ...current, role: event.target.value }))}
            >
              <option value="USER">USER</option>
              <option value="SUPER_ADMIN">SUPER_ADMIN</option>
            </select>
          </Field>
          <Button type="submit">
            Create
          </Button>
        </form>
      </Card>

      <Card>
        <Table
          pagination={{
            page: tablePage,
            pageSize: tablePageSize,
            totalItems: users.length,
            onPageChange: setTablePage,
            onPageSizeChange: (nextPageSize) => {
              setTablePageSize(nextPageSize)
              setTablePage(1)
            },
            label: 'users',
          }}
        >
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
              {pagedUsers.map((item) => (
                <tr key={item.id}>
                  <td>{item.username}</td>
                  <td>{item.email}</td>
                  <td>{item.role}</td>
                  <td>{new Date(item.created_at).toLocaleString()}</td>
                  <td>
                    <Button variant="danger" onClick={() => handleDelete(item.id)}>
                      Delete
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
        </Table>
      </Card>
    </section>
  )
}

export default UsersPage
