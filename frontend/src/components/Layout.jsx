import { NavLink, Outlet } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

function Layout() {
  const { user, logout } = useAuth()
  const isSuperAdmin = user?.role === 'SUPER_ADMIN'

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <p className="brand-kicker">Platform</p>
          <h1 className="brand-title">VM Migrator</h1>
          <div className="user-summary">
            <strong>{user?.username}</strong>
            <span>{user?.role}</span>
          </div>
        </div>

        <nav className="nav-links">
          <NavLink to="/dashboard" className={({ isActive }) => navClass(isActive)}>
            Dashboard
          </NavLink>
          <NavLink to="/migrations" className={({ isActive }) => navClass(isActive)}>
            Migrations
          </NavLink>
          <NavLink to="/inventory" className={({ isActive }) => navClass(isActive)}>
            VMware / OpenStack
          </NavLink>
          <NavLink to="/migration-jobs" className={({ isActive }) => navClass(isActive)}>
            Job Dashboard
          </NavLink>
          {isSuperAdmin ? (
            <NavLink to="/users" className={({ isActive }) => navClass(isActive)}>
              Users
            </NavLink>
          ) : null}
        </nav>

        <button className="secondary-btn sidebar-logout" onClick={logout}>
          Logout
        </button>
      </aside>

      <main className="content">
        <Outlet />
      </main>
    </div>
  )
}

function navClass(isActive) {
  return `nav-link ${isActive ? 'active' : ''}`
}

export default Layout
