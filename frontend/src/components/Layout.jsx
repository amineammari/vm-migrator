import { NavLink, Outlet } from 'react-router-dom'
import { Button } from './ui'
import { useAuth } from '../contexts/useAuth'

const NAV_ITEMS = [
  { to: '/dashboard', label: 'Dashboard', icon: 'D' },
  { to: '/infrastructure', label: 'Infrastructure', icon: 'I' },
  { to: '/inventory', label: 'Migrate VMs', icon: 'V' },
  { to: '/migration-jobs', label: 'Monitoring', icon: 'J' },
  { to: '/logs', label: 'Logs', icon: 'L' },
  { to: '/settings', label: 'Network Config', icon: 'N' },
  { to: '/design-system', label: 'Design System', icon: 'S' },
]

function Layout() {
  const { user, logout } = useAuth()
  const isSuperAdmin = user?.role === 'SUPER_ADMIN'
  const initials = user?.username?.slice(0, 2)?.toUpperCase() || 'VM'

  function toggleTheme() {
    const root = document.documentElement
    const next = root.dataset.theme === 'dark' ? 'light' : 'dark'
    root.dataset.theme = next
    localStorage.setItem('vmigrate-theme', next)
  }

  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Primary navigation">
        <div className="brand-block">
          <div className="brand-mark" aria-hidden="true">V</div>
          <div>
            <p className="brand-kicker">OpenStack Migration</p>
            <h1 className="brand-title">VMigrate</h1>
          </div>
        </div>

        <nav className="nav-links">
          {NAV_ITEMS.map((item) => (
            <NavLink key={item.to} to={item.to} className={({ isActive }) => navClass(isActive)}>
              <span className="nav-icon" aria-hidden="true">{item.icon}</span>
              <span>{item.label}</span>
            </NavLink>
          ))}
          {isSuperAdmin ? (
            <NavLink to="/users" className={({ isActive }) => navClass(isActive)}>
              <span className="nav-icon" aria-hidden="true">U</span>
              <span>Users</span>
            </NavLink>
          ) : null}
        </nav>

        <div className="sidebar-footer">
          <div className="user-summary">
            <span className="avatar" aria-hidden="true">{initials}</span>
            <div>
              <strong>{user?.username}</strong>
              <span>{user?.role}</span>
            </div>
          </div>
          <Button variant="secondary" className="sidebar-logout" onClick={logout}>
            Logout
          </Button>
        </div>
      </aside>

      <div className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Migration control plane</p>
            <strong>Production cockpit</strong>
          </div>
          <div className="topbar-actions">
            <span className="system-status"><span aria-hidden="true" /> API reachable</span>
            <Button variant="ghost" type="button" onClick={toggleTheme} aria-label="Toggle dark mode">
              Theme
            </Button>
          </div>
        </header>

        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

function navClass(isActive) {
  return `nav-link ${isActive ? 'active' : ''}`
}

export default Layout
