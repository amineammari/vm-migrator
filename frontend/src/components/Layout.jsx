import { NavLink } from 'react-router-dom'

function Layout({ children }) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <p className="brand-kicker">Platform</p>
          <h1 className="brand-title">VM Migration Control</h1>
        </div>

        <nav className="nav-links">
          <NavLink to="/vmware" className={({ isActive }) => navClass(isActive)}>
            VMware Inventory
          </NavLink>
          <NavLink to="/migrations" className={({ isActive }) => navClass(isActive)}>
            Migration Jobs
          </NavLink>
        </nav>
      </aside>

      <main className="content">{children}</main>
    </div>
  )
}

function navClass(isActive) {
  return `nav-link ${isActive ? 'active' : ''}`
}

export default Layout
