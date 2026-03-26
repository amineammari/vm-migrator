import { Navigate } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

function ProtectedRoute({ children, roles = [] }) {
  const { isAuthenticated, isLoading, user } = useAuth()

  if (isLoading) {
    return <div className="panel-state">Loading session...</div>
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  if (roles.length > 0 && !roles.includes(user?.role)) {
    return <Navigate to="/dashboard" replace />
  }

  return children
}

export default ProtectedRoute
