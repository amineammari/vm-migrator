import { Navigate } from 'react-router-dom'
import { PageLoader } from './ui'
import { useAuth } from '../contexts/useAuth'

function ProtectedRoute({ children, roles = [] }) {
  const { isAuthenticated, isLoading, user } = useAuth()

  if (isLoading) {
    return <PageLoader label="Loading session" />
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
