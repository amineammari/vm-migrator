import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import { PageLoader } from './components/ui'
import { useAuth } from './contexts/useAuth'

const DashboardPage = lazy(() => import('./pages/DashboardPage'))
const DesignSystemPage = lazy(() => import('./pages/DesignSystemPage'))
const InfraManagementPage = lazy(() => import('./pages/InfraManagementPage'))
const JobDetailPage = lazy(() => import('./pages/JobDetailPage'))
const LoginPage = lazy(() => import('./pages/LoginPage'))
const LogsPage = lazy(() => import('./pages/LogsPage'))
const MigrationJobsPage = lazy(() => import('./pages/MigrationJobsPage'))
const SettingsPage = lazy(() => import('./pages/SettingsPage'))
const UsersPage = lazy(() => import('./pages/UsersPage'))
const VMwareInventoryPage = lazy(() => import('./pages/VMwareInventoryPage'))

function App() {
  const { isAuthenticated } = useAuth()

  return (
    <Suspense fallback={<PageLoader />}>
      <Routes>
        <Route path="/login" element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <LoginPage />} />
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <Layout />
            </ProtectedRoute>
          }
        >
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="infrastructure" element={<InfraManagementPage />} />
          <Route path="migrations" element={<Navigate to="/inventory" replace />} />
          <Route path="logs" element={<LogsPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="design-system" element={<DesignSystemPage />} />
          <Route
            path="migrations/:id"
            element={
              <ProtectedRoute>
                <JobDetailPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="inventory"
            element={
              <ProtectedRoute>
                <VMwareInventoryPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="migration-jobs"
            element={
              <ProtectedRoute>
                <MigrationJobsPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="users"
            element={
              <ProtectedRoute roles={['SUPER_ADMIN']}>
                <UsersPage />
              </ProtectedRoute>
            }
          />
        </Route>
        <Route path="*" element={<Navigate to={isAuthenticated ? '/dashboard' : '/login'} replace />} />
      </Routes>
    </Suspense>
  )
}

export default App
