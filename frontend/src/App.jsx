import { Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/Layout'
import JobDetailPage from './pages/JobDetailPage'
import MigrationJobsPage from './pages/MigrationJobsPage'
import VMwareInventoryPage from './pages/VMwareInventoryPage'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/vmware" replace />} />
        <Route path="/vmware" element={<VMwareInventoryPage />} />
        <Route path="/migrations" element={<MigrationJobsPage />} />
        <Route path="/migrations/:id" element={<JobDetailPage />} />
      </Routes>
    </Layout>
  )
}

export default App
