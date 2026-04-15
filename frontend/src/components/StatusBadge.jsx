import { Badge } from './ui'

const STATUS_CLASS = {
  IDLE: 'info',
  QUEUED: 'warning',
  RUNNING: 'warning',
  SUCCESS: 'success',
  SKIPPED: 'warning',
  PENDING: 'info',
  COMPLETED: 'success',
  DISCOVERED: 'info',
  PRECHECK: 'info',
  SNAPSHOT_CREATED: 'info',
  DISK_ANALYZING: 'warning',
  CONVERTING: 'warning',
  BLOCK_VALIDATING: 'warning',
  UPLOADING: 'warning',
  DEPLOYED: 'info',
  VERIFIED: 'success',
  FAILED: 'error',
  ROLLED_BACK: 'info',
}

function StatusBadge({ status }) {
  if (!status) return <Badge tone="info">UNKNOWN</Badge>
  const tone = STATUS_CLASS[status] || 'info'
  return <Badge tone={tone}>{status}</Badge>
}

export default StatusBadge
