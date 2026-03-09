const STATUS_CLASS = {
  IDLE: 'status-idle',
  QUEUED: 'status-queued',
  RUNNING: 'status-running',
  SUCCESS: 'status-success',
  SKIPPED: 'status-skipped',
  PENDING: 'status-pending',
  DISCOVERED: 'status-discovered',
  PRECHECK: 'status-discovered',
  SNAPSHOT_CREATED: 'status-discovered',
  DISK_ANALYZING: 'status-converting',
  CONVERTING: 'status-converting',
  BLOCK_VALIDATING: 'status-uploading',
  UPLOADING: 'status-uploading',
  DEPLOYED: 'status-deployed',
  VERIFIED: 'status-verified',
  FAILED: 'status-failed',
  ROLLED_BACK: 'status-rolled-back',
}

function StatusBadge({ status }) {
  if (!status) return <span className="status-badge">UNKNOWN</span>
  const className = STATUS_CLASS[status] || ''
  return <span className={`status-badge ${className}`}>{status}</span>
}

export default StatusBadge
