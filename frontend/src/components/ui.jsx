export function Button({ children, variant = 'primary', className = '', as = 'button', ...props }) {
  const classMap = {
    primary: 'primary-btn',
    secondary: 'secondary-btn',
    danger: 'danger-btn',
    ghost: 'ghost-btn',
  }
  const Element = as
  return (
    <Element className={`${classMap[variant] || classMap.primary} ${className}`.trim()} {...props}>
      {children}
    </Element>
  )
}

export function Badge({ children, tone = 'info', className = '', ...props }) {
  return (
    <span className={`badge badge-${tone} ${className}`.trim()} {...props}>
      {children}
    </span>
  )
}

export function Table({ children, className = '', pagination = null, ...props }) {
  const {
    page = 1,
    pageSize = 10,
    totalItems = 0,
    onPageChange,
    onPageSizeChange,
    pageSizeOptions = [10, 20, 50],
    label = 'items',
  } = pagination || {}
  const totalPages = Math.max(1, Math.ceil(totalItems / Math.max(1, pageSize)))
  const from = totalItems === 0 ? 0 : (page - 1) * pageSize + 1
  const to = totalItems === 0 ? 0 : Math.min(page * pageSize, totalItems)

  return (
    <>
      <div className="table-wrap">
        <table className={`data-table ${className}`.trim()} {...props}>
          {children}
        </table>
      </div>
      {pagination ? (
        <div className="table-pagination" role="navigation" aria-label="Table pagination">
          <p>
            Showing {from}-{to} of {totalItems} {label}
          </p>
          <div className="table-pagination-controls">
            {onPageSizeChange ? (
              <label className="table-page-size">
                <span>Rows</span>
                <select value={pageSize} onChange={(event) => onPageSizeChange(Number(event.target.value))}>
                  {pageSizeOptions.map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
            <Button
              variant="secondary"
              className="slim-btn"
              type="button"
              onClick={() => onPageChange?.(Math.max(1, page - 1))}
              disabled={page <= 1}
            >
              Prev
            </Button>
            <span className="table-page-indicator">Page {page} / {totalPages}</span>
            <Button
              variant="secondary"
              className="slim-btn"
              type="button"
              onClick={() => onPageChange?.(Math.min(totalPages, page + 1))}
              disabled={page >= totalPages}
            >
              Next
            </Button>
          </div>
        </div>
      ) : null}
    </>
  )
}

export function PageHeader({ eyebrow, title, description, actions }) {
  const descriptionNode =
    typeof description === 'string' ? <p>{description}</p> : <div className="page-header-description">{description}</div>
  return (
    <div className="page-header">
      <div>
        {eyebrow ? <p className="eyebrow">{eyebrow}</p> : null}
        <h2>{title}</h2>
        {description ? descriptionNode : null}
      </div>
      {actions ? <div className="header-actions">{actions}</div> : null}
    </div>
  )
}

export function Alert({ tone = 'error', children, className = '', role }) {
  const alertRole = role || (tone === 'error' ? 'alert' : 'status')
  return (
    <div className={`alert ${tone} ${className}`.trim()} role={alertRole}>
      {children}
    </div>
  )
}

export function Card({ as = 'section', className = '', children, ...props }) {
  const Element = as
  return (
    <Element className={`panel ${className}`.trim()} {...props}>
      {children}
    </Element>
  )
}

export function Field({ label, hint, children, className = '' }) {
  return (
    <label className={`field ${className}`.trim()}>
      <span>{label}</span>
      {children}
      {hint ? <small>{hint}</small> : null}
    </label>
  )
}

export function Toggle({ label, checked, onChange, disabled }) {
  return (
    <label className="toggle">
      <input type="checkbox" checked={checked} onChange={onChange} disabled={disabled} />
      <span aria-hidden="true" />
      <strong>{label}</strong>
    </label>
  )
}

export function Skeleton({ rows = 3 }) {
  return (
    <div className="skeleton-stack" aria-label="Loading" aria-live="polite">
      {Array.from({ length: rows }).map((_, index) => (
        <span key={index} className="skeleton-line" />
      ))}
    </div>
  )
}

export function EmptyState({ title, message, action }) {
  return (
    <div className="panel-state">
      <h3>{title}</h3>
      {message ? <p>{message}</p> : null}
      {action ? <div className="empty-state-action">{action}</div> : null}
    </div>
  )
}

export function PageLoader({ label = 'Loading page' }) {
  return (
    <div className="panel page-loader" role="status" aria-live="polite">
      <strong>{label}</strong>
      <Skeleton rows={3} />
    </div>
  )
}
