function PanelState({ title, message, action }) {
  return (
    <div className="panel-state">
      <h3>{title}</h3>
      <p>{message}</p>
      {action}
    </div>
  )
}

export default PanelState
