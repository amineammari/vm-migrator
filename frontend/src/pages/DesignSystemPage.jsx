import StatusBadge from '../components/StatusBadge'
import { Alert, Button, Card, Field, PageHeader, Skeleton, Table, Toggle } from '../components/ui'

function DesignSystemPage() {
  return (
    <section>
      <PageHeader
        eyebrow="Foundation"
        title="Design System"
        description="Reusable tokens and components for VMigrate workflows."
      />

      <div className="grid-12">
        <Card className="span-6">
          <h3>Buttons</h3>
          <div className="component-row">
            <Button>Primary</Button>
            <Button variant="secondary">Secondary</Button>
            <Button variant="danger">Danger</Button>
            <Button variant="ghost">Ghost</Button>
            <Button disabled>Disabled</Button>
          </div>
        </Card>

        <Card className="span-6">
          <h3>Inputs</h3>
          <div className="spec-fields">
            <Field label="VM name" hint="Use the discovered inventory name.">
              <input defaultValue="prod-api-03" />
            </Field>
            <Field label="Target network">
              <select defaultValue="private">
                <option value="private">private-migration</option>
                <option value="public">public</option>
              </select>
            </Field>
            <label className="checkbox-line span-2">
              <input type="checkbox" defaultChecked />
              <span>Validate block devices before upload</span>
            </label>
            <Toggle label="Dark mode compatible" checked onChange={() => {}} />
          </div>
        </Card>

        <Card className="span-4">
          <h3>Status badges</h3>
          <div className="component-row">
            <StatusBadge status="RUNNING" />
            <StatusBadge status="VERIFIED" />
            <StatusBadge status="FAILED" />
          </div>
        </Card>

        <Card className="span-4">
          <h3>Alerts</h3>
          <Alert tone="success">Network catalog refreshed.</Alert>
          <Alert tone="warning">Provisioning is still running.</Alert>
          <Alert tone="error">Disk conversion failed.</Alert>
        </Card>

        <Card className="span-4">
          <h3>Skeleton loader</h3>
          <Skeleton rows={4} />
        </Card>

        <Card className="span-12">
          <h3>Table</h3>
          <Table>
              <thead>
                <tr>
                  <th>VM</th>
                  <th>Source</th>
                  <th>Target</th>
                  <th>Status</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <div className="vm-name-cell">
                      <strong>billing-db-01</strong>
                      <span>i-7f8a-tenant-prod</span>
                    </div>
                  </td>
                  <td>ESXi cluster A</td>
                  <td>OpenStack prod</td>
                  <td><StatusBadge status="UPLOADING" /></td>
                  <td><Button variant="secondary" className="slim-btn">Open</Button></td>
                </tr>
              </tbody>
          </Table>
        </Card>
      </div>
    </section>
  )
}

export default DesignSystemPage
