# VM Migration Orchestration Platform

## Project Overview
This platform orchestrates VM migration from VMware (Workstation/ESXi discovery source) to OpenStack with a staged, state-machine-driven workflow:

1. Discover VMware VMs (read-only)
2. Select VMs and create migration jobs
3. Convert VMDK to QCOW2 (`virt-v2v`)
4. Upload image to OpenStack Glance
5. Boot instance in Nova
6. Verify ACTIVE state
7. Roll back safely on failure

No authentication is enabled in this version by design.

## Architecture Diagram (Textual)

```text
[React Frontend]
   |
   | HTTP (REST)
   v
[Django API - core/migrations app]
   |
   | enqueue tasks
   v
[Celery Worker] <----> [Redis Broker/Result Backend]
   |
   | local conversion
   +--> [virt-v2v] --> QCOW2 artifacts (filesystem)
   |
   | cloud deployment
   +--> [OpenStack APIs via openstacksdk]
         |- Glance (image upload)
         |- Nova (server boot)
         |- Neutron (network selection)
```

## Technology Stack
- Backend: Django, Django REST Framework
- Async: Celery + Redis
- Virtualization: `virt-v2v`, `qemu-img`, `pyVmomi`
- Cloud: OpenStack via `openstacksdk`
- Frontend: React + Vite
- Database: MariaDB/PostgreSQL (SQLite for local development)

## Supported Migration Workflow
State machine:
- `PENDING -> DISCOVERED -> CONVERTING -> UPLOADING -> DEPLOYED -> VERIFIED`
- Failure path: `* -> FAILED -> ROLLED_BACK`

Execution semantics:
- Discovery is read-only.
- Conversion runs in Celery and is feature-flagged.
- OpenStack deployment is feature-flagged.
- Rollback is automatic on pipeline errors (when enabled).

## Safety Guarantees
- Idempotent job creation from VM selection (active jobs are not duplicated).
- Idempotent conversion/deployment checks (reuses existing artifacts/resources when possible).
- Rollback is idempotent:
  - file/dir deletion tolerates missing targets
  - OpenStack resource cleanup tolerates already-deleted resources
- Structured logging for API + worker paths.
- Feature flags default to safe values (`false` for conversion/deployment).

## Installation Steps

### 1) Backend
```bash
cd /home/amin/vm-migrator-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # if you maintain one
# OR install project deps as currently used
pip install Django djangorestframework celery redis django-environ dj-database-url \
  pyvmomi openstacksdk mysqlclient psycopg2-binary
```

Create env file:
```bash
cp .env.example .env
```

Run migrations:
```bash
python manage.py migrate
```

### 2) Frontend
```bash
cd /home/amin/Desktop/vm-migrator/frontend
cp .env.example .env
# set VITE_API_BASE_URL=http://<BACKEND_HOST>:8000
npm install
```

## Configuration

### Backend `.env` highlights
```env
DEBUG=false
ALLOWED_HOSTS=your-domain.example.com,127.0.0.1,localhost

ENABLE_REAL_CONVERSION=false
ENABLE_OPENSTACK_DEPLOYMENT=false
ENABLE_ROLLBACK=true

MIGRATION_OUTPUT_DIR=/var/lib/vm-migrator/images
VIRT_V2V_TIMEOUT_SECONDS=7200

CELERY_WORKER_CONCURRENCY=2
CELERY_WORKER_PREFETCH_MULTIPLIER=1
CELERY_TASK_SOFT_TIME_LIMIT=3600
CELERY_TASK_TIME_LIMIT=3900
```

### OpenStack credentials (`clouds.yaml`)
Expected at:
- `~/.config/openstack/clouds.yaml`

Required cloud entry name:
- `openstack` (or override with `OPENSTACK_CLOUD_NAME`)

### VMware source variables
```env
VMWARE_WORKSTATION_PATHS=~/vmware,~/Virtual Machines
VMWARE_ESXI_HOST=
VMWARE_ESXI_USERNAME=
VMWARE_ESXI_PASSWORD=
```

## Backend Hardening Notes
Implemented:
- `DEBUG=false` default
- explicit `ALLOWED_HOSTS` env parsing
- JSON structured logging (`core/logging.py`)
- separate app and worker log streams/files
  - `logs/app.log`
  - `logs/worker.log`
- Celery safe defaults:
  - `acks_late=true`
  - `prefetch_multiplier=1`
  - bounded worker concurrency
  - startup broker retry + publish retry policy
- Timeout controls:
  - `VIRT_V2V_TIMEOUT_SECONDS`
  - OpenStack verify/upload timeout+poll settings
- Retry controls:
  - OpenStack API retry count/delay settings

## Frontend Hardening Notes
Implemented:
- Backend URL from environment (`VITE_API_BASE_URL`)
- production build flow (`npm run build`)
- operational note for reverse proxy deployment

### Nginx reverse proxy example
```nginx
server {
  listen 80;
  server_name vm-migrator.example.com;

  root /var/www/vm-migrator-frontend/dist;
  index index.html;

  location / {
    try_files $uri /index.html;
  }

  location /api/ {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
```

## How to Run

### 1) Start backend API
```bash
cd /home/amin/vm-migrator-backend
source .venv/bin/activate
python manage.py runserver 0.0.0.0:8000
```

### 2) Start Celery worker
```bash
cd /home/amin/vm-migrator-backend
source .venv/bin/activate
celery -A core worker -l info --concurrency=${CELERY_WORKER_CONCURRENCY:-2}
```

### 3) Start frontend
```bash
cd /home/amin/Desktop/vm-migrator/frontend
npm run dev -- --host
```

## Demo Scenario (Step-by-step)

### 1. Show VMware discovery
- Open VMware Inventory page.
- Explain: "This list is read-only discovery data from Workstation/ESXi."

### 2. Select VMs
- Check one or more VMs in the table.
- Explain source labels and resource columns.

### 3. Trigger migration
- Click **Migrate selected VMs**.
- Explain idempotency: active jobs are skipped, not duplicated.

### 4. Observe conversion
- Open Migration Jobs dashboard and detail view.
- Explain state transitions to `CONVERTING` and conversion metadata (`command`, paths, logs).

### 5. Observe OpenStack deployment
- With `ENABLE_OPENSTACK_DEPLOYMENT=true`, watch transition through:
  - `UPLOADING -> DEPLOYED -> VERIFIED`
- Explain stored `image_id` and `server_id` in job metadata.

### 6. Simulate failure
- Use an invalid disk path or disable output path permissions.
- Explain expected transition to `FAILED`.

### 7. Observe rollback
- Show automatic rollback task execution.
- Explain cleanup evidence:
  - local artifact deletion actions
  - OpenStack server/image cleanup actions
  - final state `ROLLED_BACK`

## Jury / Talking Points
- "All risky operations are feature-flagged and default-safe."
- "Every stage is state-machine controlled and explicitly observable."
- "Failures trigger deterministic rollback with audit metadata."
- "Logs are structured JSON and separated for API and worker paths."
- "The pipeline is idempotent at job creation, conversion, deployment, and rollback."

## Known Limitations
- ESXi conversion execution path is still placeholder (planning/discovery available).
- No authentication/authorization layer yet.
- Multi-disk advanced conversion strategy can be expanded.
- No distributed lock layer yet for multi-worker strict serialization per VM.

## Future Improvements
- Add auth (OIDC/JWT) and RBAC.
- Add per-tenant quotas and policy controls.
- Add Prometheus metrics + Grafana dashboards.
- Add distributed locks and stronger exactly-once semantics.
- Add OpenStack volume-based boot path and richer network selection policy.

## Final Checklist
- [ ] Backend API starts with `DEBUG=false` and expected `ALLOWED_HOSTS`
- [ ] Celery worker starts with configured concurrency/prefetch
- [ ] Structured logs are written to `logs/app.log` and `logs/worker.log`
- [ ] VMware discovery endpoint returns data
- [ ] Migration creation endpoint creates/skips idempotently
- [ ] Conversion runs (or dry-runs) according to `ENABLE_REAL_CONVERSION`
- [ ] OpenStack is reachable (`/api/openstack/health`)
- [ ] Deployment path works when `ENABLE_OPENSTACK_DEPLOYMENT=true`
- [ ] Rollback auto-triggers on failure and reaches `ROLLED_BACK`
- [ ] Frontend builds and loads with env-based backend URL
- [ ] Demo flow is reproducible end-to-end


## Just checking git commits