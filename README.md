# VM Migrator

VM Migrator is an orchestration platform for migrating virtual machines from VMware sources (Workstation/ESXi discovery) into OpenStack, with controlled execution, observability, and rollback.

## Project Status (What Has Been Achieved)

### Implemented End-to-End Flow
1. Discover VMware VMs (read-only inventory).
2. Select VMs and create migration jobs.
3. Convert VMDK to QCOW2 (`virt-v2v` path, with dry-run/feature flag controls).
4. Upload image to OpenStack Glance.
5. Provision instance in OpenStack Nova.
6. Validate deployment and update job state.
7. Trigger rollback automatically on failure and store cleanup metadata.

### Production-Oriented Foundations Completed
- State-machine driven orchestration with explicit migration states.
- Celery-based asynchronous execution with Redis broker/result backend.
- Structured JSON logs for API and worker paths.
- Feature flags for risky operations (conversion, deployment, Terraform apply).
- Idempotency protections for job creation and cleanup flows.
- Optional infrastructure and conversion execution layers via Terraform and Ansible.

## Architectural Documentation

### High-Level Architecture
```text
[React Frontend (Vite)]
         |
         | REST (/api/*)
         v
[Django API (DRF)] ----> [DB: SQLite/PostgreSQL/MariaDB]
         |
         | enqueue async jobs
         v
[Celery Worker] <----> [Redis]
    |        \
    |         +--> [OpenStack via openstacksdk: Keystone/Nova/Glance/Neutron]
    |
    +--> [Conversion layer: virt-v2v/qemu-img]
            (local subprocess OR Ansible runner)

Optional:
[Django/Celery] --> [Terraform layer] --> [OpenStack baseline network/security resources]
```

### Component Responsibilities
- Frontend (`frontend/`):
  - VMware inventory and endpoint management UI.
  - Migration creation and status views.
  - OpenStack endpoint test/connect and provisioning status screens.
- Backend API (`backend/`):
  - Orchestration endpoints, validation, state transitions, metadata persistence.
  - Integration clients for VMware and OpenStack.
- Worker (`backend` Celery app):
  - Long-running conversion/deployment/rollback operations.
- Redis:
  - Task queue and async result backend.
- OpenStack:
  - Image, compute, and networking resource operations.
- Optional Ansible/Terraform:
  - Externalized execution for conversion/infrastructure tasks.

### Migration State Machine
```text
PENDING -> DISCOVERED -> CONVERTING -> UPLOADING -> DEPLOYED -> VERIFIED
                     \-> FAILED -> ROLLED_BACK
```

State transitions are persisted and observable through API + logs.

## Technical Documentation

### Technology Stack
- Backend: Django, Django REST Framework
- Async: Celery
- Queue/Result backend: Redis
- VMware integration: `pyVmomi`
- OpenStack integration: `openstacksdk`
- Conversion tooling: `virt-v2v`, `qemu-img`
- Frontend: React + Vite
- Optional infra tooling: Terraform
- Optional remote conversion: Ansible

### Core Backend Domains
- `migrations/` app:
  - job lifecycle and orchestration
  - OpenStack deployment and rollback logic
  - VMware discovery and endpoint session handling
- `core/`:
  - environment-driven settings
  - logging and runtime configuration

### Implemented API Surface
Base path: `/api`

- Health & task status:
  - `GET /health`
  - `GET /openstack/health`
  - `GET /tasks/<task_id>`
- VMware:
  - `GET /vmware/vms`
  - `POST /vmware/discover-now`
  - `POST /vmware/endpoints/test`
  - `POST /vmware/endpoints/connect`
- OpenStack:
  - `GET /openstack/images`
  - `GET /openstack/flavors`
  - `GET /openstack/networks`
  - `POST /openstack/endpoints/test`
  - `POST /openstack/endpoints/connect`
- Migration jobs:
  - `GET /migrations`
  - `GET /migrations/<job_id>`
  - `POST /migrations/from-vmware`
  - `POST /migrations/<job_id>/start`
  - `POST /migrations/<job_id>/rollback`
- Terraform integration:
  - `POST /openstack/provision`
  - `GET /openstack/provision/status`

### Configuration Model
- Runtime behavior is controlled primarily through backend `.env` flags.
- OpenStack credentials are consumed from `~/.config/openstack/clouds.yaml` (default cloud name: `openstack`, override with `OPENSTACK_CLOUD_NAME`).
- Sensitive operations are disabled by default and must be explicitly enabled.

Key flags in current implementation:
- `ENABLE_REAL_CONVERSION`
- `ENABLE_OPENSTACK_DEPLOYMENT`
- `ENABLE_ROLLBACK`
- `ENABLE_ANSIBLE_CONVERSION`
- `ENABLE_TERRAFORM_INFRA`
- `ENABLE_TERRAFORM_FROM_CELERY`

### Reliability and Safety Characteristics Achieved
- Idempotent active-job handling during migration creation.
- Defensive cleanup/rollback that tolerates already-missing resources.
- Timeouts and retry knobs for long-running and cloud operations.
- Structured logs split by API and worker for easier correlation.

## Repository Structure
```text
backend/
  core/
  migrations/
  logs/
frontend/
ansible/
terraform/
```

## Local Runbook (Current)

### Backend
```bash
cd /home/amin/Desktop/vm-migrator/backend
python3 -m venv .venv
source .venv/bin/activate
pip install Django djangorestframework celery redis django-environ dj-database-url \
  pyvmomi openstacksdk mysqlclient psycopg2-binary
cp .env.example .env
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

### Worker
```bash
cd /home/amin/Desktop/vm-migrator/backend
source .venv/bin/activate
celery -A core worker -l info --concurrency=${CELERY_WORKER_CONCURRENCY:-2}
```

### Frontend
```bash
cd /home/amin/Desktop/vm-migrator/frontend
cp .env.example .env
# Set VITE_API_BASE_URL=http://<backend-host>:8000
npm install
npm run dev -- --host
```

## Validation Checklist
- `GET /api/health` returns healthy status.
- VMware inventory endpoint returns discovered VMs.
- Migration creation works and is idempotent for active jobs.
- Worker executes conversion/deployment steps according to flags.
- `GET /api/openstack/health` confirms OpenStack reachability.
- Failure path transitions to `FAILED` and rollback reaches `ROLLED_BACK`.

## Known Gaps / Next Technical Targets
- Add authentication/authorization and RBAC.
- Expand ESXi conversion execution depth.
- Improve multi-disk and advanced networking policies.
- Add first-class metrics dashboards (Prometheus/Grafana).
- Add container/Kubernetes deployment artifacts.

## Notes
This README reflects the currently implemented architecture and technical state of the project, intended as a progress documentation baseline.
