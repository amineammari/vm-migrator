# VMigrate

VMigrate is a Django and Celery platform for migrating VMware virtual machines to OpenStack (DevStack-friendly).

It provides:
- Endpoint onboarding for VMware and OpenStack
- VM discovery and migration job orchestration
- Conversion planning and execution with virt-v2v or qemu-img pipelines
- Disk and filesystem validation before deployment
- Optional OpenStack deployment as boot-from-volume instances
- Rollback support for failed migration runs

For architecture-focused details, see [docs/architecture.md](docs/architecture.md).

## Project Overview

### What VMigrate solves

Migrating VMs from VMware to OpenStack is operationally complex. Teams usually need to manually combine discovery, conversion, image upload, volume handling, server boot, networking fixes, and post-checks.

VMigrate centralizes these steps into a controlled workflow with auditable states and API-driven operations.

### Key capabilities

- Discover VMs from VMware Workstation/Fusion-style paths and ESXi endpoints
- Create and track migration jobs through explicit states
- Convert source disks to OpenStack-compatible formats
- Preserve multi-disk ordering and boot-disk selection
- Upload artifacts to Glance and create Cinder volumes
- Boot Nova instances from volume and attach remaining disks
- Apply Linux guest network remediation when needed
- Detect guest OS profile and handle Linux/Windows flows safely

## Architecture

VMigrate has four main runtime layers:

- Frontend (React + Vite)
- Backend API (Django REST)
- Async execution (Celery + Redis)
- Infrastructure/tooling integrations (VMware, OpenStack, conversion tools)

~~~mermaid
flowchart LR
  U[Operator] --> FE[React Frontend]
  FE --> API[Django REST API]
  API --> DB[(Database)]
  API --> R[(Redis)]
  R --> W[Celery Worker]

  W --> VMW[VMware Sources]
  W --> TOOLS[virt-v2v qemu-img libguestfs]
  W --> OS[OpenStack APIs]

  API --> TF[Terraform Optional]
  W --> ANS[Ansible Optional]
~~~

## Features

### VM migration workflow

- Precheck and source validation
- Optional snapshot creation for ESXi source VMs
- Disk analysis and conversion execution
- Block and filesystem validation
- Optional OpenStack deployment and verification
- Rollback on failure

### Networking handling

- OpenStack network and floating IP selection/validation
- Optional baseline access security group management
- Linux guest first-boot remediation service injection via virt-customize

### OS support

Current behavior in code:
- Linux family support with distro detection (including Ubuntu, Debian, CentOS, RHEL, Rocky, AlmaLinux, Fedora, SUSE, Arch, and generic Linux fallback)
- Windows family detection and safe handling
- Unknown OS fallback with optional strict failure mode

Important: remediation script injection is Linux-oriented. Windows guests skip Linux remediation paths by design.

## How It Works

High-level migration lifecycle:

~~~mermaid
stateDiagram-v2
  [*] --> PENDING
  PENDING --> DISCOVERED
  DISCOVERED --> PRECHECK
  PRECHECK --> SNAPSHOT_CREATED
  SNAPSHOT_CREATED --> DISK_ANALYZING
  DISK_ANALYZING --> CONVERTING
  CONVERTING --> BLOCK_VALIDATING
  BLOCK_VALIDATING --> UPLOADING
  UPLOADING --> DEPLOYED
  DEPLOYED --> VERIFIED

  PENDING --> FAILED
  DISCOVERED --> FAILED
  PRECHECK --> FAILED
  SNAPSHOT_CREATED --> FAILED
  DISK_ANALYZING --> FAILED
  CONVERTING --> FAILED
  BLOCK_VALIDATING --> FAILED
  UPLOADING --> FAILED
  DEPLOYED --> FAILED
  FAILED --> ROLLED_BACK
~~~

Step-by-step execution:

1. User submits migration from discovered VMware VM(s).
2. Backend creates MigrationJob records and enqueues Celery tasks.
3. Worker runs precheck, conversion planning, and optional snapshot.
4. Worker executes conversion and artifact validation.
5. Worker detects guest OS and applies OS-aware behavior.
6. If OpenStack deployment is enabled, worker uploads image(s), creates volume(s), boots server, attaches disks, and validates.
7. Job transitions to VERIFIED or FAILED, with rollback if enabled.

## OpenStack Integration

VMigrate uses openstacksdk and integrates with the following services:

- Keystone: authentication and endpoint/session validation
- Glance: image upload and lifecycle
- Cinder: volume creation from image and attachment checks
- Nova: server creation and state verification
- Neutron: network selection, floating IP orchestration, and security group baseline logic

Current deployment model is boot-from-volume (not direct ephemeral boot from image).

## Installation and Setup

### Prerequisites

Minimum runtime prerequisites:
- Linux host with Python 3.12+ and Node.js (frontend)
- Redis for Celery broker/result backend
- VMware access (Workstation paths and/or ESXi endpoint)
- OpenStack credentials (clouds.yaml and/or OS_* variables)

Conversion host tools typically required:
- virt-v2v
- qemu-img
- libguestfs toolchain (virt-inspector, virt-filesystems, guestfish, virt-customize)

### 1) Backend environment

Use your project virtual environment and create backend environment file.

~~~bash
cd backend
cp .env.example .env
~~~

Set at least:
- SECRET_KEY
- DATABASE_URL
- REDIS_URL
- ENABLE_REAL_CONVERSION
- ENABLE_OPENSTACK_DEPLOYMENT
- MIGRATION_OUTPUT_DIR

### 2) Database migration

~~~bash
cd backend
../.venv/bin/python manage.py migrate
~~~

### 3) Start backend API

~~~bash
cd backend
../.venv/bin/python manage.py runserver 0.0.0.0:8000
~~~

### 4) Start Celery worker

~~~bash
cd backend
../.venv/bin/celery -A core worker -l info --concurrency=${CELERY_WORKER_CONCURRENCY:-2}
~~~

Optional periodic discovery scheduler:

~~~bash
cd backend
../.venv/bin/celery -A core beat -l INFO
~~~

### 5) Start frontend

~~~bash
cd frontend
npm install
npm run dev -- --host
~~~

### 6) Optional helper script

You can use the process supervisor script for local orchestration:

~~~bash
bash scripts/dev-stack.sh start
bash scripts/dev-stack.sh status
bash scripts/dev-stack.sh stop
~~~

## Usage

Typical operator flow in UI:

1. Connect VMware endpoint.
2. Connect OpenStack endpoint.
3. Run discovery.
4. Select VM(s) and target overrides.
5. Submit migration.
6. Monitor job states in dashboard/job views.

Useful API endpoints (base path: /api):

- Authentication and users
  - POST /api/auth/register
  - POST /api/auth/login
  - POST /api/auth/refresh
  - GET /api/auth/me
  - GET /api/users/
  - GET /api/users/{id}/

- Health and dashboard
  - GET /api/health
  - GET /api/dashboard
  - GET /api/openstack/health

- VMware
  - GET /api/vmware/vms
  - POST /api/vmware/discover-now
  - POST /api/vmware/endpoints/test
  - POST /api/vmware/endpoints/connect
  - GET /api/vmware/endpoints/{id}
  - POST /api/vmware/endpoints/close

- OpenStack
  - GET /api/openstack/images
  - GET /api/openstack/flavors
  - GET /api/openstack/networks
  - POST /api/openstack/networks/create
  - POST /api/openstack/endpoints/test
  - POST /api/openstack/endpoints/connect
  - GET /api/openstack/endpoints/{id}
  - POST /api/openstack/endpoints/close
  - POST /api/openstack/provision
  - GET /api/openstack/provision/status

- Migrations and tasks
  - GET /api/migrations
  - GET /api/migrations/{job_id}
  - POST /api/migrations/from-vmware
  - POST /api/migrations/{job_id}/start
  - POST /api/migrations/{job_id}/rollback
  - GET /api/tasks/{task_id}

## Project Structure

~~~text
vm-migrator/
├── README.md
├── docs/
│   └── architecture.md
├── scripts/
│   └── dev-stack.sh
├── backend/
│   ├── manage.py
│   ├── core/
│   │   ├── settings.py
│   │   ├── urls.py
│   │   └── celery.py
│   ├── migrations/
│   │   ├── views.py
│   │   ├── urls.py
│   │   ├── tasks.py
│   │   ├── models.py
│   │   ├── vmware_client.py
│   │   ├── openstack_client.py
│   │   ├── openstack_deployment.py
│   │   ├── conversion.py
│   │   ├── disk_formats.py
│   │   ├── disk_inspection.py
│   │   ├── block_validation.py
│   │   ├── filesystem_check.py
│   │   ├── network_remediation.py
│   │   ├── os_profile.py
│   │   ├── snapshot_manager.py
│   │   ├── terraform_runner.py
│   │   └── ansible_runner.py
│   └── users/
│       ├── views.py
│       ├── urls.py
│       └── serializers.py
├── frontend/
│   ├── package.json
│   └── src/
├── ansible/
│   ├── inventory/
│   └── playbooks/
└── terraform/
    ├── provider.tf
    ├── network.tf
    ├── security.tf
    └── modules/
~~~

## Configuration

Main runtime configuration is in:
- backend/.env (from backend/.env.example)
- backend/core/settings.py

Key environment variables:

- Core and runtime
  - DEBUG
  - SECRET_KEY
  - ALLOWED_HOSTS
  - DATABASE_URL
  - REDIS_URL

- Celery and discovery
  - CELERY_WORKER_CONCURRENCY
  - ENABLE_PERIODIC_DISCOVERY
  - DISCOVERY_INTERVAL_SECONDS
  - DISCOVERY_INCLUDE_WORKSTATION
  - DISCOVERY_INCLUDE_ESXI

- Conversion and artifacts
  - ENABLE_REAL_CONVERSION
  - MIGRATION_OUTPUT_DIR
  - VIRT_V2V_TIMEOUT_SECONDS
  - ENABLE_ARTIFACT_BACKUP
  - ARTIFACT_BACKUP_DIR
  - ENABLE_ROLLBACK

- OS-aware behavior
  - ENABLE_GUEST_NETWORK_REMEDIATION
  - GUEST_NETWORK_REMEDIATION_TIMEOUT_SECONDS
  - GUEST_NETWORK_DISABLE_CLOUD_INIT_NETWORK_CONFIG
  - MIGRATION_FAIL_ON_UNSUPPORTED_OS

- VMware
  - VMWARE_WORKSTATION_PATHS
  - VMWARE_ESXI_HOST
  - VMWARE_ESXI_PORT
  - VMWARE_ESXI_USERNAME
  - VMWARE_ESXI_PASSWORD
  - VMWARE_ESXI_CONVERSION_TRANSPORT
  - VMWARE_VDDK_LIBDIR
  - VMWARE_VDDK_THUMBPRINT

- OpenStack
  - ENABLE_OPENSTACK_DEPLOYMENT
  - OPENSTACK_CLOUD_NAME
  - OPENSTACK_DEFAULT_NETWORK
  - OPENSTACK_DEFAULT_EXTERNAL_NETWORK
  - OPENSTACK_IMAGE_ENDPOINT_OVERRIDE
  - OPENSTACK_VERIFY_TIMEOUT
  - OPENSTACK_IMAGE_UPLOAD_TIMEOUT
  - OPENSTACK_API_RETRIES
  - OPENSTACK_API_RETRY_DELAY
  - OS_AUTH_URL
  - OS_USERNAME
  - OS_PASSWORD
  - OS_PROJECT_NAME
  - OS_USER_DOMAIN_NAME
  - OS_PROJECT_DOMAIN_NAME
  - OS_REGION_NAME
  - OS_INTERFACE
  - OS_VERIFY

## Limitations

Current codebase limitations:

- Source platforms are VMware-focused (Workstation/Fusion-style and ESXi paths)
- Conversion quality depends on external toolchain availability and host permissions
- Linux guest network remediation is not intended for Windows guests
- OpenStack deployment is optional and disabled by default (must be enabled explicitly)
- Some environments may need manual tuning for VDDK, libguestfs, and OpenStack endpoint behavior

## Future Improvements

Planned and recommended enhancements:

- Expand OS coverage matrix and distro-specific post-migration checks
- Add Windows-specific post-migration networking/agent remediation options
- Improve performance for large multi-disk migrations (parallelism, caching, resumability)
- Add richer observability (metrics, tracing, SLA dashboards)
- Add integration/e2e test suites against reproducible DevStack labs
- Improve API documentation and publish OpenAPI schema examples

## Security Notes

- Treat endpoint credentials as sensitive and rotate regularly.
- Keep TLS verification enabled outside lab/dev environments.
- Restrict API exposure and protect JWT secrets and database credentials.

## Additional Documentation

- Architecture details: [docs/architecture.md](docs/architecture.md)
- Security and hardening notes: [SECURITY_REMEDIATION.md](SECURITY_REMEDIATION.md)
