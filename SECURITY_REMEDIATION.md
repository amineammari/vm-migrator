# VM Migrator – Post-Exposure Remediation Playbook

This document summarizes the known exposures, what must be rotated, and the exact steps to clean repository history and secrets after the previously committed sensitive artifacts (`backend/backups/vm_migrator_before_auth_reset.sql`, Terraform state, PID/schedule files).

## What was exposed
- `backend/backups/vm_migrator_before_auth_reset.sql` — likely contains user accounts (usernames/emails/password hashes) and endpoint session credentials (VMware/OpenStack usernames/passwords).
- `terraform/terraform.tfstate` and `terraform/terraform.tfstate.backup` — typically contain OpenStack auth (username/password/project/auth_url), resource IDs, and potentially IPs.
- Runtime PID/schedule files (`backend.pid`, `backend/celery_beat.pid`, `backend/celery_worker.pid`, `backend/celerybeat-schedule`, root `celery_beat.pid`, `celery_worker.pid`, `backend/frontend.pid`, `frontend/dev.out`, `frontend/dev.pid`) — not secrets but should be purged from history.

Treat all credentials contained in those files as compromised.

## Rotate these immediately (outside the repo)
1) **OpenStack**: rotate the user/password (or application credential) used in Terraform/endpoint sessions; revoke any tokens derived from them.
2) **VMware**: rotate credentials used for discovery/conversion that may be in the SQL dump.
3) **Django / JWT**: set a new `SECRET_KEY`; redeploy. Invalidate refresh tokens if practical.
4) **Database**: rotate DB user password if it appears in the dump or `DATABASE_URL`.
5) **Redis**: rotate Redis auth if used and ever stored.
6) **App users**: force password reset for all users in the SQL dump (or at least admins).

## Git history cleanup (do manually)
> Coordinate with collaborators; this requires a force-push.

1. Backup repo: `git clone --mirror <repo-url> /tmp/vm-migrator-backup`
2. Rewrite history to drop sensitive paths:
```bash
git filter-repo \
  --path backend/backups/vm_migrator_before_auth_reset.sql \
  --path terraform/terraform.tfstate \
  --path terraform/terraform.tfstate.backup \
  --path backend/celery_beat.pid \
  --path backend/celery_worker.pid \
  --path backend/celerybeat-schedule \
  --path backend/frontend.pid \
  --path backend.pid \
  --path celery_beat.pid \
  --path celery_worker.pid \
  --path frontend/dev.out \
  --path frontend/dev.pid \
  --invert-paths
```
3. Prune reflogs & aggressively GC:
```bash
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```
4. Force-push rewritten history:
```bash
git push --force --all
git push --force --tags
```
5. GitHub follow-up:
   - Delete Actions caches/artifacts (Settings → Actions → Caches).
   - Require collaborators to **re-clone** (not pull) after the force-push.

## Terraform/state follow-up
- Remove any local copies of the old tfstate/tfstate.backup.
- Create a fresh state after rotating credentials; if using remote state, migrate with new backend credentials.

## Endpoint credential rotation (quick guides)
- **OpenStack**: create a new user/password (or application credential), update `.env` and any cloud config, then re-connect endpoints via the app.
- **VMware**: create a new service account or rotate the password; update endpoint sessions; verify discovery succeeds.

## Django/DB/Redis rotation
- Set a new `SECRET_KEY` in `backend/.env` (long, random).
- Rotate `DATABASE_URL` password; migrate grants.
- Rotate Redis auth (if used) and update `REDIS_URL`.

## User password reset guidance
- Force resets for all users in the SQL dump; at minimum reset super-admins.
- Invalidate old refresh tokens after `SECRET_KEY` change by expiring sessions or using the blacklist app if enabled.

## Post-rotation verification
- Run `git ls-files 'backend/backups/*' 'terraform/*.tfstate*' '*.pid' '*.sql' 'frontend/dev.*'` — should output nothing.
- Start API/worker with new secrets and run a smoke test: auth, discovery, migration submit.
- Confirm OpenStack token issuance with the rotated credentials.

## Quick safety checks (HEAD)
```bash
# ensure no secrets tracked now
git ls-files 'backend/backups/*' 'terraform/*.tfstate*' '*.pid' '*.sql' 'frontend/dev.*'

# search for obvious secrets in HEAD
rg -n \"password\" backend terraform ansible | head
rg -n \"SECRET_KEY\" backend
rg -n \"redis://\" backend
```

If any of the above reveal real secrets, rotate again and repeat the filter-repo steps including the newly found paths.
