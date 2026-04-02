#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/backend/logs"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"

BACKEND_PID="$RUN_DIR/backend.pid"
CELERY_WORKER_PID="$RUN_DIR/celery-worker.pid"
CELERY_BEAT_PID="$RUN_DIR/celery-beat.pid"
FRONTEND_PID="$RUN_DIR/frontend.pid"

BACKEND_LOG="$LOG_DIR/backend.out"
CELERY_WORKER_LOG="$LOG_DIR/celery-worker.out"
CELERY_BEAT_LOG="$LOG_DIR/celery-beat.out"
FRONTEND_LOG="$FRONTEND_DIR/dev.out"

mkdir -p "$RUN_DIR" "$LOG_DIR"

activate_venv() {
  if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.venv/bin/activate"
    return 0
  fi
  if [[ -f "$BACKEND_DIR/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$BACKEND_DIR/.venv/bin/activate"
    return 0
  fi
  return 1
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  local file="$1"
  if [[ -f "$file" ]]; then
    cat "$file"
  fi
}

write_pid() {
  local pid="$1"
  local file="$2"
  echo "$pid" > "$file"
}

remove_pid() {
  local file="$1"
  rm -f "$file"
}

start_service() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  local work_dir="$4"
  local command="$5"

  local existing_pid
  existing_pid=$(read_pid "$pid_file")
  if is_pid_running "$existing_pid"; then
    echo "$name already running (pid $existing_pid)"
    return 0
  fi

  (cd "$work_dir" && nohup bash -c "$command" > "$log_file" 2>&1 & echo $! > "$pid_file")

  local new_pid
  new_pid=$(read_pid "$pid_file")
  if is_pid_running "$new_pid"; then
    echo "Started $name (pid $new_pid)"
    return 0
  fi

  echo "Failed to start $name"
  return 1
}

stop_service() {
  local name="$1"
  local pid_file="$2"

  local existing_pid
  existing_pid=$(read_pid "$pid_file")
  if is_pid_running "$existing_pid"; then
    kill "$existing_pid" 2>/dev/null || true
    sleep 1
    if is_pid_running "$existing_pid"; then
      kill -9 "$existing_pid" 2>/dev/null || true
    fi
    remove_pid "$pid_file"
    echo "Stopped $name"
    return 0
  fi

  remove_pid "$pid_file"
  echo "$name not running"
}

redis_listening() {
  ss -lnt 2>/dev/null | grep -q ":6379"
}

start_redis_if_needed() {
  if redis_listening; then
    echo "Redis is already listening on 6379"
    return 0
  fi

  if command -v redis-server >/dev/null 2>&1; then
    redis-server --daemonize yes
    sleep 1
    if redis_listening; then
      echo "Started redis (port 6379)"
      return 0
    fi
  fi

  echo "Redis is not listening on 6379"
  return 1
}

run_migrations() {
  echo "Applying backend migrations"
  (cd "$BACKEND_DIR" && python manage.py migrate)
}

status_line() {
  local label="$1"
  local pid_file="$2"
  local extra="$3"

  local existing_pid
  existing_pid=$(read_pid "$pid_file")
  if is_pid_running "$existing_pid"; then
    printf "%-14s running (pid %s)%s\n" "$label" "$existing_pid" "$extra"
  else
    printf "%-14s stopped%s\n" "$label" "$extra"
  fi
}

status_all() {
  status_line "backend" "$BACKEND_PID" ""
  status_line "celery worker" "$CELERY_WORKER_PID" ""
  status_line "celery beat" "$CELERY_BEAT_PID" ""
  status_line "frontend" "$FRONTEND_PID" ""

  if redis_listening; then
    echo "redis          running (port 6379)"
  else
    echo "redis          stopped"
  fi
}

start_all() {
  activate_venv || echo "Warning: virtualenv not found; using system python"

  start_redis_if_needed
  run_migrations

  start_service "backend" "$BACKEND_PID" "$BACKEND_LOG" "$BACKEND_DIR" "python manage.py runserver 0.0.0.0:8000"
  start_service "celery worker" "$CELERY_WORKER_PID" "$CELERY_WORKER_LOG" "$BACKEND_DIR" "celery -A core worker -l info --concurrency=\${CELERY_WORKER_CONCURRENCY:-2}"
  start_service "celery beat" "$CELERY_BEAT_PID" "$CELERY_BEAT_LOG" "$BACKEND_DIR" "celery -A core beat -l INFO"

  if command -v npm >/dev/null 2>&1; then
    start_service "frontend" "$FRONTEND_PID" "$FRONTEND_LOG" "$FRONTEND_DIR" "npm run dev -- --host"
  else
    echo "npm not found; frontend not started"
  fi

  echo ""
  status_all
}

stop_all() {
  stop_service "frontend" "$FRONTEND_PID"
  stop_service "celery beat" "$CELERY_BEAT_PID"
  stop_service "celery worker" "$CELERY_WORKER_PID"
  stop_service "backend" "$BACKEND_PID"
}

usage() {
  echo "Usage: $0 {start|stop|restart|status}"
}

case "${1:-}" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    start_all
    ;;
  status)
    status_all
    ;;
  *)
    usage
    exit 1
    ;;
esac
