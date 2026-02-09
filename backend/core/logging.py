from __future__ import annotations

import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in {
                "args",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            payload[key] = value

        return json.dumps(payload, default=str)


class WorkerLogFilter(logging.Filter):
    """Route Celery/async pipeline logs to worker handlers."""

    WORKER_PREFIXES = (
        "celery",
        "migrations.tasks",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith(self.WORKER_PREFIXES)


class AppLogFilter(logging.Filter):
    """Exclude worker logs from app handlers."""

    WORKER_PREFIXES = (
        "celery",
        "migrations.tasks",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(self.WORKER_PREFIXES)
