from datetime import timedelta
from pathlib import Path

import dj_database_url
import environ

BASE_DIR = Path(__file__).resolve().parent.parent

env = environ.Env(
    DEBUG=(bool, False),
)
environ.Env.read_env(BASE_DIR / ".env")

SECRET_KEY = env("SECRET_KEY", default="unsafe-dev-key-change-me")
DEBUG = env("DEBUG", default=False)

# In production, set explicit hosts (comma-separated). Defaults are local-safe.
ALLOWED_HOSTS = [
    h.strip()
    for h in env("ALLOWED_HOSTS", default="127.0.0.1,localhost").split(",")
    if h.strip()
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "migrations",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "core.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "core.wsgi.application"
ASGI_APPLICATION = "core.asgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        env("DATABASE_URL", default=f"sqlite:///{BASE_DIR / 'db.sqlite3'}"),
        conn_max_age=env.int("DB_CONN_MAX_AGE", default=600),
    )
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = env("TIME_ZONE", default="UTC")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.SessionAuthentication",
        "rest_framework.authentication.BasicAuthentication",
    ],
}

# Celery core settings
CELERY_BROKER_URL = env("REDIS_URL", default="redis://127.0.0.1:6379/0")
CELERY_RESULT_BACKEND = env("REDIS_URL", default="redis://127.0.0.1:6379/0")
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE

# Celery reliability and safe worker defaults
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_TASK_ACKS_LATE = True
CELERY_TASK_REJECT_ON_WORKER_LOST = True
CELERY_WORKER_PREFETCH_MULTIPLIER = env.int("CELERY_WORKER_PREFETCH_MULTIPLIER", default=1)
CELERY_WORKER_CONCURRENCY = env.int("CELERY_WORKER_CONCURRENCY", default=2)
CELERY_TASK_SOFT_TIME_LIMIT = env.int("CELERY_TASK_SOFT_TIME_LIMIT", default=3600)
CELERY_TASK_TIME_LIMIT = env.int("CELERY_TASK_TIME_LIMIT", default=3900)
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_DEFAULT_RETRY_DELAY = env.int("CELERY_TASK_DEFAULT_RETRY_DELAY", default=30)
CELERY_TASK_PUBLISH_RETRY = True
CELERY_TASK_PUBLISH_RETRY_POLICY = {
    "max_retries": env.int("CELERY_PUBLISH_MAX_RETRIES", default=3),
    "interval_start": 0,
    "interval_step": 0.5,
    "interval_max": 3,
}

# Periodic discovery (Celery beat)
ENABLE_PERIODIC_DISCOVERY = env.bool("ENABLE_PERIODIC_DISCOVERY", default=False)
DISCOVERY_INTERVAL_SECONDS = env.int("DISCOVERY_INTERVAL_SECONDS", default=300)
DISCOVERY_INCLUDE_WORKSTATION = env.bool("DISCOVERY_INCLUDE_WORKSTATION", default=True)
DISCOVERY_INCLUDE_ESXI = env.bool("DISCOVERY_INCLUDE_ESXI", default=True)

if ENABLE_PERIODIC_DISCOVERY:
    CELERY_BEAT_SCHEDULE = {
        "discover-vmware-vms": {
            "task": "migrations.discover_vmware_vms",
            "schedule": timedelta(seconds=DISCOVERY_INTERVAL_SECONDS),
            "args": (DISCOVERY_INCLUDE_WORKSTATION, DISCOVERY_INCLUDE_ESXI),
        }
    }

# Conversion execution controls
ENABLE_REAL_CONVERSION = env.bool("ENABLE_REAL_CONVERSION", default=False)
MIGRATION_OUTPUT_DIR = env("MIGRATION_OUTPUT_DIR", default="/var/lib/vm-migrator/images")
VIRT_V2V_TIMEOUT_SECONDS = env.int("VIRT_V2V_TIMEOUT_SECONDS", default=7200)

ENABLE_ROLLBACK = env.bool("ENABLE_ROLLBACK", default=True)

# Minimal artifact backup (stores a copy of QCOW2 before OpenStack upload).
ENABLE_ARTIFACT_BACKUP = env.bool("ENABLE_ARTIFACT_BACKUP", default=False)
ARTIFACT_BACKUP_DIR = env("ARTIFACT_BACKUP_DIR", default=str(Path(MIGRATION_OUTPUT_DIR) / "backups"))
ARTIFACT_BACKUP_REQUIRED = env.bool("ARTIFACT_BACKUP_REQUIRED", default=False)

# ESXi conversion guardrails
VMWARE_REQUIRE_NO_SNAPSHOTS = env.bool("VMWARE_REQUIRE_NO_SNAPSHOTS", default=True)

# OpenStack deployment controls
ENABLE_OPENSTACK_DEPLOYMENT = env.bool("ENABLE_OPENSTACK_DEPLOYMENT", default=False)
OPENSTACK_CLOUD_NAME = env("OPENSTACK_CLOUD_NAME", default="openstack")
OPENSTACK_DEFAULT_NETWORK = env("OPENSTACK_DEFAULT_NETWORK", default="")
# Optional. When DevStack publishes a broken /image proxy endpoint, point this to Glance directly
# (eg http://192.168.72.169:60999 after exposing it on 0.0.0.0 on the OpenStack node).
OPENSTACK_IMAGE_ENDPOINT_OVERRIDE = env("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", default="")
OPENSTACK_VERIFY_TIMEOUT = env.int("OPENSTACK_VERIFY_TIMEOUT", default=900)
OPENSTACK_VERIFY_POLL_INTERVAL = env.int("OPENSTACK_VERIFY_POLL_INTERVAL", default=10)
OPENSTACK_IMAGE_UPLOAD_TIMEOUT = env.int("OPENSTACK_IMAGE_UPLOAD_TIMEOUT", default=900)
OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL = env.int("OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL", default=5)
OPENSTACK_API_RETRIES = env.int("OPENSTACK_API_RETRIES", default=2)
OPENSTACK_API_RETRY_DELAY = env.int("OPENSTACK_API_RETRY_DELAY", default=3)

# Logging
LOG_DIR = Path(env("LOG_DIR", default=str(BASE_DIR / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_LEVEL = env("LOG_LEVEL", default="INFO")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "worker_only": {"()": "core.logging.WorkerLogFilter"},
        "app_only": {"()": "core.logging.AppLogFilter"},
    },
    "formatters": {
        "json": {"()": "core.logging.JsonFormatter"},
    },
    "handlers": {
        "console_app": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "filters": ["app_only"],
        },
        "console_worker": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "filters": ["worker_only"],
        },
        "app_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOG_DIR / "app.log"),
            "maxBytes": env.int("APP_LOG_MAX_BYTES", default=10 * 1024 * 1024),
            "backupCount": env.int("APP_LOG_BACKUP_COUNT", default=5),
            "formatter": "json",
            "filters": ["app_only"],
        },
        "worker_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOG_DIR / "worker.log"),
            "maxBytes": env.int("WORKER_LOG_MAX_BYTES", default=20 * 1024 * 1024),
            "backupCount": env.int("WORKER_LOG_BACKUP_COUNT", default=7),
            "formatter": "json",
            "filters": ["worker_only"],
        },
    },
    "root": {
        "handlers": ["console_app", "console_worker", "app_file", "worker_file"],
        "level": LOG_LEVEL,
    },
    "loggers": {
        "django": {
            "handlers": ["console_app", "app_file"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "celery": {
            "handlers": ["console_worker", "worker_file"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "migrations.tasks": {
            "handlers": ["console_worker", "worker_file"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
}
