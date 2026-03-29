from __future__ import annotations

import json
import os
import re
import secrets
import hashlib
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
APP_DIR = BASE_DIR / "App"
ENV_PATH = BASE_DIR / ".env"
STATIC_DIR = APP_DIR / "static"
TEMPLATES_DIR = APP_DIR / "templates"


def load_env_file() -> None:
    if not ENV_PATH.exists():
        return
    try:
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file()

DEFAULT_DATA_DIR = APP_DIR / "data"
DATA_DIR = Path(os.getenv("SPEC_EXTRACTION_DATA_DIR", "") or DEFAULT_DATA_DIR)
DB_PATH = DATA_DIR / "spec_extraction.sqlite3"
TEMPLATES_ROOT = DATA_DIR / "templates"
JOBS_ROOT = DATA_DIR / "jobs"
EXPORTS_ROOT = DATA_DIR / "exports"

SECRET_KEY = os.getenv("SPEC_EXTRACTION_SECRET_KEY", "") or secrets.token_urlsafe(32)
ADMIN_USERNAME = os.getenv("SPEC_EXTRACTION_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("SPEC_EXTRACTION_ADMIN_PASSWORD", "admin")
ADMIN_PASSWORD_HASH = os.getenv("SPEC_EXTRACTION_ADMIN_PASSWORD_HASH", "")
SESSION_DOMAIN = os.getenv("SPEC_EXTRACTION_SESSION_DOMAIN", "").strip()
HOST_DOMAIN = os.getenv("SPEC_EXTRACTION_HOST_DOMAIN", "").strip()
HTTPS_ONLY = os.getenv("SPEC_EXTRACTION_HTTPS_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}
OPENAI_ENABLED = os.getenv("SPEC_EXTRACTION_ENABLE_OPENAI", "0").strip().lower() in {"1", "true", "yes", "on"}
OPENAI_MODEL = os.getenv("SPEC_EXTRACTION_OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_VISION_ENABLED = os.getenv("SPEC_EXTRACTION_ENABLE_OPENAI_VISION", "1").strip().lower() in {"1", "true", "yes", "on"}
OPENAI_VISION_MAX_PAGES = int(os.getenv("SPEC_EXTRACTION_OPENAI_VISION_MAX_PAGES", "24"))
OPENAI_VISION_DPI = int(os.getenv("SPEC_EXTRACTION_OPENAI_VISION_DPI", "144"))
WEB_PORT = int(os.getenv("SPEC_EXTRACTION_WEB_PORT", "8010"))
MAX_UPLOAD_MB = int(os.getenv("SPEC_EXTRACTION_MAX_UPLOAD_MB", "50"))
WORKER_LEASE_TTL_SECONDS = int(os.getenv("SPEC_EXTRACTION_WORKER_LEASE_TTL_SECONDS", "240"))


def _detect_git_short_head() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(BASE_DIR),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _source_tree_fingerprint() -> str:
    digest = hashlib.sha1()
    for path in sorted(APP_DIR.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".py", ".html", ".css"}:
            continue
        relative = path.relative_to(BASE_DIR).as_posix()
        stat = path.stat()
        digest.update(relative.encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
    return digest.hexdigest()[:8]


APP_BUILD_ID = os.getenv("SPEC_EXTRACTION_APP_BUILD_ID", "").strip() or (
    f"{_detect_git_short_head() or 'local'}-{_source_tree_fingerprint()}"
)


def ensure_runtime_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATES_ROOT.mkdir(parents=True, exist_ok=True)
    JOBS_ROOT.mkdir(parents=True, exist_ok=True)
    EXPORTS_ROOT.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def utc_after_seconds_iso(seconds: int) -> str:
    return (utc_now() + timedelta(seconds=seconds)).replace(microsecond=0).isoformat()


def slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "builder"


def safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "_", (name or "").strip())
    cleaned = cleaned.replace(" ", "_")
    return cleaned or "file"


def ensure_builder_dir(builder_slug: str) -> Path:
    path = TEMPLATES_ROOT / slugify(builder_slug)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_job_dirs(job_no: str) -> dict[str, Path]:
    job_root = JOBS_ROOT / job_no
    spec_dir = job_root / "spec"
    drawing_dir = job_root / "drawings"
    export_dir = job_root / "exports"
    for path in (job_root, spec_dir, drawing_dir, export_dir):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "job_root": job_root,
        "spec_dir": spec_dir,
        "drawing_dir": drawing_dir,
        "export_dir": export_dir,
    }


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent), prefix=path.stem, suffix=".tmp") as handle:
        temp_path = Path(handle.name)
        handle.write(content)
    temp_path.replace(path)


def write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("wb", delete=False, dir=str(path.parent), prefix=path.stem, suffix=".tmp") as handle:
        temp_path = Path(handle.name)
        handle.write(content)
    temp_path.replace(path)

ensure_runtime_dirs()
