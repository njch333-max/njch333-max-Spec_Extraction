from __future__ import annotations

import argparse
import os
import posixpath
import stat
import sys
import urllib.request
from pathlib import Path

try:
    import paramiko
except ImportError as exc:  # pragma: no cover - local tooling guard
    raise SystemExit("paramiko is required for tools/deploy_online.py. Install it in the local environment first.") from exc


DEFAULT_INCLUDE_PATHS = (
    "App",
    "requirements.txt",
    "PRD.md",
    "Arch.md",
    "Project_state.md",
    "AGENTS.md",
)

SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "__pycache__",
    "data",
    "tmp",
    "tmp_job6",
}

SKIP_SUFFIXES = {".pyc", ".pyo"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy Spec_Extraction to the LXtransport production server.")
    parser.add_argument("--host", default=os.environ.get("SPEC_EXTRACTION_DEPLOY_HOST", "43.160.209.86"))
    parser.add_argument("--user", default=os.environ.get("SPEC_EXTRACTION_DEPLOY_USER", "ubuntu"))
    parser.add_argument("--password-env", default="SPEC_EXTRACTION_DEPLOY_PASSWORD")
    parser.add_argument("--remote-root", default="/opt/spec-extraction")
    parser.add_argument("--health-url", default="https://spec.lxtransport.online/api/health")
    parser.add_argument("--staging-dir", default="")
    parser.add_argument("--no-restart", action="store_true")
    parser.add_argument(
        "--include",
        nargs="+",
        default=list(DEFAULT_INCLUDE_PATHS),
        help="Repo-relative files or directories to deploy.",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def collect_files(root: Path, include_paths: list[str]) -> list[Path]:
    collected: list[Path] = []
    for include_path in include_paths:
        target = root / include_path
        if not target.exists():
            raise SystemExit(f"Include path does not exist: {target}")
        if target.is_file():
            collected.append(target)
            continue
        for path in target.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            parts = set(relative.parts)
            if parts & SKIP_DIR_NAMES:
                continue
            if path.suffix.lower() in SKIP_SUFFIXES:
                continue
            collected.append(path)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in collected:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return sorted(unique, key=lambda item: str(item.relative_to(root)).lower())


def remote_mkdirs(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts = remote_dir.strip("/").split("/")
    current = ""
    for part in parts:
        current = f"{current}/{part}" if current else f"/{part}"
        try:
            sftp.stat(current)
        except OSError:
            sftp.mkdir(current)


def upload_to_staging(
    sftp: paramiko.SFTPClient,
    root: Path,
    files: list[Path],
    staging_dir: str,
) -> list[tuple[str, str, int]]:
    staged: list[tuple[str, str, int]] = []
    for local_path in files:
        relative = local_path.relative_to(root).as_posix()
        remote_stage = posixpath.join(staging_dir, relative)
        remote_mkdirs(sftp, posixpath.dirname(remote_stage))
        sftp.put(str(local_path), remote_stage)
        mode = 0o755 if local_path.suffix.lower() == ".sh" else 0o644
        staged.append((remote_stage, posixpath.join("/opt/spec-extraction", relative), mode))
    return staged


def run_remote(client: paramiko.SSHClient, command: str, password: str, require_sudo: bool = False) -> tuple[int, str, str]:
    full_command = f"sudo {command}" if require_sudo else command
    stdin, stdout, stderr = client.exec_command(full_command, get_pty=require_sudo)
    if require_sudo:
        stdin.write(password + "\n")
        stdin.flush()
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    return exit_code, out, err


def install_staged_files(client: paramiko.SSHClient, staged: list[tuple[str, str, int]], password: str) -> None:
    for stage_path, remote_path, mode in staged:
        remote_dir = posixpath.dirname(remote_path)
        exit_code, out, err = run_remote(client, f"mkdir -p {remote_dir}", password, require_sudo=True)
        if exit_code != 0:
            raise RuntimeError(f"Failed to create remote directory {remote_dir}: {out}{err}")
        exit_code, out, err = run_remote(
            client,
            f"install -o ubuntu -g ubuntu -m {mode:o} {stage_path} {remote_path}",
            password,
            require_sudo=True,
        )
        if exit_code != 0:
            raise RuntimeError(f"Failed to install {remote_path}: {out}{err}")


def check_health(health_url: str) -> None:
    with urllib.request.urlopen(health_url, timeout=30) as response:
        body = response.read().decode("utf-8", errors="replace").strip()
        if response.status != 200 or '"status":"ok"' not in body.replace(" ", ""):
            raise RuntimeError(f"Unexpected health response from {health_url}: {response.status} {body}")


def main() -> int:
    args = parse_args()
    password = os.environ.get(args.password_env, "")
    if not password:
        raise SystemExit(f"Missing deploy password in environment variable {args.password_env}.")

    root = repo_root()
    staging_dir = args.staging_dir or f"/home/{args.user}/spec-extraction-staging"
    files = collect_files(root, list(args.include))
    print(f"Deploying {len(files)} file(s) from {root} to {args.user}@{args.host}:{args.remote_root}")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=args.host, username=args.user, password=password, timeout=30)
    try:
        exit_code, out, err = run_remote(client, f"rm -rf {staging_dir} && mkdir -p {staging_dir}", password, require_sudo=False)
        if exit_code != 0:
            raise RuntimeError(f"Failed to prepare staging dir: {out}{err}")
        sftp = client.open_sftp()
        try:
            staged = upload_to_staging(sftp, root, files, staging_dir)
        finally:
            sftp.close()
        install_staged_files(client, staged, password)
        if not args.no_restart:
            for service in ("spec-extraction-web.service", "spec-extraction-worker.service"):
                exit_code, out, err = run_remote(client, f"systemctl restart {service}", password, require_sudo=True)
                if exit_code != 0:
                    raise RuntimeError(f"Failed to restart {service}: {out}{err}")
            for service in ("spec-extraction-web.service", "spec-extraction-worker.service"):
                exit_code, out, err = run_remote(client, f"systemctl is-active {service}", password, require_sudo=True)
                if exit_code != 0 or out.strip() != "active":
                    raise RuntimeError(f"{service} is not active after restart: {out}{err}")
        check_health(args.health_url)
        print("Deployment completed and health check passed.")
        return 0
    finally:
        try:
            run_remote(client, f"rm -rf {staging_dir}", password, require_sudo=False)
        except Exception:
            pass
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
