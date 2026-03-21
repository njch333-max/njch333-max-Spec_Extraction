from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any

from fastapi import HTTPException, Request

from App.services import store
from App.services.runtime import ADMIN_PASSWORD, ADMIN_PASSWORD_HASH, ADMIN_USERNAME


def make_password_hash(password: str, salt: str | None = None, iterations: int = 120_000) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    if not encoded.startswith("pbkdf2_sha256$"):
        return hmac.compare_digest(password, encoded)
    _algo, iter_text, salt, digest = encoded.split("$", 3)
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), int(iter_text)).hex()
    return hmac.compare_digest(candidate, digest)


def expected_password_hash() -> str:
    if ADMIN_PASSWORD_HASH:
        return ADMIN_PASSWORD_HASH
    return make_password_hash(ADMIN_PASSWORD, salt="spec-extraction-default-salt")


def authenticate(username: str, password: str) -> bool:
    if username != ADMIN_USERNAME:
        return False
    return verify_password(password, expected_password_hash())


def ensure_csrf_token(session: dict[str, Any]) -> str:
    token = session.get("csrf_token")
    if token:
        return str(token)
    token = secrets.token_urlsafe(24)
    session["csrf_token"] = token
    return token


def verify_csrf(request: Request, token: str | None) -> None:
    expected = request.session.get("csrf_token", "")
    if not expected or not token or not hmac.compare_digest(str(expected), str(token)):
        raise HTTPException(status_code=400, detail="Invalid CSRF token.")


def login_user(request: Request, username: str) -> None:
    request.session["user"] = username
    store.insert_auth_event(username=username, action="login", detail=request.client.host if request.client else "")


def logout_user(request: Request) -> None:
    username = str(request.session.get("user", ""))
    request.session.clear()
    if username:
        store.insert_auth_event(username=username, action="logout")


def current_user(request: Request) -> str | None:
    value = request.session.get("user")
    return str(value) if value else None


def require_user(request: Request) -> str:
    user = current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return user
