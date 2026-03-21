from __future__ import annotations

import getpass

from App.services.auth import make_password_hash


def main() -> None:
    password = getpass.getpass("Password: ")
    confirm = getpass.getpass("Confirm: ")
    if password != confirm:
        raise SystemExit("Passwords do not match.")
    print(make_password_hash(password))


if __name__ == "__main__":
    main()
