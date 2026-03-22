#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Please run as root: sudo bash install_systemd.sh"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install -m 0644 "${SCRIPT_DIR}/spec-extraction-web.service" /etc/systemd/system/spec-extraction-web.service
install -m 0644 "${SCRIPT_DIR}/spec-extraction-worker.service" /etc/systemd/system/spec-extraction-worker.service
systemctl daemon-reload
systemctl enable spec-extraction-web.service
systemctl enable spec-extraction-worker.service
echo "Installed systemd unit files for Spec_Extraction."
echo "Edit /etc/spec-extraction.env if your deploy path, domain, or secrets differ from the defaults."
