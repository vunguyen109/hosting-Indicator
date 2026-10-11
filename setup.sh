#!/usr/bin/env bash
# ============================================================
# Hosting Indicator Engine -- Server Setup Script
# Tested on: Ubuntu 22.04 LTS, Debian 12
# Run as root: bash setup.sh
# ============================================================
set -euo pipefail

APP_DIR="/opt/indicator"
APP_USER="indicator"
SERVICE="indicator-engine"

echo "[1/7] Creating system user..."
id "${APP_USER}" &>/dev/null || useradd --system --no-create-home --shell /bin/false "${APP_USER}"

echo "[2/7] Creating directory structure..."
mkdir -p "${APP_DIR}"/{data,logs}
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"
chmod 750 "${APP_DIR}"

echo "[3/7] Installing Python 3.11..."
apt-get update -qq
apt-get install -y python3.11 python3.11-venv python3-pip --no-install-recommends

echo "[4/7] Creating virtual environment..."
python3.11 -m venv "${APP_DIR}/venv"
"${APP_DIR}/venv/bin/pip" install --upgrade pip --quiet
"${APP_DIR}/venv/bin/pip" install -r "${APP_DIR}/requirements.txt" --quiet

echo "[5/7] Setting permissions..."
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"
chmod 600 "${APP_DIR}/.env"   # Protect API keys

echo "[6/7] Installing systemd service..."
cp "${APP_DIR}/indicator-engine.service" /etc/systemd/system/
cp logrotate.conf /etc/logrotate.d/indicator-engine
systemctl daemon-reload
systemctl enable "${SERVICE}"
systemctl start "${SERVICE}"

echo "[7/7] Done! Status:"
systemctl status "${SERVICE}" --no-pager -l
