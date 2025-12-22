#!/bin/bash
set -euo pipefail

APP_DIR="/opt/deviceloop-backend"
TMP_DIR="/tmp/deviceloop-backend"
VENV_DIR="${APP_DIR}/venv"

echo "[AfterInstall] Installing operating system dependencies..."
apt-get update -y
apt-get install -y python3-venv python3-pip

echo "[AfterInstall] Creating virtual environment..."
python3 -m venv "${VENV_DIR}"

echo "[AfterInstall] Installing Python dependencies..."
"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install -r "${APP_DIR}/requirements.txt"

echo "[AfterInstall] Restoring instance .env (if it exists)..."
if [ -f "${TMP_DIR}/.env" ]; then
  cp "${TMP_DIR}/.env" "${APP_DIR}/.env"
fi

echo "[AfterInstall] Setting permissions..."
chown -R ubuntu:ubuntu "${APP_DIR}" || true
chmod -R u+rwX,go-rwx "${APP_DIR}" || true
