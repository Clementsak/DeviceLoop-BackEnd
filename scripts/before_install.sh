#!/bin/bash
set -euo pipefail

APP_DIR="/opt/deviceloop-backend"
TMP_DIR="/tmp/deviceloop-backend"
ENV_FILE="${APP_DIR}/.env"

echo "[BeforeInstall] Stopping service if it exists..."
systemctl stop deviceloop-backend || true

echo "[BeforeInstall] Preserving instance .env (if present)..."
mkdir -p "${TMP_DIR}"
if [ -f "${ENV_FILE}" ]; then
  cp "${ENV_FILE}" "${TMP_DIR}/.env"
fi

echo "[BeforeInstall] Ensuring application directory exists..."
mkdir -p "${APP_DIR}"
