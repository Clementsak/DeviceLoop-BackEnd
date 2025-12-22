#!/bin/bash
set -euo pipefail

echo "[ValidateService] Checking service status..."
sleep 2
systemctl is-active --quiet deviceloop-backend

echo "[ValidateService] Service is running."
