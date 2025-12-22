#!/bin/bash
set -euo pipefail

echo "[ApplicationStart] Reloading systemd and starting service..."
systemctl daemon-reload
systemctl restart deviceloop-backend
