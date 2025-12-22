#!/usr/bin/env bash
set -euo pipefail

# Make sure scripts are executable (helps if Git did not preserve permissions)
chmod -R +x /opt/codedeploy-agent/deployment-root/*/deployment-archive/scripts 2>/dev/null || true

# Stop service if it exists
systemctl stop deviceloop-backend || true

# Ensure destination exists
mkdir -p /opt/deviceloop-backend
chown -R ubuntu:ubuntu /opt/deviceloop-backend || true
