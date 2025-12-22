#!/usr/bin/env bash
set -euo pipefail

# Confirm service is running
systemctl is-active --quiet deviceloop-backend

# Show last logs (useful in CodeDeploy console output)
journalctl -u deviceloop-backend --no-pager -n 50
