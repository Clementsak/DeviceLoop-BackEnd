#!/usr/bin/env bash
set -euo pipefail

cd /opt/deviceloop-backend

# Ensure required packages exist
apt-get update -y
apt-get install -y python3-pip python3-venv

# Create or reuse a virtual environment (recommended)
if [ ! -d "/opt/deviceloop-backend/venv" ]; then
  python3 -m venv /opt/deviceloop-backend/venv
fi

source /opt/deviceloop-backend/venv/bin/activate

python -m pip install --upgrade pip

if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt
fi

chown -R ubuntu:ubuntu /opt/deviceloop-backend || true
