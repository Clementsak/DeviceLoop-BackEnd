#!/usr/bin/env bash
set -euo pipefail

systemctl daemon-reload
systemctl restart deviceloop-backend
systemctl enable deviceloop-backend
