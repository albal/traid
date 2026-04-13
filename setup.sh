#!/usr/bin/env bash
# TRAID installation script
# Must be run as root on Debian 13.

set -euo pipefail

TRAID_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$TRAID_DIR/venv"

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: setup.sh must be run as root (sudo ./setup.sh)" >&2
  exit 1
fi

if ! command -v python3.11 &>/dev/null; then
  echo "ERROR: python3.11 not found. Install it with: apt install python3.11 python3.11-venv" >&2
  exit 1
fi

echo "==> TRAID install — working directory: $TRAID_DIR"

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------

echo "==> Installing system packages..."
apt-get install -y --no-install-recommends \
  mdadm \
  lvm2 \
  parted \
  python3.11-venv \
  python3.11-dev

# ---------------------------------------------------------------------------
# Python virtual environment
# ---------------------------------------------------------------------------

echo "==> Creating Python venv at $VENV_DIR..."
python3.11 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip --quiet
"$VENV_DIR/bin/pip" install -r "$TRAID_DIR/requirements.txt" --quiet
echo "    Python dependencies installed."

# ---------------------------------------------------------------------------
# Ensure www-data group membership for socket access
# ---------------------------------------------------------------------------

echo "==> Checking www-data group..."
if ! getent group www-data &>/dev/null; then
  groupadd --system www-data
  echo "    Created www-data group."
fi

# ---------------------------------------------------------------------------
# Systemd services
# ---------------------------------------------------------------------------

echo "==> Installing systemd units..."
cp "$TRAID_DIR/systemd/hybrid-worker.service" /etc/systemd/system/
cp "$TRAID_DIR/systemd/hybrid-web.service" /etc/systemd/system/
systemctl daemon-reload

echo "==> Enabling and starting services..."
systemctl enable hybrid-worker hybrid-web
systemctl start hybrid-worker

# Give the worker a moment to bind the socket before starting the web service
sleep 2
systemctl start hybrid-web

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

echo ""
echo "==> Installation complete."
echo ""
systemctl status hybrid-worker --no-pager --lines=5 || true
echo ""
systemctl status hybrid-web --no-pager --lines=5 || true
echo ""
echo "TRAID is available at: http://localhost:8000"
