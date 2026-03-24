#!/bin/bash
# PropEdge V8 — Automation Setup
# Run once after cloning the repo to activate launchd agents.
# Usage: bash setup_automation.sh

set -e

REPO_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
PLIST_DIR="$REPO_PATH/launchd"

echo "==================================="
echo "PropEdge V8 — Automation Setup"
echo "==================================="
echo "Repo: $REPO_PATH"
echo ""

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 not found. Install from https://python.org"
    exit 1
fi

# Check pip packages
echo "[1/4] Checking Python dependencies..."
python3 -c "import pandas, openpyxl, requests" 2>/dev/null || {
    echo "Installing dependencies..."
    pip3 install pandas openpyxl requests nba_api
}
echo "  OK"

# Patch REPO_PATH into plist files
echo "[2/4] Patching plist files with repo path..."
mkdir -p "$LAUNCHD_DIR"
for plist in "$PLIST_DIR"/*.plist; do
    name=$(basename "$plist")
    dest="$LAUNCHD_DIR/$name"
    sed "s|REPO_PATH|$REPO_PATH|g" "$plist" > "$dest"
    echo "  Written: $dest"
done

# Unload existing agents (ignore errors if not loaded)
echo "[3/4] Unloading any existing agents..."
for plist in "$PLIST_DIR"/*.plist; do
    name=$(basename "$plist" .plist)
    launchctl unload "$LAUNCHD_DIR/$name.plist" 2>/dev/null || true
done

# Load all agents
echo "[4/4] Loading agents..."
for plist in "$LAUNCHD_DIR"/com.propedge.*.plist; do
    launchctl load "$plist"
    echo "  Loaded: $(basename "$plist")"
done

echo ""
echo "==================================="
echo "Automation active. Schedule (UK):"
echo "  06:00 — grade_today.py"
echo "  08:00 — prematch_today.py"
echo "  18:00 — prematch_today.py"
echo "  22:00 — prematch_today.py"
echo "==================================="
echo ""
echo "To run manually:"
echo "  python3 scripts/run_everything.py"
echo "  python3 scripts/prematch_today.py"
echo "  python3 scripts/grade_today.py"
echo "  python3 scripts/fetch_grade_setup.py  (first time only)"
