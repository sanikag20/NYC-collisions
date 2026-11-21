#!/usr/bin/env bash
# Downloads the NYC Open Data "Motor Vehicle Collisions - Crashes" dataset (~500 MB CSV).
set -euo pipefail
PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
mkdir -p "$PROJECT_DIR/data/raw"
curl -L -o "$PROJECT_DIR/data/raw/collisions.csv" \
  "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"
ls -lh "$PROJECT_DIR/data/raw/collisions.csv"
