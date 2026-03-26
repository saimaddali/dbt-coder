#!/bin/bash
# Launch an RL training run.
#
# Usage:
#   ./train.sh fusion    # Model B: dbt-fusion as reward compiler
#   ./train.sh core      # Model A: dbt-core as reward compiler
#
# This script handles everything:
#   1. Validates prompts and seed data
#   2. Builds parquet dataset from rl_sandbox/prompts.py
#   3. Syncs data into rl_training/ (single source of truth: rl_sandbox/)
#   4. Deploys Modal reward server
#   5. Pushes training job to Baseten

set -euo pipefail

# ─── Parse args ───────────────────────────────────────────────────────
COMPILER="${1:-}"
if [[ "$COMPILER" != "fusion" && "$COMPILER" != "core" ]]; then
    echo "Usage: ./train.sh <fusion|core>"
    echo ""
    echo "  fusion — Model B: dbt-fusion as compile-step reward signal"
    echo "  core   — Model A: dbt-core as compile-step reward signal"
    exit 1
fi

SKIP_MODAL="${SKIP_MODAL:-false}"
CONFIG="rl_training/config_${COMPILER}.py"

echo "=== dbt-coder training: $COMPILER ==="
echo ""

# ─── 1. Validate ──────────────────────────────────────────────────────
echo "[1/5] Validating..."

PROMPT_COUNT=$(python3 -c "
import sys; sys.path.insert(0, 'rl_sandbox')
from prompts import RL_PROMPTS; print(len(RL_PROMPTS))
" 2>/dev/null || echo "0")

if [[ "$PROMPT_COUNT" -lt 400 ]]; then
    echo "  ERROR: Only $PROMPT_COUNT prompts in rl_sandbox/prompts.py (expected 400+)"
    exit 1
fi
echo "  $PROMPT_COUNT prompts"

SEED_COUNT=$(ls rl_sandbox/dbt_project/seeds/*.csv 2>/dev/null | wc -l | tr -d ' ')
if [[ "$SEED_COUNT" -lt 20 ]]; then
    echo "  ERROR: Only $SEED_COUNT seed CSVs (expected 20+)"
    exit 1
fi
echo "  $SEED_COUNT seed tables"

if [[ ! -f "$CONFIG" ]]; then
    echo "  ERROR: Config not found: $CONFIG"
    exit 1
fi
echo "  Config: $CONFIG"

# ─── 2. Build parquet ─────────────────────────────────────────────────
echo "[2/5] Building parquet dataset..."

# prepare_dataset.py imports from prompts.py in its own directory,
# so we copy the canonical prompts there first
cp rl_sandbox/prompts.py rl_training/prompts.py

python3 rl_training/prepare_dataset.py --local_dir rl_training/data/
echo "  Parquet written to rl_training/data/"

# ─── 3. Sync dbt_project ─────────────────────────────────────────────
echo "[3/5] Syncing dbt_project to rl_training/..."

# rsync canonical dbt_project into rl_training/ (excluding build artifacts)
rsync -a --delete \
    --exclude '.DS_Store' \
    --exclude '.user.yml' \
    --exclude 'target/' \
    --exclude 'logs/' \
    --exclude 'dbt_packages/' \
    rl_sandbox/dbt_project/ rl_training/dbt_project/

echo "  Synced $SEED_COUNT tables + sources.yml"

# ─── 4. Deploy Modal ─────────────────────────────────────────────────
if [[ "$SKIP_MODAL" == "true" ]]; then
    echo "[4/5] Skipping Modal deploy (SKIP_MODAL=true)"
else
    echo "[4/5] Deploying Modal reward server..."
    (cd rl_sandbox && modal deploy modal_reward_server.py)
    echo "  Modal deployed"
fi

# ─── 5. Push training job ─────────────────────────────────────────────
echo "[5/5] Pushing training job to Baseten..."
echo ""
echo "  Config:  $CONFIG"
echo "  Prompts: $PROMPT_COUNT"
echo "  Seeds:   $SEED_COUNT tables"
echo ""

# Clean up temporary prompts.py copy (parquet is already built)
rm -f rl_training/prompts.py

truss train push "$CONFIG"
