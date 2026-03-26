#!/bin/bash
# Pre-flight checks before launching RL training.
# Run this BEFORE `truss train push` to catch problems that cost money.
#
# Usage:
#   ./preflight.sh            # check everything
#   ./preflight.sh --fix      # check + auto-fix what's fixable

set -euo pipefail

FIX=false
[[ "${1:-}" == "--fix" ]] && FIX=true

ERRORS=0
WARNINGS=0

pass()  { echo "  ✓ $1"; }
fail()  { echo "  ✗ $1"; ERRORS=$((ERRORS + 1)); }
warn()  { echo "  ! $1"; WARNINGS=$((WARNINGS + 1)); }

echo "=== dbt-coder pre-flight checks ==="
echo ""

# ─── 1. Prompts in sync ───────────────────────────────────────────────
echo "[1/5] Checking prompts.py sync..."

SANDBOX_COUNT=$(python3 -c "
import sys; sys.path.insert(0, 'rl_sandbox')
from prompts import RL_PROMPTS; print(len(RL_PROMPTS))
" 2>/dev/null || echo "0")

TRAINING_COUNT=$(python3 -c "
import sys; sys.path.insert(0, 'rl_training')
from prompts import RL_PROMPTS; print(len(RL_PROMPTS))
" 2>/dev/null || echo "0")

if [[ "$SANDBOX_COUNT" == "0" ]]; then
    fail "Cannot read rl_sandbox/prompts.py"
elif [[ "$TRAINING_COUNT" == "0" ]]; then
    fail "Cannot read rl_training/prompts.py"
elif [[ "$SANDBOX_COUNT" != "$TRAINING_COUNT" ]]; then
    fail "Prompt count mismatch: rl_sandbox=$SANDBOX_COUNT, rl_training=$TRAINING_COUNT"
    if $FIX; then
        cp rl_sandbox/prompts.py rl_training/prompts.py
        echo "       → Fixed: copied rl_sandbox/prompts.py → rl_training/prompts.py"
    else
        echo "       → Run: ./preflight.sh --fix  OR  cp rl_sandbox/prompts.py rl_training/prompts.py"
    fi
else
    # Same count — check if files are identical
    if diff -q rl_sandbox/prompts.py rl_training/prompts.py > /dev/null 2>&1; then
        pass "prompts.py in sync ($SANDBOX_COUNT prompts)"
    else
        fail "prompts.py files differ despite same count — content mismatch"
        if $FIX; then
            cp rl_sandbox/prompts.py rl_training/prompts.py
            echo "       → Fixed: copied rl_sandbox/prompts.py → rl_training/prompts.py"
        fi
    fi
fi

# ─── 2. Seed data ─────────────────────────────────────────────────────
echo "[2/5] Checking seed data..."

SEED_COUNT=$(ls rl_sandbox/dbt_project/seeds/*.csv 2>/dev/null | wc -l | tr -d ' ')
if [[ "$SEED_COUNT" -lt 20 ]]; then
    fail "Only $SEED_COUNT seed CSVs found (expected 22)"
else
    pass "$SEED_COUNT seed CSVs in dbt_project/seeds/"
fi

# Check sources.yml references match seed files
if [[ -f rl_sandbox/dbt_project/models/sources.yml ]]; then
    SOURCE_TABLES=$(grep "name: raw_" rl_sandbox/dbt_project/models/sources.yml | wc -l | tr -d ' ')
    if [[ "$SOURCE_TABLES" != "$SEED_COUNT" ]]; then
        warn "sources.yml has $SOURCE_TABLES tables but $SEED_COUNT CSVs exist"
    else
        pass "sources.yml matches seed count ($SOURCE_TABLES tables)"
    fi
fi

# ─── 3. Reward function ───────────────────────────────────────────────
echo "[3/5] Checking reward function..."

if grep -q "check_row_count" rl_sandbox/reward.py 2>/dev/null; then
    pass "reward.py has row count validation"
else
    warn "reward.py missing check_row_count — v2 reward tiers won't work"
fi

if grep -q "check_no_duplicates" rl_sandbox/reward.py 2>/dev/null; then
    pass "reward.py has duplicate detection"
else
    warn "reward.py missing check_no_duplicates — v2 reward tiers won't work"
fi

# ─── 4. Training config sanity ────────────────────────────────────────
echo "[4/5] Checking training configs..."

for config in rl_training/config_core.py rl_training/config_fusion.py; do
    if [[ -f "$config" ]]; then
        if grep -q "MODAL_REWARD_URL" "$config"; then
            pass "$(basename $config) has MODAL_REWARD_URL"
        else
            fail "$(basename $config) missing MODAL_REWARD_URL"
        fi
    else
        fail "$(basename $config) not found"
    fi
done

# Check hyperparams in run.sh
if [[ -f rl_training/run.sh ]]; then
    LR=$(grep -o "lr=[0-9e.-]*" rl_training/run.sh | head -1)
    KL=$(grep -o "kl_loss_coef=[0-9e.-]*" rl_training/run.sh | head -1)
    EPOCHS=$(grep -o "total_epochs=[0-9]*" rl_training/run.sh | head -1)
    pass "run.sh hyperparams: $LR, $KL, $EPOCHS"
fi

# ─── 5. Uncommitted changes ───────────────────────────────────────────
echo "[5/5] Checking git state..."

if git diff --quiet rl_training/ rl_sandbox/reward.py rl_sandbox/modal_reward_server.py 2>/dev/null; then
    pass "No uncommitted changes in training/reward files"
else
    CHANGED=$(git diff --name-only rl_training/ rl_sandbox/reward.py rl_sandbox/modal_reward_server.py 2>/dev/null)
    warn "Uncommitted changes in: $CHANGED"
    echo "       → Training will use the COMMITTED version, not your local changes"
fi

# ─── Summary ──────────────────────────────────────────────────────────
echo ""
echo "=== Results ==="
if [[ $ERRORS -gt 0 ]]; then
    echo "  $ERRORS error(s), $WARNINGS warning(s) — DO NOT launch training"
    exit 1
elif [[ $WARNINGS -gt 0 ]]; then
    echo "  0 errors, $WARNINGS warning(s) — review warnings before launching"
    exit 0
else
    echo "  All checks passed — safe to launch training"
    exit 0
fi
