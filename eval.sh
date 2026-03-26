#!/bin/bash
# Evaluate trained models with dual-compiler scoring.
#
# Usage:
#   ./eval.sh --core <JOB_ID> --fusion <JOB_ID>              # eval both
#   ./eval.sh --core <JOB_ID> --fusion <JOB_ID> --step 16    # specific checkpoint
#   ./eval.sh --core <JOB_ID>                                 # eval one
#   ./eval.sh --fusion <JOB_ID> --base <MODEL_ID>             # include base model
#
# This script handles everything:
#   1. Deploys checkpoints as model endpoints
#   2. Waits for models to become active
#   3. Runs dual-compiler eval (scores through both dbt-core and dbt-fusion)
#   4. Prints comparison table

set -euo pipefail

# ─── Parse args ───────────────────────────────────────────────────────
CORE_JOB=""
FUSION_JOB=""
BASE_MODEL=""
STEP=""
OUTPUT="eval_results/dual_compiler_eval.json"

while [[ $# -gt 0 ]]; do
    case $1 in
        --core)    CORE_JOB="$2"; shift 2 ;;
        --fusion)  FUSION_JOB="$2"; shift 2 ;;
        --base)    BASE_MODEL="$2"; shift 2 ;;
        --step)    STEP="$2"; shift 2 ;;
        --output)  OUTPUT="$2"; shift 2 ;;
        *)         echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -z "$CORE_JOB" && -z "$FUSION_JOB" && -z "$BASE_MODEL" ]]; then
    echo "Usage: ./eval.sh --core <JOB_ID> --fusion <JOB_ID> [--step N] [--base <MODEL_ID>]"
    exit 1
fi

if [[ -z "$BASETEN_API_KEY" ]]; then
    echo "ERROR: BASETEN_API_KEY not set"
    exit 1
fi

echo "=== dbt-coder eval ==="
echo ""

# ─── Deploy checkpoints ──────────────────────────────────────────────
CORE_MODEL=""
FUSION_MODEL=""

deploy_checkpoint() {
    local job_id="$1"
    local label="$2"
    local step_flag=""

    if [[ -n "$STEP" ]]; then
        step_flag="--checkpoint global_step_${STEP}"
    fi

    echo "  Deploying $label checkpoint from job $job_id..."
    local deploy_output
    deploy_output=$(truss train deploy "$job_id" $step_flag 2>&1)
    echo "$deploy_output"

    # Extract model ID from deploy output
    local model_id
    model_id=$(echo "$deploy_output" | grep -oE '[a-z0-9]{8,}' | tail -1)
    echo "$model_id"
}

if [[ -n "$CORE_JOB" ]]; then
    echo "[1] Deploying core checkpoint..."
    CORE_MODEL=$(deploy_checkpoint "$CORE_JOB" "core-rl")
    echo "  Core model ID: $CORE_MODEL"
fi

if [[ -n "$FUSION_JOB" ]]; then
    echo "[2] Deploying fusion checkpoint..."
    FUSION_MODEL=$(deploy_checkpoint "$FUSION_JOB" "fusion-rl")
    echo "  Fusion model ID: $FUSION_MODEL"
fi

# ─── Wait for models ─────────────────────────────────────────────────
echo ""
echo "Models are deploying. This typically takes 5-10 minutes."
echo "Check status with:"
[[ -n "$CORE_MODEL" ]] && echo "  truss train status $CORE_JOB"
[[ -n "$FUSION_MODEL" ]] && echo "  truss train status $FUSION_JOB"
echo ""
echo "Once models are ACTIVE, run the eval:"
echo ""

# ─── Build eval command ──────────────────────────────────────────────
EVAL_CMD="python eval_dual_compiler.py"
[[ -n "$BASE_MODEL" ]] && EVAL_CMD="$EVAL_CMD --base-model-id $BASE_MODEL"
[[ -n "$CORE_MODEL" ]] && EVAL_CMD="$EVAL_CMD --core-model-id $CORE_MODEL"
[[ -n "$FUSION_MODEL" ]] && EVAL_CMD="$EVAL_CMD --fusion-model-id $FUSION_MODEL"
[[ -n "$STEP" ]] && EVAL_CMD="$EVAL_CMD --checkpoint-step $STEP"
EVAL_CMD="$EVAL_CMD --output $OUTPUT"

echo "  $EVAL_CMD"
echo ""
read -p "Press Enter when models are ACTIVE to run eval (or Ctrl+C to run later)..."

echo ""
echo "Running dual-compiler eval..."
$EVAL_CMD
