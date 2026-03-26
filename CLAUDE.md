# dbt-coder — Compiler-as-Reward for Code Generation

## What This Is
Fine-tune Qwen 2.5 Coder 7B on dbt/analytics engineering using RL with code execution (GRPO). Uses dbt-core and dbt-fusion as reward signals to test whether a stricter compiler produces better training signal. Key finding: fusion-trained model is 39% better on fusion eval AND 4% better on core eval.

## Data Architecture

Single source of truth: `rl_sandbox/` owns all training data (prompts, seeds, reward logic).
`train.sh` syncs everything into `rl_training/` before pushing to Baseten.
Never edit prompts or seeds in `rl_training/` directly — they get overwritten.

```
rl_sandbox/prompts.py          ← single source of truth for prompts
rl_sandbox/dbt_project/seeds/  ← single source of truth for seed data
         │
    train.sh syncs at launch
         │
         ▼
rl_training/data/*.parquet     ← built from prompts, uploaded to container
rl_training/dbt_project/       ← synced copy, used by reward fallback
```

## Key Commands

```bash
# Train (handles validate → build parquet → deploy Modal → push)
./train.sh fusion    # Model B: dbt-fusion reward signal
./train.sh core      # Model A: dbt-core reward signal

# Evaluate
./eval.sh --core <JOB_ID> --fusion <JOB_ID> --step 16

# Test reward function locally
cd rl_sandbox && python reward.py

# Regenerate data (only if changing schema)
python scripts/generate_seeds.py
python scripts/generate_prompts.py
```

## Reward Function Tiers
- **0.0** — No valid SQL or doesn't compile
- **0.3** — Compiles (valid Jinja + SQL structure)
- **0.6** — Compiles AND runs against DuckDB
- **0.7** — Runs, has expected columns, but row count off or duplicates detected
- **0.8** — Runs, correct columns, row count within 20% tolerance, no duplicates
- **1.0** — Full marks (all checks pass)

## A/B Experiment: dbt-core vs dbt-fusion
- Same base model (Qwen 2.5 Coder 7B), same prompts, same GRPO hyperparams
- Only difference: which compiler scores the compile step
- dbt-fusion catches semantic errors at compile time (type mismatches, missing columns)
- dbt-core only catches syntax errors at compile, defers semantic errors to runtime

## Key Findings (Run 1)
- Fusion-RL: 0.533 on fusion eval vs Core-RL: 0.383 (**39% better**)
- Fusion-RL: 0.633 on core eval vs Core-RL: 0.608 (4% better)
- Stricter compiler as reward signal produces better model on BOTH compilers
- See `results/FINDINGS.md` for full breakdown
