# dbt-coder — Compiler-as-Reward for Code Generation

## What This Is
Fine-tune Qwen 2.5 Coder 7B on dbt/analytics engineering using RL with code execution (GRPO). Uses dbt-core and dbt-fusion as reward signals to test whether a stricter compiler produces better training signal. Key finding: fusion-trained model is 39% better on fusion eval AND 4% better on core eval.

## Project Structure

```
rl_sandbox/                    # Reward infrastructure
  reward.py                    # Score completions: 0.0→0.3→0.6→0.7→0.8→1.0
  prompts.py                   # 505 training prompts across 15 categories
  modal_reward_server.py       # Modal HTTP endpoint for parallel scoring
  test_e2e.py                  # Validate all prompts against both compilers
  dbt_project/                 # DuckDB sandbox template
    seeds/                     # 22-table jaffle_shop (1533 rows)
    models/sources.yml         # Source definitions

rl_training/                   # VeRL GRPO training configs
  config_core.py               # Model A (dbt-core reward)
  config_fusion.py             # Model B (dbt-fusion reward)
  run.sh                       # verl.trainer.main_ppo with GRPO config
  prepare_dataset.py           # Convert prompts → VeRL parquet (85/15 split)
  reward_function.py           # VeRL-compatible compute_score()
  system_prompt.txt            # <think>/<answer> format prompt

eval/                          # Evaluation
  gazelle_adapter.py           # Bridge to dbt-mcp-gazelle Spider2-DBT
  spider2_task_filter.py       # Filter Spider2-DBT tasks for single-shot eval

eval_dual_compiler.py          # Dual-compiler eval harness
scripts/                       # Data generation (seeds + prompts)
results/FINDINGS.md            # Detailed experiment results
```

## Key Commands

```bash
# Generate seed data + prompts
python scripts/generate_seeds.py
python scripts/generate_prompts.py
python scripts/generate_prompts_extra.py
python scripts/generate_prompts_batch3.py

# Test dbt project
cd rl_sandbox/dbt_project && dbt seed --profiles-dir . && dbt compile --profiles-dir .

# RL training (A/B experiment)
cd rl_training
truss train push config_core.py    # Model A: dbt-core reward
truss train push config_fusion.py  # Model B: dbt-fusion reward

# Modal reward server
cd rl_sandbox
modal deploy modal_reward_server.py

# Test reward function locally
cd rl_sandbox && python reward.py

# Dual-compiler eval
python eval_dual_compiler.py --base-model-id <ID> --core-model-id <ID> --fusion-model-id <ID>
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
