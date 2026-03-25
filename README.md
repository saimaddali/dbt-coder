# dbt-coder: Compiler-as-Reward for Code Generation

Fine-tuning Qwen 2.5 Coder 7B on dbt/analytics engineering using RL with code execution (GRPO). The key insight: **using a stricter compiler as the reward signal produces a better model, even when evaluated against the permissive compiler.**

## Key Finding

We trained two identical models — same base model, same prompts, same hyperparameters — differing only in which compiler scores the compile step:

| Eval Compiler | Core-RL Model | Fusion-RL Model | Delta |
|---------------|---------------|-----------------|-------|
| dbt-core      | 0.608         | 0.633           | +4%   |
| dbt-fusion    | 0.383         | **0.533**       | **+39%** |

The fusion-trained model writes better SQL on **both** compilers. The stricter compiler catches semantic errors (type mismatches, ambiguous references) at compile time that dbt-core only catches at runtime or not at all. This tighter feedback loop teaches the model to avoid entire classes of bugs.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Training Loop (VeRL GRPO, 4x H100)                    │
│                                                         │
│  prompt → model generates SQL → score → policy update   │
│                    │                                    │
│                    ▼                                    │
│  ┌──────────────────────────────┐                       │
│  │  Reward Function (Modal)     │                       │
│  │                              │                       │
│  │  SQL → dbt seed → compile    │ ← dbt-core OR fusion  │
│  │      → run → check columns  │                       │
│  │      → check row count      │                       │
│  │      → check duplicates     │                       │
│  │                              │                       │
│  │  Score: 0.0 → 0.3 → 0.6     │                       │
│  │       → 0.7 → 0.8 → 1.0    │                       │
│  └──────────────────────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

## Reward Tiers

| Score | Meaning |
|-------|---------|
| 0.0   | No valid SQL or doesn't compile |
| 0.3   | Compiles (valid Jinja + SQL structure) |
| 0.6   | Compiles AND runs against DuckDB |
| 0.7   | Runs, has expected columns, but row count off or duplicates detected |
| 0.8   | Runs, correct columns, row count within 20% tolerance, no duplicates |
| 1.0   | Full marks (all checks pass) |

## Project Structure

```
rl_sandbox/                    # Reward infrastructure
  reward.py                    # Score completions: 0.0 → 1.0
  modal_reward_server.py       # Modal HTTP endpoint for parallel scoring
  prompts.py                   # 505 training prompts (auto-generated)
  test_e2e.py                  # Validate prompts against both compilers
  dbt_project/                 # DuckDB sandbox template
    seeds/                     # 22-table jaffle_shop (1533 rows)
    models/sources.yml         # Source definitions

rl_training/                   # VeRL GRPO training configs
  config_core.py               # Model A: dbt-core reward signal
  config_fusion.py             # Model B: dbt-fusion reward signal
  reward_function.py           # VeRL-compatible compute_score()
  prepare_dataset.py           # Prompts → VeRL parquet (85/15 split)
  run.sh                       # Training entrypoint
  system_prompt.txt            # <think>/<answer> format prompt

eval/                          # Evaluation
  gazelle_adapter.py           # Bridge to dbt-mcp-gazelle Spider2-DBT
  spider2_task_filter.py       # Filter Spider2-DBT tasks for single-shot eval

eval_dual_compiler.py          # Dual-compiler eval harness

scripts/                       # Data generation
  generate_seeds.py            # Reproducible 22-table dataset (seed=42)
  generate_prompts.py          # Base 206 prompts
  generate_prompts_extra.py    # +141 prompts
  generate_prompts_batch3.py   # +158 prompts
```

## Dataset

22-table e-commerce schema (jaffle_shop), ~1533 rows with referential integrity:

| Table | Rows | Description |
|-------|------|-------------|
| raw_customers | 50 | Customer demographics, emails |
| raw_orders | 100 | Multi-year orders, various statuses |
| raw_payments | 119 | Multiple per order, various methods |
| raw_products | 30 | Categories, price ranges |
| raw_order_items | 200 | Line items with quantities, discounts |
| raw_addresses | 60 | Multiple per customer |
| raw_refunds | 25 | Reasons, statuses, processing times |
| raw_inventory | 50 | Stock levels, reorder points |
| raw_promotions | 20 | Discount codes, validity dates |
| raw_shipping | 75 | Carriers, tracking, delivery dates |
| raw_reviews | 60 | Ratings, review text |
| raw_sessions | 200 | Device, channel, timestamps |
| raw_page_views | 300 | URLs, durations |
| raw_support_tickets | 40 | Categories, priorities |
| raw_employees | 15 | Roles, departments |
| raw_suppliers | 10 | Countries, lead times |
| raw_returns | 30 | Reasons, conditions |
| raw_subscriptions | 25 | Plans, MRR |
| raw_categories | 8 | Hierarchical categories |
| raw_warehouses | 4 | Locations, capacity |
| raw_campaigns | 12 | Marketing campaigns |
| raw_email_events | 100 | Sent/opened/clicked |

## 505 Training Prompts

Covering 15 categories of dbt patterns:

- **Staging** (22): 1:1 source-to-staging transforms
- **Two-table joins** (80+): Various join patterns
- **Three-table joins** (30): Multi-source models
- **Aggregations** (20+): GROUP BY, HAVING patterns
- **Window functions** (34): RANK, ROW_NUMBER, LAG/LEAD, running totals
- **CASE/conditional** (30): Segmentation, tiering, flags
- **UNIONs** (24): Type-mixing patterns (fusion-divergent)
- **Incremental** (20): Merge strategy, watermarks
- **Jinja** (15): Variables, macros, conditionals
- **Complex multi-hop** (30): 4+ table joins
- **Subqueries** (20): EXISTS, correlated, comparison
- **Date/string ops** (25): Extraction, parsing, duration calc
- **Mart/dim/fact** (34): Dimensional modeling
- **NULL/defensive** (17): COALESCE, NULLIF, safe division
- **Cross-domain analytics** (15+): Cross-functional queries

## Quick Start

```bash
# Generate seed data
python scripts/generate_seeds.py

# Verify dbt project works
cd rl_sandbox/dbt_project
dbt seed --profiles-dir .
dbt compile --profiles-dir .

# Test reward function locally
cd rl_sandbox
python reward.py

# Deploy Modal reward server
cd rl_sandbox
modal deploy modal_reward_server.py

# Prepare training data
cd rl_training
python prepare_dataset.py --local_dir ./data/

# Launch RL training
cd rl_training
truss train push config_core.py     # Model A: dbt-core reward
truss train push config_fusion.py   # Model B: dbt-fusion reward

# Run dual-compiler eval
python eval_dual_compiler.py \
  --base-model-id <BASE_ID> \
  --core-model-id <CORE_ID> \
  --fusion-model-id <FUSION_ID>
```

## Why This Matters

Most code generation benchmarks test syntax correctness. But in analytics engineering, the harder bugs are semantic: type mismatches in UNIONs, fan-out joins creating duplicate rows, incorrect NULL handling. A stricter compiler catches these at compile time, providing a tighter reward signal during RL training.

dbt-fusion isn't just a better compiler for humans — it's a better training signal for AI models. No other SQL compiler can claim to improve AI model quality through training feedback. Compiler strictness is an underexplored axis for reward function design: a stricter compiler provides a tighter feedback signal without requiring test cases or gold outputs.
