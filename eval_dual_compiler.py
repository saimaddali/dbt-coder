"""
Dual-compiler eval for dbt-coder RL models.

Generates completions from a model endpoint, then scores each completion
through BOTH dbt-core and dbt-fusion via Modal. This reveals whether the
fusion-trained model produces more semantically correct SQL even when
dbt-core would accept it.

Usage:
  # Score all 3 models (base, core-RL, fusion-RL) through both compilers
  python eval_dual_compiler.py --base-model-id qjdo9y2q \
                                --core-model-id <CORE_MODEL_ID> \
                                --fusion-model-id <FUSION_MODEL_ID>

  # Just score one model
  python eval_dual_compiler.py --base-model-id qjdo9y2q

  # Use local completions file (skip generation)
  python eval_dual_compiler.py --completions-file completions.json
"""

import os
import re
import json
import time
import argparse
import requests
from pathlib import Path

BASETEN_API_KEY = os.environ.get("BASETEN_API_KEY", "")
MODAL_REWARD_URL = os.environ.get(
    "MODAL_REWARD_URL",
    "https://sai-maddali3--dbt-coder-reward-reward-endpoint.modal.run"
)

# System prompt matches training format
SYSTEM_PROMPT = Path(__file__).parent / "rl_training" / "system_prompt.txt"
if SYSTEM_PROMPT.exists():
    SYSTEM_PROMPT = SYSTEM_PROMPT.read_text()
else:
    SYSTEM_PROMPT = (
        "You are an expert analytics engineer. When given a dbt modeling task, "
        "think through it step by step in <think> tags, then provide your SQL "
        "answer in <answer> tags with a sql code fence."
    )

# Eval prompts — mix of simple and fusion-divergent patterns
EVAL_PROMPTS = [
    # Simple staging (both compilers should agree)
    {
        "id": "stg_customers",
        "prompt": "Write a dbt staging model called stg_customers that reads from {{ source('jaffle_shop', 'raw_customers') }}. Rename 'id' to 'customer_id', cast 'created_at' to date, include first_name, last_name, email.",
        "model_name": "stg_customers",
        "expected_columns": ["customer_id", "first_name", "last_name", "email"],
    },
    {
        "id": "stg_orders",
        "prompt": "Write a dbt staging model called stg_orders that reads from {{ source('jaffle_shop', 'raw_orders') }}. Rename 'id' to 'order_id', convert amount from cents to dollars (divide by 100.0), cast order_date to date.",
        "model_name": "stg_orders",
        "expected_columns": ["order_id", "customer_id", "order_date", "amount"],
    },
    # Joins (moderate complexity)
    {
        "id": "customer_orders",
        "prompt": "Write a dbt model called customer_orders that joins raw_customers and raw_orders on customer_id. Include customer_id, first_name, last_name, order count, and total order amount in dollars. Use CTEs.",
        "model_name": "customer_orders",
        "expected_columns": ["customer_id", "first_name", "order_count", "total_amount"],
    },
    # Multi-table join
    {
        "id": "order_details",
        "prompt": "Write a dbt model called order_details that joins raw_orders, raw_order_items, and raw_products. Include order_id, product_name, quantity, unit_price, and line_total (quantity * unit_price). Use CTEs with {{ source('jaffle_shop', ...) }}.",
        "model_name": "order_details",
        "expected_columns": ["order_id", "product_name", "quantity", "line_total"],
    },
    # UNION — fusion-divergent (type mismatch potential)
    {
        "id": "all_events",
        "prompt": "Write a dbt model called all_events that UNIONs order events and payment events into a single timeline. From raw_orders use order_date as event_date and 'order' as event_type. From raw_payments use payment_date as event_date and 'payment' as event_type. Include relevant IDs.",
        "model_name": "all_events",
        "expected_columns": ["event_date", "event_type"],
    },
    # Aggregation with window function
    {
        "id": "customer_lifetime",
        "prompt": "Write a dbt model called customer_lifetime that calculates for each customer: first_order_date, most_recent_order_date, lifetime_order_count, lifetime_spend_dollars (amount/100), and days_as_customer (most_recent - first). Join raw_customers and raw_orders.",
        "model_name": "customer_lifetime",
        "expected_columns": ["customer_id", "first_order_date", "lifetime_order_count", "lifetime_spend_dollars"],
    },
    # CASE statement
    {
        "id": "order_status_summary",
        "prompt": "Write a dbt model called order_status_summary that categorizes orders from raw_orders into 'high_value' (amount > 5000), 'medium_value' (1000-5000), and 'low_value' (< 1000). Include order_id, customer_id, amount, and value_category.",
        "model_name": "order_status_summary",
        "expected_columns": ["order_id", "value_category"],
    },
    # Incremental pattern
    {
        "id": "incremental_orders",
        "prompt": "Write a dbt incremental model called incremental_orders that reads from {{ source('jaffle_shop', 'raw_orders') }}. Use the merge strategy, unique on id, and incrementally load based on order_date. Include all columns.",
        "model_name": "incremental_orders",
        "expected_columns": ["id", "customer_id", "order_date"],
    },
    # Jinja variable + conditional
    {
        "id": "filtered_payments",
        "prompt": "Write a dbt model called filtered_payments that reads from raw_payments. Use a Jinja variable to set a minimum payment amount (default 1000). Filter out payments below that threshold. Include payment_id (renamed from id), order_id, amount, and payment_method.",
        "model_name": "filtered_payments",
        "expected_columns": ["payment_id", "order_id", "amount"],
    },
    # Complex — 4 table join with aggregation
    {
        "id": "customer_360",
        "prompt": "Write a dbt model called customer_360 that creates a complete customer profile. Join raw_customers, raw_orders, raw_payments, and raw_addresses. For each customer include: customer_id, full_name (first || last), email, total_orders, total_payments_dollars, default_city, default_state. Use CTEs.",
        "model_name": "customer_360",
        "expected_columns": ["customer_id", "full_name", "total_orders", "total_payments_dollars"],
    },
    # Refund analysis — tests fusion on nullable/type handling
    {
        "id": "refund_analysis",
        "prompt": "Write a dbt model called refund_analysis that joins raw_refunds with raw_orders and raw_order_items. Calculate refund_rate (refund_amount / order total), include order_id, refund_reason, refund_status, and days_to_process (processed_at - requested_at). Handle NULLs for pending refunds.",
        "model_name": "refund_analysis",
        "expected_columns": ["order_id", "refund_reason", "refund_status"],
    },
    # Window function — running total
    {
        "id": "running_revenue",
        "prompt": "Write a dbt model called running_revenue that calculates daily revenue from raw_orders (amount/100 as revenue_dollars) and a running total using a window function ordered by order_date. Include order_date, daily_revenue, and cumulative_revenue.",
        "model_name": "running_revenue",
        "expected_columns": ["order_date", "daily_revenue", "cumulative_revenue"],
    },
]


def generate_completion(model_id, prompt, system_prompt=SYSTEM_PROMPT, model_name=None):
    """Generate a completion from a Baseten model endpoint."""
    # Support both model ID and full URL
    if model_id.startswith("http"):
        url = model_id.rstrip("/") + "/chat/completions"
    else:
        url = f"https://model-{model_id}.api.baseten.co/environments/production/sync/v1/chat/completions"
    headers = {"Authorization": f"Api-Key {BASETEN_API_KEY}"}
    payload = {
        "model": model_name or "baseten-model",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 4096,
        "temperature": 0.1,
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    if "choices" in data:
        return data["choices"][0]["message"]["content"]
    elif "output" in data:
        return data["output"]
    else:
        return str(data)


def extract_sql_from_completion(completion):
    """Extract SQL from <answer>...</answer> tags."""
    match = re.findall(r"<answer>(.*?)</answer>", completion, re.DOTALL)
    if len(match) != 1:
        return ""
    sql = match[0].strip()
    fence = re.search(r"```(?:sql)?\s*\n(.*?)```", sql, re.DOTALL)
    if fence:
        sql = fence.group(1).strip()
    return sql


def score_completions_dual(completions, eval_prompts):
    """Score completions through BOTH compilers via Modal."""
    results = []

    for compiler in ["dbt-core", "dbt-fusion"]:
        items = []
        for comp, ep in zip(completions, eval_prompts):
            sql = extract_sql_from_completion(comp)
            items.append({
                "prompt": ep["prompt"],
                "completion": f"```sql\n{sql}\n```" if sql else "",
                "model_name": ep["model_name"],
                "expected_columns": ep.get("expected_columns", []),
            })

        try:
            resp = requests.post(
                MODAL_REWARD_URL,
                json={"completions": items, "compiler": compiler},
                timeout=600,
            )
            resp.raise_for_status()
            scores = resp.json()["scores"]
        except Exception as e:
            print(f"  Modal scoring failed for {compiler}: {e}")
            scores = [{"score": -1}] * len(items)

        for i, s in enumerate(scores):
            if len(results) <= i:
                results.append({"id": eval_prompts[i]["id"]})
            results[i][f"{compiler}_score"] = s["score"]
            results[i][f"{compiler}_compile"] = s.get("compile_ok", None)
            results[i][f"{compiler}_run"] = s.get("run_ok", None)
            results[i][f"{compiler}_columns"] = s.get("columns_ok", None)

    return results


def run_eval(model_id, display_name, eval_prompts, vllm_model_name=None):
    """Generate completions and score through both compilers."""
    print(f"\n{'='*60}")
    print(f"Evaluating: {display_name} (model_id={model_id})")
    print(f"{'='*60}")

    # Generate completions
    completions = []
    for i, ep in enumerate(eval_prompts):
        print(f"  [{i+1}/{len(eval_prompts)}] Generating: {ep['id']}...", end=" ", flush=True)
        try:
            comp = generate_completion(model_id, ep["prompt"], model_name=vllm_model_name)
            completions.append(comp)
            sql = extract_sql_from_completion(comp)
            print(f"{'OK' if sql else 'NO SQL'} ({len(sql)} chars)")
        except Exception as e:
            print(f"FAILED: {e}")
            completions.append("")

    # Score through both compilers
    print(f"\n  Scoring {len(completions)} completions through both compilers via Modal...")
    results = score_completions_dual(completions, eval_prompts)

    return completions, results


def print_results_table(all_results):
    """Print comparison table across models and compilers."""
    print(f"\n{'='*80}")
    print("DUAL-COMPILER EVAL RESULTS")
    print(f"{'='*80}\n")

    # Header
    models = list(all_results.keys())
    header = f"{'Prompt':<25}"
    for model in models:
        header += f" | {model[:12]:>12} core  {model[:12]:>12} fusion"
    print(header)
    print("-" * len(header))

    prompt_ids = [r["id"] for r in list(all_results.values())[0]]

    for pid in prompt_ids:
        row = f"{pid:<25}"
        for model in models:
            results = all_results[model]
            r = next((x for x in results if x["id"] == pid), None)
            if r:
                core = r.get("dbt-core_score", -1)
                fusion = r.get("dbt-fusion_score", -1)
                core_str = f"{core:.1f}" if core >= 0 else "ERR"
                fusion_str = f"{fusion:.1f}" if fusion >= 0 else "ERR"
                divergent = " *" if core >= 0 and fusion >= 0 and abs(core - fusion) > 0.1 else "  "
                row += f" |    {core_str:>4}         {fusion_str:>4}{divergent}"
            else:
                row += f" |     ---          ---  "
        print(row)

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY (* = divergent: core and fusion scores differ by > 0.1)\n")
    for model in models:
        results = all_results[model]
        core_scores = [r["dbt-core_score"] for r in results if r.get("dbt-core_score", -1) >= 0]
        fusion_scores = [r["dbt-fusion_score"] for r in results if r.get("dbt-fusion_score", -1) >= 0]
        divergent = sum(1 for r in results
                       if r.get("dbt-core_score", -1) >= 0
                       and r.get("dbt-fusion_score", -1) >= 0
                       and abs(r["dbt-core_score"] - r["dbt-fusion_score"]) > 0.1)

        print(f"  {model}:")
        print(f"    dbt-core  mean: {sum(core_scores)/len(core_scores):.3f}" if core_scores else "    dbt-core  mean: N/A")
        print(f"    dbt-fusion mean: {sum(fusion_scores)/len(fusion_scores):.3f}" if fusion_scores else "    dbt-fusion mean: N/A")
        print(f"    divergent prompts: {divergent}/{len(results)}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Dual-compiler eval for dbt-coder models")
    parser.add_argument("--base-model-id", help="Base Qwen model ID on Baseten")
    parser.add_argument("--core-model-id", help="Core-RL trained model ID")
    parser.add_argument("--fusion-model-id", help="Fusion-RL trained model ID")
    parser.add_argument("--completions-file", help="Skip generation, load completions from JSON")
    parser.add_argument("--output", default="eval_results/dual_compiler_eval.json",
                       help="Output file for results")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    all_results = {}
    all_completions = {}

    # Models: (endpoint_id, vllm_model_name)
    # LoRA checkpoints use "global_step_27/actor/lora_adapter" as model name
    # Base model uses "baseten-model"
    LORA_MODEL_NAME = "global_step_27/actor/lora_adapter"
    models = {}
    model_names = {}
    if args.base_model_id:
        models["base"] = args.base_model_id
        model_names["base"] = "baseten-model"
    if args.core_model_id:
        models["core-rl"] = args.core_model_id
        model_names["core-rl"] = LORA_MODEL_NAME
    if args.fusion_model_id:
        models["fusion-rl"] = args.fusion_model_id
        model_names["fusion-rl"] = LORA_MODEL_NAME

    if not models and not args.completions_file:
        parser.error("Provide at least one model ID or --completions-file")

    if args.completions_file:
        with open(args.completions_file) as f:
            saved = json.load(f)
        for model_name, completions in saved.items():
            print(f"\nScoring saved completions for {model_name}...")
            results = score_completions_dual(completions, EVAL_PROMPTS)
            all_results[model_name] = results
            all_completions[model_name] = completions
    else:
        for display_name, model_id in models.items():
            vllm_name = model_names.get(display_name)
            completions, results = run_eval(model_id, display_name, EVAL_PROMPTS,
                                            vllm_model_name=vllm_name)
            all_results[display_name] = results
            all_completions[display_name] = completions

    print_results_table(all_results)

    # Save everything
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "models": models if models else "from_file",
        "results": all_results,
        "completions": all_completions,
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
