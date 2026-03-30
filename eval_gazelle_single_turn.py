"""
Single-turn eval using Gazelle/Spider2-DBT task prompts.

Extracts real-world dbt task descriptions from the Spider2-DBT benchmark,
generates SQL completions from fine-tuned models, and scores through both
dbt-core and dbt-fusion compilers.

This bridges our compiler-as-reward eval with external benchmark tasks the
model was NOT trained on, without requiring agentic tool-calling capabilities.

Usage:
  # Compare all three models
  python eval_gazelle_single_turn.py \
    --base-url https://model-XXX.api.baseten.co/environments/production/sync/v1 \
    --base-model baseten-model \
    --fusion-url https://model-YYY.api.baseten.co/environments/production/sync/v1 \
    --fusion-model "global_step_64/actor/lora_adapter" \
    --core-url https://model-ZZZ.api.baseten.co/environments/production/sync/v1 \
    --core-model "global_step_32/actor/lora_adapter" \
    --num-tasks 20

  # Just one model
  python eval_gazelle_single_turn.py \
    --fusion-url https://model-YYY.api.baseten.co/environments/production/sync/v1 \
    --fusion-model "global_step_64/actor/lora_adapter"
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

GAZELLE_TASKS_PATH = Path(__file__).parent.parent / "dbt-mcp-gazelle" / "benchmarks" / "spider2" / "tasks.jsonl"
LLM_BENCH_TASKS_PATH = Path(__file__).parent.parent / "dbt-mcp-gazelle" / "benchmarks" / "llm-bench" / "tasks.jsonl"

SYSTEM_PROMPT = (
    "You are an expert analytics engineer specializing in dbt. "
    "Given a task description and output schema, write the dbt SQL model(s) that produce the required output. "
    "Think through the task step by step in <think> tags, then provide your SQL in <answer> tags with a sql code fence. "
    "Use {{ source('...', '...') }} references. Write clean, production-quality SQL with CTEs."
)


def load_tasks(tasks_path, max_tasks=None):
    """Load tasks from a JSONL file."""
    tasks = []
    if not tasks_path.exists():
        print(f"  Warning: {tasks_path} not found")
        return tasks

    with open(tasks_path) as f:
        for line in f:
            task = json.loads(line.strip())
            if task.get("instance_id", "").startswith("demo"):
                continue
            tasks.append(task)

    if max_tasks:
        tasks = tasks[:max_tasks]
    return tasks


def format_prompt(task):
    """Convert a Spider2/LLM-bench task into a single-turn prompt."""
    instruction = task["instruction"]
    output_schema = task.get("output_schema", {})

    prompt = f"Task: {instruction}\n\n"

    if output_schema:
        prompt += "## REQUIRED OUTPUT SCHEMA\n\n"
        prompt += "You MUST produce dbt model(s) matching this exact schema:\n\n"
        for table_name, spec in output_schema.items():
            columns = spec.get("columns", {})
            prompt += f"### {table_name}\n"
            prompt += "| Column | Type |\n|--------|------|\n"
            for col, dtype in columns.items():
                prompt += f"| {col} | {dtype} |\n"
            prompt += "\n"

    prompt += (
        "Write the SQL for the primary output model. "
        "Use {{ source('...', '...') }} for input tables. "
        "Include all required columns with correct names."
    )
    return prompt


def generate_completion(url, model_name, prompt, api_key):
    """Generate a completion from a model endpoint."""
    headers = {"Authorization": f"Api-Key {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 4096,
        "temperature": 0.1,
    }
    endpoint = url.rstrip("/") + "/chat/completions"
    resp = requests.post(endpoint, json=payload, headers=headers, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def extract_sql(completion):
    """Extract SQL from <answer> tags or code fences."""
    # Try <answer> tags first
    match = re.findall(r"<answer>(.*?)</answer>", completion, re.DOTALL)
    if match:
        sql = match[0].strip()
        fence = re.search(r"```(?:sql)?\s*\n(.*?)```", sql, re.DOTALL)
        if fence:
            return fence.group(1).strip()
        return sql

    # Fall back to any SQL code fence
    fence = re.search(r"```(?:sql)?\s*\n(.*?)```", completion, re.DOTALL)
    if fence:
        return fence.group(1).strip()

    return ""


def score_via_modal(items, compiler):
    """Score completions through Modal reward server."""
    try:
        resp = requests.post(
            MODAL_REWARD_URL,
            json={"completions": items, "compiler": compiler},
            timeout=600,
        )
        resp.raise_for_status()
        return resp.json()["scores"]
    except Exception as e:
        print(f"  Modal scoring failed for {compiler}: {e}")
        return [{"score": -1}] * len(items)


def run_model_eval(url, model_name, display_name, tasks, api_key):
    """Run eval for a single model across all tasks."""
    print(f"\n{'='*60}")
    print(f"  {display_name}")
    print(f"  endpoint: {url}")
    print(f"  model: {model_name}")
    print(f"  tasks: {len(tasks)}")
    print(f"{'='*60}")

    completions = []
    sqls = []
    for i, task in enumerate(tasks):
        prompt = format_prompt(task)
        tid = task["instance_id"]
        print(f"  [{i+1}/{len(tasks)}] {tid}...", end=" ", flush=True)
        try:
            comp = generate_completion(url, model_name, prompt, api_key)
            sql = extract_sql(comp)
            completions.append(comp)
            sqls.append(sql)
            print(f"{'OK' if sql else 'NO SQL'} ({len(sql)} chars)")
        except Exception as e:
            print(f"FAILED: {e}")
            completions.append("")
            sqls.append("")

    # Score through both compilers
    print(f"\n  Scoring {len(sqls)} completions through both compilers...")

    # Get first table name from output_schema as model_name
    results = []
    for compiler in ["dbt-core", "dbt-fusion"]:
        items = []
        for task, sql in zip(tasks, sqls):
            schema = task.get("output_schema", {})
            table_name = list(schema.keys())[0] if schema else "generated_model"
            expected_cols = []
            if schema:
                first_table = list(schema.values())[0]
                expected_cols = [c.lower() for c in first_table.get("columns", {}).keys()][:5]

            items.append({
                "prompt": task["instruction"],
                "completion": f"```sql\n{sql}\n```" if sql else "",
                "model_name": table_name,
                "expected_columns": expected_cols,
            })

        scores = score_via_modal(items, compiler)

        for i, s in enumerate(scores):
            if len(results) <= i:
                results.append({"task_id": tasks[i]["instance_id"]})
            results[i][f"{compiler}_score"] = s["score"]
            results[i][f"{compiler}_compile"] = s.get("compile_ok", None)
            results[i][f"{compiler}_run"] = s.get("run_ok", None)

    return completions, results


def print_results(all_results):
    """Print comparison table."""
    print(f"\n{'='*80}")
    print("GAZELLE SINGLE-TURN EVAL RESULTS")
    print(f"{'='*80}\n")

    models = list(all_results.keys())

    # Per-task table
    header = f"{'Task':<30}"
    for model in models:
        header += f" | {model:>10} core {model:>10} fusion"
    print(header)
    print("-" * len(header))

    task_ids = [r["task_id"] for r in list(all_results.values())[0]]
    for tid in task_ids:
        row = f"{tid:<30}"
        for model in models:
            r = next((x for x in all_results[model] if x["task_id"] == tid), None)
            if r:
                core = r.get("dbt-core_score", -1)
                fusion = r.get("dbt-fusion_score", -1)
                cs = f"{core:.1f}" if core >= 0 else "ERR"
                fs = f"{fusion:.1f}" if fusion >= 0 else "ERR"
                div = " *" if core >= 0 and fusion >= 0 and abs(core - fusion) > 0.1 else "  "
                row += f" |    {cs:>4}       {fs:>4}{div}"
        print(row)

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY\n")
    for model in models:
        results = all_results[model]
        core_scores = [r["dbt-core_score"] for r in results if r.get("dbt-core_score", -1) >= 0]
        fusion_scores = [r["dbt-fusion_score"] for r in results if r.get("dbt-fusion_score", -1) >= 0]
        compile_core = sum(1 for r in results if r.get("dbt-core_compile"))
        compile_fusion = sum(1 for r in results if r.get("dbt-fusion_compile"))
        run_core = sum(1 for r in results if r.get("dbt-core_run"))
        run_fusion = sum(1 for r in results if r.get("dbt-fusion_run"))

        print(f"  {model}:")
        if core_scores:
            print(f"    dbt-core  mean: {sum(core_scores)/len(core_scores):.3f}  (compile: {compile_core}/{len(results)}, run: {run_core}/{len(results)})")
        if fusion_scores:
            print(f"    dbt-fusion mean: {sum(fusion_scores)/len(fusion_scores):.3f}  (compile: {compile_fusion}/{len(results)}, run: {run_fusion}/{len(results)})")
        print()


def main():
    parser = argparse.ArgumentParser(description="Single-turn eval using Gazelle/Spider2 tasks")
    parser.add_argument("--base-url", help="Base Qwen endpoint URL")
    parser.add_argument("--base-model", default="baseten-model", help="Base model name on endpoint")
    parser.add_argument("--fusion-url", help="Fusion-RL endpoint URL")
    parser.add_argument("--fusion-model", default="global_step_64/actor/lora_adapter")
    parser.add_argument("--core-url", help="Core-RL endpoint URL")
    parser.add_argument("--core-model", default="global_step_32/actor/lora_adapter")
    parser.add_argument("--num-tasks", type=int, default=20, help="Number of tasks to eval (default: 20)")
    parser.add_argument("--task-source", choices=["spider2", "llm-bench", "both"], default="both")
    parser.add_argument("--output", default="eval_results/gazelle_single_turn.json")
    args = parser.parse_args()

    api_key = BASETEN_API_KEY
    if not api_key:
        raise RuntimeError("BASETEN_API_KEY not set")

    # Load tasks
    tasks = []
    if args.task_source in ("spider2", "both"):
        tasks.extend(load_tasks(GAZELLE_TASKS_PATH))
    if args.task_source in ("llm-bench", "both"):
        tasks.extend(load_tasks(LLM_BENCH_TASKS_PATH))

    if not tasks:
        print("No tasks found. Check paths:")
        print(f"  Spider2: {GAZELLE_TASKS_PATH}")
        print(f"  LLM-bench: {LLM_BENCH_TASKS_PATH}")
        return

    tasks = tasks[:args.num_tasks]
    print(f"Loaded {len(tasks)} tasks for evaluation")

    # Build model configs
    models = {}
    if args.base_url:
        models["base"] = (args.base_url, args.base_model)
    if args.core_url:
        models["core-rl"] = (args.core_url, args.core_model)
    if args.fusion_url:
        models["fusion-rl"] = (args.fusion_url, args.fusion_model)

    if not models:
        parser.error("Provide at least one model endpoint")

    all_results = {}
    all_completions = {}
    for display_name, (url, model_name) in models.items():
        completions, results = run_model_eval(url, model_name, display_name, tasks, api_key)
        all_results[display_name] = results
        all_completions[display_name] = completions

    print_results(all_results)

    # Save
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "num_tasks": len(tasks),
        "task_source": args.task_source,
        "task_ids": [t["instance_id"] for t in tasks],
        "models": {k: {"url": v[0], "model": v[1]} for k, v in models.items()},
        "results": all_results,
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
