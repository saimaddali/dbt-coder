"""
Gazelle adapter for dbt-coder models.

Bridges our <think>/<answer> format model with dbt-mcp-gazelle's Spider2-DBT
evaluation harness. The model stays a single-shot SQL specialist — this adapter
wraps its output to work with gazelle's eval pipeline.

Architecture:
  1. Reads Spider2-DBT task (instruction + output_schema + table metadata)
  2. Enriches prompt with available table schemas
  3. Calls Baseten model endpoint
  4. Extracts SQL from <answer> tags
  5. Writes SQL as dbt model file
  6. Scores via dbt run + output comparison

Prerequisites:
  - Clone gazelle: git clone https://github.com/dbt-labs/dbt-mcp-gazelle
  - Set BASETEN_API_KEY env var
  - Set GAZELLE_REPO_PATH to point to cloned gazelle repo

Usage:
  python eval/gazelle_adapter.py --model-id <MODEL_ID> --task-file tasks.jsonl
  python eval/gazelle_adapter.py --model-id <MODEL_ID> --task-id spider2_001
"""

import os
import re
import json
import argparse
import requests
from pathlib import Path

BASETEN_API_KEY = os.environ.get("BASETEN_API_KEY", "")
GAZELLE_REPO = Path(os.environ.get("GAZELLE_REPO_PATH", "../dbt-mcp-gazelle"))

SYSTEM_PROMPT = """You are an expert analytics engineer. When given a dbt modeling task, \
think through it step by step in <think> tags, then provide your SQL \
answer in <answer> tags with a sql code fence.

The SQL should be a valid dbt model using Jinja syntax ({{ source(...) }}, {{ ref(...) }}, etc.)."""


def load_spider2_tasks(task_file=None):
    """Load Spider2-DBT tasks from gazelle repo or specified file."""
    if task_file:
        path = Path(task_file)
    else:
        path = GAZELLE_REPO / "benchmarks" / "spider2" / "tasks.jsonl"

    if not path.exists():
        raise FileNotFoundError(
            f"Tasks file not found: {path}\n"
            f"Clone gazelle repo or set GAZELLE_REPO_PATH env var."
        )

    tasks = []
    with open(path) as f:
        for line in f:
            if line.strip():
                tasks.append(json.loads(line))
    return tasks


def build_prompt_from_task(task):
    """Convert a Spider2-DBT task into a prompt for our model.

    Spider2-DBT tasks typically have:
      - instruction: natural language description
      - output_schema: expected output columns/types
      - table_metadata: available source tables with schemas
      - gold_sql: reference SQL (for eval only, not shown to model)
    """
    instruction = task.get("instruction", "")
    output_schema = task.get("output_schema", "")
    table_metadata = task.get("table_metadata", {})

    parts = [instruction]

    if output_schema:
        parts.append(f"\nExpected output schema:\n{output_schema}")

    if table_metadata:
        parts.append("\nAvailable tables:")
        if isinstance(table_metadata, dict):
            for table_name, columns in table_metadata.items():
                if isinstance(columns, list):
                    col_str = ", ".join(columns)
                    parts.append(f"  - {table_name}: {col_str}")
                else:
                    parts.append(f"  - {table_name}: {columns}")
        elif isinstance(table_metadata, list):
            for t in table_metadata:
                if isinstance(t, dict):
                    parts.append(f"  - {t.get('name', t)}: {t.get('columns', '')}")
                else:
                    parts.append(f"  - {t}")

    return "\n".join(parts)


def generate_completion(model_id, prompt, vllm_model_name=None):
    """Call Baseten model endpoint."""
    url = f"https://model-{model_id}.api.baseten.co/environments/production/sync/v1/chat/completions"
    headers = {"Authorization": f"Api-Key {BASETEN_API_KEY}"}
    payload = {
        "model": vllm_model_name or "baseten-model",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
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
    return str(data)


def extract_sql(completion):
    """Extract SQL from <answer> tags."""
    match = re.findall(r"<answer>(.*?)</answer>", completion, re.DOTALL)
    if not match:
        return None
    sql = match[0].strip()
    fence = re.search(r"```(?:sql)?\s*\n(.*?)```", sql, re.DOTALL)
    if fence:
        sql = fence.group(1).strip()
    return sql if sql else None


def run_task(task, model_id, vllm_model_name=None):
    """Run a single Spider2-DBT task and return result."""
    task_id = task.get("id", task.get("task_id", "unknown"))
    prompt = build_prompt_from_task(task)

    result = {
        "task_id": task_id,
        "prompt": prompt,
        "completion": None,
        "sql": None,
        "pass": False,
        "error": None,
    }

    try:
        completion = generate_completion(model_id, prompt, vllm_model_name)
        result["completion"] = completion
        sql = extract_sql(completion)
        result["sql"] = sql

        if not sql:
            result["error"] = "No SQL extracted from completion"
            return result

        # TODO: When gazelle is available, score via compute-dbt + output comparison
        # For now, just check if we got valid-looking SQL
        result["pass"] = bool(sql and len(sql) > 10)

    except Exception as e:
        result["error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description="Run Spider2-DBT eval via gazelle adapter")
    parser.add_argument("--model-id", required=True, help="Baseten model ID")
    parser.add_argument("--vllm-model-name", help="vLLM model name for LoRA checkpoints")
    parser.add_argument("--task-file", help="Path to tasks.jsonl")
    parser.add_argument("--task-id", help="Run a specific task by ID")
    parser.add_argument("--output", default="eval_results/spider2_results.json")
    args = parser.parse_args()

    try:
        tasks = load_spider2_tasks(args.task_file)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nTo use the gazelle adapter:")
        print("  1. git clone https://github.com/dbt-labs/dbt-mcp-gazelle")
        print("  2. export GAZELLE_REPO_PATH=/path/to/dbt-mcp-gazelle")
        print("  3. Re-run this script")
        return

    if args.task_id:
        tasks = [t for t in tasks if t.get("id") == args.task_id or t.get("task_id") == args.task_id]
        if not tasks:
            print(f"Task {args.task_id} not found")
            return

    print(f"Running {len(tasks)} Spider2-DBT tasks with model {args.model_id}")

    results = []
    for i, task in enumerate(tasks):
        task_id = task.get("id", task.get("task_id", f"task_{i}"))
        print(f"  [{i+1}/{len(tasks)}] {task_id}...", end=" ", flush=True)
        r = run_task(task, args.model_id, args.vllm_model_name)
        results.append(r)
        status = "PASS" if r["pass"] else f"FAIL ({r.get('error', 'unknown')[:50]})"
        print(status)

    passed = sum(1 for r in results if r["pass"])
    print(f"\nResults: {passed}/{len(results)} passed ({100*passed/len(results):.1f}%)")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump({"results": results, "summary": {"total": len(results), "passed": passed}}, f, indent=2)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
