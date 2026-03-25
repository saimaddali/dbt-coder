"""
Filter Spider2-DBT tasks for single-shot SQL generation eval.

Spider2-DBT has ~73 tasks. Not all are suitable for our single-shot SQL model:
- Some require multi-file changes (config, schema.yml)
- Some are debugging tasks (fix existing SQL)
- Some require tool use (meta.search, data.query)

This script categorizes tasks and outputs a filtered list for eval.

Usage:
  python eval/spider2_task_filter.py --task-file path/to/tasks.jsonl
  python eval/spider2_task_filter.py  # uses default gazelle path
"""

import os
import json
import argparse
from pathlib import Path

GAZELLE_REPO = Path(os.environ.get("GAZELLE_REPO_PATH", "../dbt-mcp-gazelle"))

# Keywords that indicate task types
SQL_GENERATION_KEYWORDS = [
    "create a model", "write a model", "build a model", "add a model",
    "create a staging", "write a query", "build a query",
    "select", "join", "aggregate", "calculate",
]

DEBUG_KEYWORDS = [
    "fix", "debug", "error", "broken", "failing", "incorrect",
]

CONFIG_KEYWORDS = [
    "schema.yml", "dbt_project.yml", "sources.yml", "properties",
    "add a test", "configure", "set up",
]

MULTI_FILE_KEYWORDS = [
    "multiple models", "create two", "refactor", "split",
]


def classify_task(task):
    """Classify a Spider2-DBT task by type."""
    instruction = task.get("instruction", "").lower()

    if any(kw in instruction for kw in DEBUG_KEYWORDS):
        return "debug"
    if any(kw in instruction for kw in CONFIG_KEYWORDS):
        return "config"
    if any(kw in instruction for kw in MULTI_FILE_KEYWORDS):
        return "multi_file"
    if any(kw in instruction for kw in SQL_GENERATION_KEYWORDS):
        return "sql_generation"
    return "other"


def filter_tasks(tasks):
    """Filter to tasks suitable for single-shot SQL generation."""
    categories = {}
    filtered = []

    for task in tasks:
        category = classify_task(task)
        categories.setdefault(category, []).append(task)
        if category == "sql_generation":
            filtered.append(task)

    return filtered, categories


def main():
    parser = argparse.ArgumentParser(description="Filter Spider2-DBT tasks")
    parser.add_argument("--task-file", help="Path to tasks.jsonl")
    parser.add_argument("--output", default="eval/filtered_tasks.jsonl")
    args = parser.parse_args()

    task_path = Path(args.task_file) if args.task_file else GAZELLE_REPO / "benchmarks" / "spider2" / "tasks.jsonl"

    if not task_path.exists():
        print(f"Tasks file not found: {task_path}")
        print("\nTo use this script:")
        print("  1. git clone https://github.com/dbt-labs/dbt-mcp-gazelle")
        print("  2. export GAZELLE_REPO_PATH=/path/to/dbt-mcp-gazelle")
        return

    tasks = []
    with open(task_path) as f:
        for line in f:
            if line.strip():
                tasks.append(json.loads(line))

    filtered, categories = filter_tasks(tasks)

    print(f"Total tasks: {len(tasks)}")
    print(f"\nBy category:")
    for cat, cat_tasks in sorted(categories.items(), key=lambda x: -len(x[1])):
        print(f"  {cat}: {len(cat_tasks)}")
        for t in cat_tasks[:3]:
            tid = t.get("id", t.get("task_id", "?"))
            instr = t.get("instruction", "")[:80]
            print(f"    - {tid}: {instr}...")

    print(f"\nFiltered for single-shot SQL generation: {len(filtered)} tasks")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        for task in filtered:
            f.write(json.dumps(task) + "\n")
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
