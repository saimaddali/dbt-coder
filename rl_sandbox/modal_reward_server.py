"""
Modal-based reward server for RL training.

Runs dbt compile/run/test in isolated sandboxes.
Exposes an HTTP endpoint that the Baseten training loop calls.

Usage:
  modal deploy modal_reward_server.py   # deploy as persistent endpoint
  modal run modal_reward_server.py      # test locally
"""

import modal

app = modal.App("dbt-coder-reward")

# Container image with dbt-core, dbt-fusion, DuckDB
sandbox_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("curl")
    .pip_install("dbt-core>=1.9.0", "dbt-duckdb>=1.9.0", "duckdb>=1.0.0",
                 "fastapi[standard]")
    .run_commands(
        # Install dbt-fusion (installs to ~/.local/bin/dbt)
        "curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update || true",
        # Rename fusion binary so it doesn't shadow dbt-core
        "mv /root/.local/bin/dbt /usr/local/bin/dbt-fusion || true",
    )
    .env({"DBT_CORE_BIN": "dbt", "DBT_FUSION_BIN": "/usr/local/bin/dbt-fusion"})
    .add_local_dir("dbt_project", remote_path="/sandbox/dbt_project", copy=True)
    .add_local_file("reward.py", remote_path="/sandbox/reward.py", copy=True)
)


@app.function(image=sandbox_image, timeout=120)
def score_single(prompt: str, completion: str, compiler: str = "dbt-core",
                 model_name: str = "generated_model",
                 expected_columns: list[str] = None,
                 expected_row_count: int = None,
                 unique_key: str = None,
                 schema_yml: str = None) -> dict:
    """Score a single completion in an isolated container."""
    import sys
    sys.path.insert(0, "/sandbox")
    from reward import score_completion

    return score_completion(
        prompt=prompt,
        completion=completion,
        compiler=compiler,
        model_name=model_name,
        expected_columns=expected_columns,
        expected_row_count=expected_row_count,
        unique_key=unique_key,
        schema_yml=schema_yml,
    )


@app.function(image=sandbox_image, timeout=300)
def score_batch(items: list[dict], compiler: str = "dbt-core") -> list[dict]:
    """Score a batch of completions sequentially in one container."""
    import sys
    sys.path.insert(0, "/sandbox")
    from reward import score_completion

    results = []
    for item in items:
        r = score_completion(
            prompt=item["prompt"],
            completion=item["completion"],
            compiler=compiler,
            model_name=item.get("model_name", "generated_model"),
            expected_columns=item.get("expected_columns"),
            expected_row_count=item.get("expected_row_count"),
            unique_key=item.get("unique_key"),
            schema_yml=item.get("schema_yml"),
        )
        results.append(r)
    return results


@app.function(image=sandbox_image, timeout=300)
def score_parallel(items: list[dict], compiler: str = "dbt-core") -> list[dict]:
    """Score completions in parallel — one sandbox per completion."""
    futures = []
    for item in items:
        futures.append(
            score_single.spawn(
                prompt=item["prompt"],
                completion=item["completion"],
                compiler=compiler,
                model_name=item.get("model_name", "generated_model"),
                expected_columns=item.get("expected_columns"),
                expected_row_count=item.get("expected_row_count"),
                unique_key=item.get("unique_key"),
                schema_yml=item.get("schema_yml"),
            )
        )
    return [f.get() for f in futures]


# HTTP endpoint for Baseten training loop to call
@app.function(image=sandbox_image, timeout=600)
@modal.fastapi_endpoint(method="POST")
def reward_endpoint(request: dict) -> dict:
    """
    HTTP endpoint for RL training.

    Request body:
    {
        "completions": [
            {"prompt": "...", "completion": "...", "model_name": "...",
             "expected_columns": [...], "expected_row_count": N,
             "unique_key": "col", "schema_yml": "..."}
        ],
        "compiler": "dbt-core" | "dbt-fusion"
    }

    Response:
    {
        "scores": [{"score": 0.8, "compile_ok": true, ...}, ...]
    }
    """
    completions = request.get("completions", [])
    compiler = request.get("compiler", "dbt-core")

    results = score_parallel.remote(completions, compiler)

    return {"scores": results}


# Local test
@app.local_entrypoint()
def main():
    import json

    test_items = [
        {
            "prompt": "Write a staging model for raw_orders",
            "completion": """```sql
WITH source AS (
    SELECT * FROM {{ source('jaffle_shop', 'raw_orders') }}
)
SELECT
    id AS order_id,
    customer_id,
    order_date,
    status,
    amount
FROM source
```""",
            "expected_columns": ["order_id", "customer_id"],
        },
        {
            "prompt": "Write a staging model for raw_orders",
            "completion": """```sql
SELECT * FROM nonexistent_table
```""",
        },
        {
            "prompt": "Write a model that joins orders and customers",
            "completion": """```sql
WITH orders AS (
    SELECT * FROM {{ source('jaffle_shop', 'raw_orders') }}
),
customers AS (
    SELECT * FROM {{ source('jaffle_shop', 'raw_customers') }}
)
SELECT
    o.id AS order_id,
    o.order_date,
    c.first_name,
    c.last_name,
    o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.id
```""",
            "expected_columns": ["order_id", "first_name"],
        },
    ]

    print("Scoring with dbt-core...")
    results = score_batch.remote(test_items, compiler="dbt-core")
    for item, result in zip(test_items, results):
        print(f"\n  Prompt: {item['prompt'][:60]}")
        print(f"  Score: {result['score']} | compile={result['compile_ok']} "
              f"run={result['run_ok']} columns={result['columns_ok']}")
        if result['errors']:
            print(f"  Errors: {result['errors']}")
