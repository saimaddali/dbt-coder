"""
Reward function for RL training of dbt code generation.

Takes a prompt and a model completion (SQL), writes it into a dbt project,
runs compile/run/test with either dbt-core or dbt-fusion, returns a score.

Scoring tiers:
  0.0 — Jinja/syntax error (doesn't compile)
  0.3 — Compiles (valid Jinja + SQL structure)
  0.6 — Compiles AND runs against DuckDB (produces rows)
  0.7 — Runs, has expected columns, BUT row count off or duplicates detected
  0.8 — Runs, expected columns, row count within tolerance, no duplicates
  1.0 — Full marks (all above + tests pass if provided)
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


PROJECT_TEMPLATE = Path(__file__).parent / "dbt_project"
PRE_SEEDED_DB = Path("/sandbox/pre_seeded.duckdb")


def extract_sql(completion: str) -> str:
    """Extract SQL from a completion that may include markdown fences."""
    # Try to extract from ```sql ... ``` blocks
    match = re.search(r"```sql\s*\n(.*?)```", completion, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try generic ``` blocks
    match = re.search(r"```\s*\n(.*?)```", completion, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Assume the whole thing is SQL
    return completion.strip()


def setup_sandbox(sql: str, model_name: str = "generated_model",
                  schema_yml: str = None) -> Path:
    """Copy the dbt project template to a temp dir and inject the generated SQL."""
    sandbox_dir = Path(tempfile.mkdtemp(prefix="dbt_sandbox_"))
    shutil.copytree(PROJECT_TEMPLATE, sandbox_dir / "project", dirs_exist_ok=True)

    project_dir = sandbox_dir / "project"

    # Write the generated model
    model_path = project_dir / "models" / f"{model_name}.sql"
    model_path.write_text(sql)

    # Optionally write a schema.yml for the generated model
    if schema_yml:
        schema_path = project_dir / "models" / f"{model_name}_schema.yml"
        schema_path.write_text(schema_yml)

    return project_dir


DBT_CORE_BIN = os.environ.get("DBT_CORE_BIN", "dbt")
DBT_FUSION_BIN = os.environ.get("DBT_FUSION_BIN", os.path.expanduser("~/.local/bin/dbt"))


def run_dbt_command(project_dir: Path, command: list[str],
                    compiler: str = "dbt-core") -> dict:
    """Run a dbt command and return result."""
    if compiler == "dbt-fusion":
        cmd = [DBT_FUSION_BIN] + command
    else:
        cmd = [DBT_CORE_BIN] + command

    cmd += ["--project-dir", str(project_dir),
            "--profiles-dir", str(project_dir)]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(project_dir),
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "stdout": "",
            "stderr": "TIMEOUT",
            "returncode": -1,
        }
    except FileNotFoundError:
        return {
            "success": False,
            "stdout": "",
            "stderr": f"{compiler} not found",
            "returncode": -1,
        }


def check_output_columns(project_dir: Path, model_name: str,
                         expected_columns: list[str] = None) -> bool:
    """Check if the model output has expected columns using DuckDB."""
    if not expected_columns:
        return True

    try:
        import duckdb
        db_path = "/tmp/sandbox.duckdb"
        conn = duckdb.connect(db_path, read_only=True)
        columns = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{model_name}'"
        ).fetchall()
        conn.close()

        actual_columns = {row[0].lower() for row in columns}
        expected_set = {c.lower() for c in expected_columns}
        return expected_set.issubset(actual_columns)
    except Exception:
        return False


def check_row_count(model_name: str, expected_row_count: int,
                    tolerance: float = 0.2) -> dict:
    """Check if model output row count is within tolerance of expected."""
    if expected_row_count is None:
        return {"ok": True, "actual": None, "expected": None, "reason": "no expectation"}
    try:
        import duckdb
        conn = duckdb.connect("/tmp/sandbox.duckdb", read_only=True)
        actual = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
        conn.close()
        lower = int(expected_row_count * (1 - tolerance))
        upper = int(expected_row_count * (1 + tolerance))
        ok = lower <= actual <= upper
        return {
            "ok": ok,
            "actual": actual,
            "expected": expected_row_count,
            "reason": None if ok else f"row count {actual} outside [{lower}, {upper}]",
        }
    except Exception as e:
        return {"ok": False, "actual": None, "expected": expected_row_count, "reason": str(e)}


def check_no_duplicates(model_name: str, unique_key: str) -> dict:
    """Check that unique_key has no duplicates in model output."""
    if not unique_key:
        return {"ok": True, "reason": "no unique_key specified"}
    try:
        import duckdb
        conn = duckdb.connect("/tmp/sandbox.duckdb", read_only=True)
        result = conn.execute(
            f"SELECT COUNT(*) AS total, COUNT(DISTINCT {unique_key}) AS distinct_count "
            f"FROM {model_name}"
        ).fetchone()
        conn.close()
        total, distinct = result
        ok = total == distinct
        return {
            "ok": ok,
            "total": total,
            "distinct": distinct,
            "reason": None if ok else f"duplicates on {unique_key}: {total} rows but {distinct} distinct",
        }
    except Exception as e:
        return {"ok": False, "total": None, "distinct": None, "reason": str(e)}


def score_completion(
    prompt: str,
    completion: str,
    compiler: str = "dbt-core",
    model_name: str = "generated_model",
    expected_columns: list[str] = None,
    expected_row_count: int = None,
    unique_key: str = None,
    schema_yml: str = None,
) -> dict:
    """
    Score a model completion by executing it in a dbt sandbox.

    Returns:
        dict with 'score' (0.0-1.0), 'compile_ok', 'run_ok',
        'columns_ok', 'test_ok', 'errors'
    """
    result = {
        "score": 0.0,
        "compile_ok": False,
        "run_ok": False,
        "columns_ok": False,
        "row_count_ok": False,
        "no_duplicates": False,
        "test_ok": False,
        "errors": [],
        "compiler": compiler,
    }

    # Extract SQL from completion
    sql = extract_sql(completion)
    if not sql:
        result["errors"].append("No SQL found in completion")
        return result

    # Set up sandbox
    project_dir = setup_sandbox(sql, model_name, schema_yml)

    try:
        # Set up DuckDB — use pre-seeded snapshot if available, otherwise seed
        db_path = Path("/tmp/sandbox.duckdb")
        if db_path.exists():
            db_path.unlink()

        if PRE_SEEDED_DB.exists():
            shutil.copy(PRE_SEEDED_DB, db_path)
        else:
            seed_result = run_dbt_command(project_dir, ["seed"], compiler="dbt-core")
            if not seed_result["success"]:
                result["errors"].append(f"Seed failed: {seed_result['stderr'][:200]}")
                return result

        # Level 1: Compile
        compile_result = run_dbt_command(
            project_dir,
            ["compile", "--select", model_name],
            compiler=compiler,
        )

        if not compile_result["success"]:
            result["errors"].append(
                f"Compile failed: {compile_result['stderr'][:200]}"
            )
            return result

        result["compile_ok"] = True
        result["score"] = 0.3

        # Level 2: Run
        run_result = run_dbt_command(
            project_dir,
            ["run", "--select", model_name],
            compiler="dbt-core",  # always run with dbt-core (DuckDB adapter)
        )

        if not run_result["success"]:
            result["errors"].append(
                f"Run failed: {run_result['stderr'][:200]}"
            )
            return result

        result["run_ok"] = True
        result["score"] = 0.6

        # Level 3: Check output columns
        if expected_columns:
            if check_output_columns(project_dir, model_name, expected_columns):
                result["columns_ok"] = True
            else:
                result["errors"].append("Output missing expected columns")
                return result
        else:
            result["columns_ok"] = True

        # Level 4: Check row count and duplicates
        row_check = check_row_count(model_name, expected_row_count)
        dup_check = check_no_duplicates(model_name, unique_key)
        result["row_count_ok"] = row_check["ok"]
        result["no_duplicates"] = dup_check["ok"]

        if not row_check["ok"]:
            result["errors"].append(f"Row count: {row_check['reason']}")
        if not dup_check["ok"]:
            result["errors"].append(f"Duplicates: {dup_check['reason']}")

        if result["row_count_ok"] and result["no_duplicates"]:
            result["score"] = 0.8
        else:
            # Columns match but row count or duplicates are wrong
            result["score"] = 0.7

        # Level 5: Run tests (if schema provided)
        if schema_yml and result["score"] >= 0.8:
            test_result = run_dbt_command(
                project_dir,
                ["test", "--select", model_name],
                compiler="dbt-core",
            )
            if test_result["success"]:
                result["test_ok"] = True
                result["score"] = 1.0
            else:
                result["errors"].append(
                    f"Tests failed: {test_result['stderr'][:200]}"
                )
        elif not schema_yml and result["score"] >= 0.8:
            result["score"] = 1.0

    finally:
        # Cleanup
        shutil.rmtree(project_dir.parent, ignore_errors=True)

    return result


def batch_score(
    prompts_and_completions: list[dict],
    compiler: str = "dbt-core",
) -> list[dict]:
    """Score a batch of completions. Used by the RL training loop."""
    results = []
    for item in prompts_and_completions:
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


if __name__ == "__main__":
    # Quick test
    test_prompt = "Write a staging model that selects from raw_orders"

    test_good = """
```sql
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
```
"""

    test_bad = """
```sql
SELECT * FROM nonexistent_table
```
"""

    print("=== Good completion ===")
    r = score_completion(test_prompt, test_good, compiler="dbt-core",
                         expected_columns=["order_id", "customer_id"])
    print(json.dumps(r, indent=2))

    print("\n=== Bad completion ===")
    r = score_completion(test_prompt, test_bad, compiler="dbt-core")
    print(json.dumps(r, indent=2))
