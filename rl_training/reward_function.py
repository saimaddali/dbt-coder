"""
VeRL-compatible reward function for dbt code generation.

VeRL calls compute_score(data_source, solution_str, ground_truth, extra_info)
for each completion. We extract the SQL from <answer> tags and score it.

Primary path: POST to Modal for parallel scoring in isolated containers.
Fallback path: run dbt locally (slow, sequential).

The DBT_COMPILER env var controls whether we use dbt-core or dbt-fusion for
the compile step. This is how we run the A/B experiment:
  - Model A training: DBT_COMPILER=dbt-core
  - Model B training: DBT_COMPILER=dbt-fusion
"""

import os
import re
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


COMPILER = os.environ.get("DBT_COMPILER", "dbt-core")
MODAL_URL = os.environ.get(
    "MODAL_REWARD_URL",
    "https://sai-maddali3--dbt-coder-reward-reward-endpoint.modal.run"
)
USE_MODAL = os.environ.get("USE_MODAL", "true").lower() in ("true", "1", "yes")

DBT_CORE_BIN = os.environ.get("DBT_CORE_BIN", "dbt")
DBT_FUSION_BIN = os.environ.get("DBT_FUSION_BIN", "/usr/local/bin/dbt-fusion")
_here = Path(__file__).parent
PROJECT_TEMPLATE = _here / "dbt_project" if (_here / "dbt_project").exists() else Path("/sandbox/dbt_project")

_modal_warned = False


def extract_answer(solution_str):
    """Extract SQL from <answer>...</answer> tags."""
    match = re.findall(r"<answer>(.*?)</answer>", solution_str, re.DOTALL)
    if len(match) != 1:
        return None
    sql = match[0].strip()
    fence_match = re.search(r"```(?:sql)?\s*\n(.*?)```", sql, re.DOTALL)
    if fence_match:
        sql = fence_match.group(1).strip()
    return sql if sql else None


def _score_via_modal(sql, model_name, expected_columns, expected_row_count,
                     unique_key, schema_yml):
    """Score a single completion via Modal HTTP endpoint."""
    import requests

    completion = f"```sql\n{sql}\n```"
    item = {
        "prompt": "",
        "completion": completion,
        "model_name": model_name,
        "expected_columns": expected_columns or [],
        "expected_row_count": expected_row_count,
        "unique_key": unique_key,
        "schema_yml": schema_yml or "",
    }
    resp = requests.post(
        MODAL_URL,
        json={"completions": [item], "compiler": COMPILER},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["scores"][0]["score"]


def setup_sandbox(sql, model_name="generated_model", schema_yml=None):
    sandbox_dir = Path(tempfile.mkdtemp(prefix="dbt_rl_"))
    shutil.copytree(PROJECT_TEMPLATE, sandbox_dir / "project", dirs_exist_ok=True)
    project_dir = sandbox_dir / "project"
    model_path = project_dir / "models" / f"{model_name}.sql"
    model_path.write_text(sql)
    if schema_yml:
        schema_path = project_dir / "models" / f"{model_name}_schema.yml"
        schema_path.write_text(schema_yml)
    return project_dir


def run_dbt(project_dir, command, use_fusion=False):
    bin_path = DBT_FUSION_BIN if use_fusion else DBT_CORE_BIN
    cmd = [bin_path] + command + [
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                                cwd=str(project_dir))
        return result.returncode == 0, result.stderr[:300]
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return False, str(e)[:200]


def check_columns(model_name, expected_columns):
    if not expected_columns:
        return True
    try:
        import duckdb
        conn = duckdb.connect("/tmp/sandbox.duckdb", read_only=True)
        cols = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{model_name}'"
        ).fetchall()
        conn.close()
        actual = {r[0].lower() for r in cols}
        return {c.lower() for c in expected_columns}.issubset(actual)
    except Exception:
        return False


def check_row_count_local(model_name, expected_row_count, tolerance=0.2):
    if expected_row_count is None:
        return True
    try:
        import duckdb
        conn = duckdb.connect("/tmp/sandbox.duckdb", read_only=True)
        actual = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
        conn.close()
        lower = int(expected_row_count * (1 - tolerance))
        upper = int(expected_row_count * (1 + tolerance))
        return lower <= actual <= upper
    except Exception:
        return False


def check_no_duplicates_local(model_name, unique_key):
    if not unique_key:
        return True
    try:
        import duckdb
        conn = duckdb.connect("/tmp/sandbox.duckdb", read_only=True)
        total, distinct = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {unique_key}) FROM {model_name}"
        ).fetchone()
        conn.close()
        return total == distinct
    except Exception:
        return False


def _score_locally(sql, model_name, expected_columns, expected_row_count,
                   unique_key, schema_yml):
    """Score by running dbt locally (fallback)."""
    project_dir = setup_sandbox(sql, model_name, schema_yml or None)
    try:
        db_path = Path("/tmp/sandbox.duckdb")
        if db_path.exists():
            db_path.unlink()

        ok, err = run_dbt(project_dir, ["seed"], use_fusion=False)
        if not ok:
            return 0.0

        use_fusion = (COMPILER == "dbt-fusion")
        ok, err = run_dbt(project_dir, ["compile", "--select", model_name],
                          use_fusion=use_fusion)
        if not ok:
            return 0.0

        score = 0.3

        ok, err = run_dbt(project_dir, ["run", "--select", model_name],
                          use_fusion=False)
        if not ok:
            return score

        score = 0.6

        if expected_columns:
            if not check_columns(model_name, expected_columns):
                return score

        row_ok = check_row_count_local(model_name, expected_row_count)
        dup_ok = check_no_duplicates_local(model_name, unique_key)

        if row_ok and dup_ok:
            score = 0.8
        else:
            score = 0.7

        if schema_yml and score >= 0.8:
            ok, err = run_dbt(project_dir, ["test", "--select", model_name],
                              use_fusion=False)
            if ok:
                score = 1.0
        elif not schema_yml and score >= 0.8:
            score = 1.0

        return score
    finally:
        shutil.rmtree(project_dir.parent, ignore_errors=True)


def compute_score(data_source, solution_str, ground_truth, extra_info=None):
    """
    VeRL reward function interface.

    Returns float score 0.0-1.0:
      0.0 — no valid SQL or doesn't compile
      0.3 — compiles (valid Jinja + SQL)
      0.6 — compiles AND runs on DuckDB
      0.7 — runs, has columns, but row count off or duplicates
      0.8 — runs, columns match, row count ok, no duplicates
      1.0 — full marks (all checks pass + tests if provided)
    """
    global _modal_warned

    extra = extra_info or {}
    model_name = extra.get("model_name", "generated_model")
    expected_columns = extra.get("expected_columns", [])
    expected_row_count = extra.get("expected_row_count")
    unique_key = extra.get("unique_key")
    schema_yml = extra.get("schema_yml", "")

    sql = extract_answer(solution_str)
    if sql is None:
        return 0.0

    # Primary path: Modal (fast, isolated containers)
    if USE_MODAL:
        try:
            score = _score_via_modal(sql, model_name, expected_columns,
                                     expected_row_count, unique_key, schema_yml)
            return float(score)
        except Exception as e:
            if not _modal_warned:
                print(f"[reward] Modal failed, falling back to local: {e}")
                _modal_warned = True

    # Fallback: local dbt execution (slow but always works)
    return _score_locally(sql, model_name, expected_columns,
                          expected_row_count, unique_key, schema_yml)
