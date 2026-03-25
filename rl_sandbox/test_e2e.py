"""
End-to-end test: score all prompts with both dbt-core and dbt-fusion.

Generates a simple "baseline" completion for each prompt, scores it through
both compilers, and shows where they diverge. This validates the sandbox
and demonstrates the A/B signal difference.

Usage:
  python test_e2e.py
"""

import json
import time
from prompts import RL_PROMPTS
from reward import score_completion


# Simple completions that a mediocre model might generate.
# Some correct, some with issues — to show scoring spread.
BASELINE_COMPLETIONS = {
    "stg_customers": """```sql
WITH source AS (
    SELECT * FROM {{ source('jaffle_shop', 'raw_customers') }}
)
SELECT
    id AS customer_id,
    first_name,
    last_name,
    email,
    CAST(created_at AS date) AS created_at
FROM source
```""",
    "stg_orders": """```sql
SELECT
    id AS order_id,
    customer_id,
    CAST(order_date AS date) AS order_date,
    status,
    amount
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "stg_payments": """```sql
SELECT
    id AS payment_id,
    order_id,
    payment_method,
    amount AS payment_amount
FROM {{ source('jaffle_shop', 'raw_payments') }}
```""",
    "stg_orders_filtered": """```sql
SELECT *
FROM {{ source('jaffle_shop', 'raw_orders') }}
WHERE status != 'cancelled'
```""",
    "stg_customers_with_full_name": """```sql
SELECT
    id,
    first_name || ' ' || last_name AS full_name,
    email
FROM {{ source('jaffle_shop', 'raw_customers') }}
```""",
    "orders_with_customers": """```sql
SELECT
    o.id AS order_id,
    o.order_date,
    c.first_name,
    c.last_name,
    o.amount
FROM {{ source('jaffle_shop', 'raw_orders') }} o
JOIN {{ source('jaffle_shop', 'raw_customers') }} c ON o.customer_id = c.id
```""",
    "orders_with_payments": """```sql
SELECT
    o.id AS order_id,
    o.status,
    p.payment_method,
    p.amount AS payment_amount
FROM {{ source('jaffle_shop', 'raw_orders') }} o
JOIN {{ source('jaffle_shop', 'raw_payments') }} p ON o.id = p.order_id
```""",
    "customer_orders_summary": """```sql
SELECT
    c.id AS customer_id,
    c.first_name,
    COUNT(o.id) AS total_orders,
    SUM(o.amount) AS total_amount
FROM {{ source('jaffle_shop', 'raw_customers') }} c
LEFT JOIN {{ source('jaffle_shop', 'raw_orders') }} o ON c.id = o.customer_id
GROUP BY c.id, c.first_name
```""",
    "order_totals_by_status": """```sql
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM {{ source('jaffle_shop', 'raw_orders') }}
GROUP BY status
```""",
    "daily_order_summary": """```sql
SELECT
    order_date,
    COUNT(*) AS num_orders,
    SUM(amount) AS total_revenue
FROM {{ source('jaffle_shop', 'raw_orders') }}
GROUP BY order_date
```""",
    "payment_method_summary": """```sql
SELECT
    payment_method,
    COUNT(*) AS payment_count,
    SUM(amount) AS total_amount
FROM {{ source('jaffle_shop', 'raw_payments') }}
GROUP BY payment_method
```""",
    "top_customers": """```sql
SELECT
    c.first_name,
    c.last_name,
    SUM(o.amount) AS total_amount
FROM {{ source('jaffle_shop', 'raw_customers') }} c
JOIN {{ source('jaffle_shop', 'raw_orders') }} o ON c.id = o.customer_id
GROUP BY c.first_name, c.last_name
ORDER BY total_amount DESC
LIMIT 5
```""",
    "customer_first_order": """```sql
WITH orders AS (
    SELECT * FROM {{ source('jaffle_shop', 'raw_orders') }}
),
first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT
    o.customer_id,
    o.order_date,
    o.id AS order_id,
    o.amount
FROM orders o
JOIN first_orders f ON o.customer_id = f.customer_id AND o.order_date = f.first_order_date
```""",
    "orders_with_row_number": """```sql
SELECT
    id,
    customer_id,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS row_number
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "repeat_customers": """```sql
SELECT
    customer_id,
    COUNT(*) AS order_count
FROM {{ source('jaffle_shop', 'raw_orders') }}
GROUP BY customer_id
HAVING COUNT(*) > 1
```""",
    "dim_customers": """```sql
{{ config(materialized='table') }}

SELECT
    id AS customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM {{ source('jaffle_shop', 'raw_customers') }}
```""",
    "fct_orders": """```sql
{{ config(materialized='table') }}

SELECT
    id AS order_id,
    customer_id,
    CAST(order_date AS date) AS order_date,
    status,
    amount
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "incremental_orders": """```sql
{{ config(materialized='incremental', unique_key='id') }}

SELECT *
FROM {{ source('jaffle_shop', 'raw_orders') }}

{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```""",
    "typed_orders": """```sql
SELECT
    CAST(id AS INTEGER) AS id,
    customer_id,
    CAST(amount AS DECIMAL) AS amount,
    CAST(order_date AS DATE) AS order_date,
    status
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "payments_in_dollars": """```sql
SELECT
    id,
    order_id,
    payment_method,
    amount / 100.0 AS amount_dollars
FROM {{ source('jaffle_shop', 'raw_payments') }}
```""",
    "orders_with_defaults": """```sql
SELECT
    id,
    customer_id,
    COALESCE(amount, 0) AS amount,
    COALESCE(status, 'unknown') AS status,
    order_date
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "order_size_category": """```sql
SELECT
    id,
    amount,
    CASE
        WHEN amount < 1000 THEN 'small'
        WHEN amount BETWEEN 1000 AND 3000 THEN 'medium'
        ELSE 'large'
    END AS order_size
FROM {{ source('jaffle_shop', 'raw_orders') }}
```""",
    "payment_type_flag": """```sql
SELECT
    id,
    order_id,
    payment_method,
    CASE WHEN payment_method = 'credit_card' THEN true ELSE false END AS is_credit_card
FROM {{ source('jaffle_shop', 'raw_payments') }}
```""",
    "all_events": """```sql
SELECT id, customer_id, order_date AS event_date, 'order' AS event_type
FROM {{ source('jaffle_shop', 'raw_orders') }}
UNION ALL
SELECT id, order_id AS customer_id, created_at AS event_date, 'payment' AS event_type
FROM {{ source('jaffle_shop', 'raw_payments') }}
```""",
    "above_avg_orders": """```sql
SELECT id, customer_id, amount, order_date, status
FROM {{ source('jaffle_shop', 'raw_orders') }}
WHERE amount > (SELECT AVG(amount) FROM {{ source('jaffle_shop', 'raw_orders') }})
```""",
}


def run_test():
    results = {"dbt-core": [], "dbt-fusion": []}
    divergences = []

    for i, prompt in enumerate(RL_PROMPTS):
        name = prompt["model_name"]
        completion = BASELINE_COMPLETIONS.get(name)
        if not completion:
            print(f"  {i+1}. [{name}] SKIP — no baseline completion")
            continue

        # Score with both compilers
        core = score_completion(
            prompt["prompt"], completion, compiler="dbt-core",
            model_name=name, expected_columns=prompt.get("expected_columns"),
        )
        fusion = score_completion(
            prompt["prompt"], completion, compiler="dbt-fusion",
            model_name=name, expected_columns=prompt.get("expected_columns"),
        )

        results["dbt-core"].append(core["score"])
        results["dbt-fusion"].append(fusion["score"])

        marker = ""
        if core["score"] != fusion["score"]:
            marker = " <-- DIVERGENCE"
            divergences.append({
                "model": name,
                "core_score": core["score"],
                "fusion_score": fusion["score"],
                "core_errors": core["errors"],
                "fusion_errors": fusion["errors"],
            })

        print(f"  {i+1}. [{name}] core={core['score']:.1f} fusion={fusion['score']:.1f}{marker}")

    # Summary
    core_avg = sum(results["dbt-core"]) / len(results["dbt-core"])
    fusion_avg = sum(results["dbt-fusion"]) / len(results["dbt-fusion"])
    print(f"\n--- Summary ---")
    print(f"  Prompts tested: {len(results['dbt-core'])}")
    print(f"  dbt-core avg:   {core_avg:.2f}")
    print(f"  dbt-fusion avg: {fusion_avg:.2f}")
    print(f"  Divergences:    {len(divergences)}")

    if divergences:
        print(f"\n--- Divergences ---")
        for d in divergences:
            print(f"  {d['model']}: core={d['core_score']} fusion={d['fusion_score']}")
            if d["fusion_errors"]:
                print(f"    fusion error: {d['fusion_errors'][0][:120]}")


if __name__ == "__main__":
    start = time.time()
    run_test()
    print(f"\nDone in {time.time() - start:.1f}s")
