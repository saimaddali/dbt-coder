"""
Prompt dataset for RL training.

Each prompt has:
- prompt: the instruction
- model_name: what the generated model file should be called
- expected_columns: columns the output must have (for scoring)

These are prompts ONLY — no answers. The model generates, the sandbox scores.

Schema (jaffle_shop):
  raw_customers: id, first_name, last_name, email, phone, created_at, updated_at,
                 is_active, customer_type, referral_source
  raw_orders:    id, customer_id, order_date, status, amount, shipping_method,
                 discount_amount, tax_amount, currency, channel, notes
  raw_payments:  id, order_id, payment_method, amount, status, created_at, updated_at,
                 processor_id, is_refund
  raw_products:  id, name, category, subcategory, unit_price, cost, is_active,
                 created_at, updated_at, weight_kg, sku
  raw_order_items: id, order_id, product_id, quantity, unit_price, discount_pct, created_at
  raw_addresses: id, customer_id, address_type, street, city, state, zip_code,
                 country, is_default, created_at, updated_at
  raw_refunds:   id, order_id, reason, refund_amount, status, requested_at,
                 processed_at, processed_by
"""

RL_PROMPTS = [
    # ==========================================================
    # STAGING LAYER — source → stg (rename, cast, clean)
    # ==========================================================
    {
        "prompt": "Write a dbt staging model called `stg_customers` that selects from {{ source('jaffle_shop', 'raw_customers') }}. Rename `id` to `customer_id`, cast `created_at` to date as `signup_date`, and select first_name, last_name, email, phone, is_active, customer_type, and referral_source.",
        "model_name": "stg_customers",
        "expected_columns": ["customer_id", "first_name", "last_name", "email", "signup_date", "customer_type"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_orders` that selects from {{ source('jaffle_shop', 'raw_orders') }}. Rename `id` to `order_id`, cast `order_date` to date, cast `amount` to decimal and rename it `order_amount_cents`, and include customer_id, status, shipping_method, discount_amount, tax_amount, currency, and channel.",
        "model_name": "stg_orders",
        "expected_columns": ["order_id", "customer_id", "status", "order_amount_cents", "channel"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_payments` that selects from {{ source('jaffle_shop', 'raw_payments') }}. Rename `id` to `payment_id`, cast `created_at` to date as `payment_date`, filter out rows where is_refund is true, and include order_id, payment_method, amount as payment_amount, and status.",
        "model_name": "stg_payments",
        "expected_columns": ["payment_id", "order_id", "payment_method", "payment_amount", "payment_date"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_products` that selects from {{ source('jaffle_shop', 'raw_products') }}. Rename `id` to `product_id`, convert `unit_price` and `cost` from cents to dollars by dividing by 100.0 (naming them `price_dollars` and `cost_dollars`), and calculate `margin_dollars` as price_dollars - cost_dollars. Include name, category, subcategory, is_active, weight_kg, and sku.",
        "model_name": "stg_products",
        "expected_columns": ["product_id", "name", "category", "price_dollars", "cost_dollars", "margin_dollars"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_order_items` that selects from {{ source('jaffle_shop', 'raw_order_items') }}. Rename `id` to `order_item_id`, calculate `line_total` as quantity * unit_price, calculate `discount_amount` as line_total * discount_pct / 100, and calculate `net_amount` as line_total - discount_amount. Include order_id and product_id.",
        "model_name": "stg_order_items",
        "expected_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_total", "net_amount"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_addresses` that selects from {{ source('jaffle_shop', 'raw_addresses') }}. Rename `id` to `address_id`, include customer_id, address_type, city, state, zip_code, country, is_default, and cast created_at to date as `created_date`.",
        "model_name": "stg_addresses",
        "expected_columns": ["address_id", "customer_id", "address_type", "city", "state", "is_default"],
    },
    {
        "prompt": "Write a dbt staging model called `stg_refunds` that selects from {{ source('jaffle_shop', 'raw_refunds') }}. Rename `id` to `refund_id`, convert `refund_amount` from cents to dollars by dividing by 100.0 as `refund_amount_dollars`, include order_id, reason, status, and cast requested_at to date as `requested_date`.",
        "model_name": "stg_refunds",
        "expected_columns": ["refund_id", "order_id", "reason", "status", "refund_amount_dollars", "requested_date"],
    },
    {
        "prompt": "Write a staging model called `stg_active_customers` that selects from {{ source('jaffle_shop', 'raw_customers') }} where is_active is true. Rename id to customer_id and concatenate first_name and last_name as full_name.",
        "model_name": "stg_active_customers",
        "expected_columns": ["customer_id", "full_name", "email", "customer_type"],
    },
    {
        "prompt": "Write a staging model called `stg_completed_orders` that selects from {{ source('jaffle_shop', 'raw_orders') }} where status = 'completed'. Rename id to order_id, convert amount to dollars as `order_amount_dollars` (divide by 100.0).",
        "model_name": "stg_completed_orders",
        "expected_columns": ["order_id", "customer_id", "order_amount_dollars"],
    },
    {
        "prompt": "Write a staging model called `stg_successful_payments` that selects from {{ source('jaffle_shop', 'raw_payments') }} where status = 'success' and is_refund = false. Rename id to payment_id, convert amount to dollars as `amount_dollars`.",
        "model_name": "stg_successful_payments",
        "expected_columns": ["payment_id", "order_id", "payment_method", "amount_dollars"],
    },
    {
        "prompt": "Write a staging model called `stg_active_products` that selects from {{ source('jaffle_shop', 'raw_products') }} where is_active is true. Rename id to product_id, convert unit_price to dollars as `price`, and include name, category, subcategory, sku.",
        "model_name": "stg_active_products",
        "expected_columns": ["product_id", "name", "category", "price", "sku"],
    },
    {
        "prompt": "Write a staging model called `stg_refund_requests` that selects from {{ source('jaffle_shop', 'raw_refunds') }} where status != 'denied'. Rename id to refund_id, include order_id, reason, refund_amount, and requested_at.",
        "model_name": "stg_refund_requests",
        "expected_columns": ["refund_id", "order_id", "reason", "refund_amount"],
    },
    {
        "prompt": "Write a staging model called `stg_default_addresses` that selects from {{ source('jaffle_shop', 'raw_addresses') }} where is_default = true and address_type = 'shipping'. Rename id to address_id, include customer_id, city, state, zip_code.",
        "model_name": "stg_default_addresses",
        "expected_columns": ["address_id", "customer_id", "city", "state"],
    },
    {
        "prompt": "Write a staging model called `stg_customers_with_domain` that selects from {{ source('jaffle_shop', 'raw_customers') }}. Rename id to customer_id, extract the email domain (everything after '@') as `email_domain`, and include first_name, last_name, customer_type.",
        "model_name": "stg_customers_with_domain",
        "expected_columns": ["customer_id", "first_name", "email_domain", "customer_type"],
    },
    {
        "prompt": "Write a staging model called `stg_orders_with_year_month` that selects from {{ source('jaffle_shop', 'raw_orders') }}. Rename id to order_id, extract `order_year` and `order_month` from order_date, include customer_id, status, amount, channel.",
        "model_name": "stg_orders_with_year_month",
        "expected_columns": ["order_id", "customer_id", "order_year", "order_month", "channel"],
    },

    # ==========================================================
    # INTERMEDIATE LAYER — joins, business logic, denormalization
    # ==========================================================
    {
        "prompt": "Write an intermediate dbt model called `int_orders_with_items` that joins {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_order_items') }} on order id. Group by order_id and calculate: total_items (sum of quantity), total_line_amount (sum of quantity * unit_price), and item_count (count of distinct product_id).",
        "model_name": "int_orders_with_items",
        "expected_columns": ["order_id", "total_items", "total_line_amount", "item_count"],
    },
    {
        "prompt": "Write an intermediate model called `int_order_items_enriched` that joins {{ source('jaffle_shop', 'raw_order_items') }} with {{ source('jaffle_shop', 'raw_products') }} on product_id. Include order_item_id (from raw_order_items.id), order_id, product_name (from products.name), category, quantity, unit_price, and calculate line_total as quantity * unit_price.",
        "model_name": "int_order_items_enriched",
        "expected_columns": ["order_id", "product_name", "category", "quantity", "line_total"],
    },
    {
        "prompt": "Write an intermediate model called `int_customer_orders` that joins {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_orders') }} on customer_id. Include customer_id, first_name, last_name, customer_type, order_id (from raw_orders.id), order_date, status, amount, and channel.",
        "model_name": "int_customer_orders",
        "expected_columns": ["customer_id", "first_name", "customer_type", "order_id", "order_date", "status", "amount"],
    },
    {
        "prompt": "Write an intermediate model called `int_order_payments` that joins {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_payments') }} on order id, filtering payments where is_refund = false and status = 'success'. Group by order_id, order status, order amount. Calculate total_paid (sum of payment amounts) and payment_count.",
        "model_name": "int_order_payments",
        "expected_columns": ["order_id", "total_paid", "payment_count"],
    },
    {
        "prompt": "Write an intermediate model called `int_customer_addresses` that joins {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_addresses') }} where is_default = true and address_type = 'shipping'. Include customer_id, first_name, last_name, city, state, zip_code.",
        "model_name": "int_customer_addresses",
        "expected_columns": ["customer_id", "first_name", "city", "state"],
    },
    {
        "prompt": "Write an intermediate model called `int_order_refunds` that LEFT JOINs {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_refunds') }} on order_id. Include order_id (from raw_orders.id), customer_id, order amount, refund_amount (COALESCE to 0), refund_reason, refund_status, and a boolean `has_refund` (true when refund id is not null).",
        "model_name": "int_order_refunds",
        "expected_columns": ["order_id", "customer_id", "has_refund"],
    },
    {
        "prompt": "Write an intermediate model called `int_product_order_summary` that joins {{ source('jaffle_shop', 'raw_order_items') }} with {{ source('jaffle_shop', 'raw_products') }} on product_id. Group by product_id, product name, and category. Calculate total_quantity_sold, total_revenue (sum of quantity * unit_price), and num_orders (count distinct order_id).",
        "model_name": "int_product_order_summary",
        "expected_columns": ["product_id", "total_quantity_sold", "total_revenue", "num_orders"],
    },
    {
        "prompt": "Write an intermediate model called `int_orders_enriched` using CTEs. CTE1: raw_orders. CTE2: aggregate payments per order (total_paid, payment_count) from raw_payments where is_refund=false and status='success'. CTE3: aggregate refunds per order (total_refunded) from raw_refunds where status='approved'. Final: join all CTEs. Include order_id, customer_id, order_date, status, amount, total_paid, total_refunded (COALESCE to 0), and net_amount (amount - total_refunded).",
        "model_name": "int_orders_enriched",
        "expected_columns": ["order_id", "customer_id", "total_paid", "net_amount"],
    },
    {
        "prompt": "Write an intermediate model called `int_customer_order_items` that joins raw_customers, raw_orders, raw_order_items, and raw_products (all using {{ source('jaffle_shop', ...) }}). This is a 4-table join. Include customer_id, first_name, order_id, product_name, category, quantity, and unit_price.",
        "model_name": "int_customer_order_items",
        "expected_columns": ["customer_id", "first_name", "order_id", "product_name", "quantity"],
    },
    {
        "prompt": "Write an intermediate model called `int_order_channel_summary` that reads from {{ source('jaffle_shop', 'raw_orders') }} and groups by channel. Calculate order_count, total_revenue (sum of amount), avg_order_value (avg of amount), and pct_completed (count where status='completed' / total count * 100).",
        "model_name": "int_order_channel_summary",
        "expected_columns": ["channel", "order_count", "total_revenue", "avg_order_value"],
    },
    {
        "prompt": "Write an intermediate model called `int_payment_reconciliation` that joins {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_payments') }} (where is_refund=false and status='success'). Group by order_id and order amount. Calculate total_payments. Add is_fully_paid (boolean: total_payments >= order amount) and payment_gap (order amount - total_payments).",
        "model_name": "int_payment_reconciliation",
        "expected_columns": ["order_id", "is_fully_paid", "payment_gap"],
    },
    {
        "prompt": "Write an intermediate model called `int_category_performance` that joins {{ source('jaffle_shop', 'raw_order_items') }} with {{ source('jaffle_shop', 'raw_products') }} on product_id. Group by category. Calculate total_items_sold, total_revenue, unique_products (count distinct product_id), and avg_price (avg unit_price from products).",
        "model_name": "int_category_performance",
        "expected_columns": ["category", "total_items_sold", "total_revenue", "unique_products"],
    },

    # ==========================================================
    # MART LAYER — final business-facing models
    # ==========================================================
    {
        "prompt": "Write a mart model called `fct_orders` with {{ config(materialized='table') }}. Use CTEs to stage raw_orders, join with aggregated payments (sum from raw_payments where is_refund=false, status='success') and aggregated items (count, sum of quantity*price from raw_order_items). Final output: order_id, customer_id, order_date, status, order_amount (raw amount), total_paid, total_items, item_count, shipping_method, channel, discount_amount, tax_amount.",
        "model_name": "fct_orders",
        "expected_columns": ["order_id", "customer_id", "order_date", "status", "total_paid", "channel"],
    },
    {
        "prompt": "Write a mart model called `dim_customers` with {{ config(materialized='table') }}. Use CTEs. CTE1: raw_customers. CTE2: aggregate orders per customer (count, sum, min date, max date) from raw_orders. CTE3: default shipping address from raw_addresses. Final: join all. Include customer_id, full_name (first+last), email, phone, customer_type, referral_source, is_active, signup_date, total_orders, total_spent, first_order_date, last_order_date, city, state.",
        "model_name": "dim_customers",
        "expected_columns": ["customer_id", "full_name", "email", "customer_type", "total_orders", "total_spent"],
    },
    {
        "prompt": "Write a mart model called `dim_products` with {{ config(materialized='table') }}. Use CTEs. CTE1: raw_products with unit_price/100.0 as price_dollars, cost/100.0 as cost_dollars. CTE2: aggregate order_items per product (total_quantity, total_revenue, num_orders). Final: join. Include product_id, name, category, subcategory, sku, price_dollars, cost_dollars, margin_pct ((price-cost)/price*100), is_active, total_quantity_sold, total_revenue, num_orders.",
        "model_name": "dim_products",
        "expected_columns": ["product_id", "name", "category", "price_dollars", "total_quantity_sold"],
    },
    {
        "prompt": "Write a mart model called `fct_order_items` with {{ config(materialized='table') }}. Join raw_order_items with raw_products and raw_orders. Include order_item_id, order_id, product_id, product_name, category, quantity, unit_price_dollars (unit_price/100.0), line_total_dollars (quantity * unit_price/100.0), discount_pct, discount_amount_dollars, net_amount_dollars, order_date, order_status.",
        "model_name": "fct_order_items",
        "expected_columns": ["order_item_id", "order_id", "product_name", "category", "quantity"],
    },
    {
        "prompt": "Write a mart model called `fct_payments` with {{ config(materialized='table') }}. Select from raw_payments. Rename id to payment_id, convert amount to dollars, cast created_at to date as payment_date, include order_id, payment_method, status, processor_id, is_refund. Add payment_type column: 'refund' when is_refund=true, 'charge' otherwise.",
        "model_name": "fct_payments",
        "expected_columns": ["payment_id", "order_id", "payment_method", "payment_type"],
    },
    {
        "prompt": "Write a mart model called `customer_lifetime_value` with {{ config(materialized='table') }}. Join raw_customers with raw_orders. For each customer calculate: total_orders, total_revenue, avg_order_value, first_order_date, last_order_date, customer_tenure_days (last - first order), and ltv_segment ('whale' if total_revenue > 500000, 'regular' if > 100000, 'low_value' otherwise). Include customer_id, first_name, last_name, customer_type.",
        "model_name": "customer_lifetime_value",
        "expected_columns": ["customer_id", "first_name", "total_orders", "total_revenue", "ltv_segment"],
    },
    {
        "prompt": "Write a mart model called `daily_revenue` with {{ config(materialized='table') }}. Read from raw_orders. Group by order_date. Calculate num_orders, total_revenue, avg_order_value, num_unique_customers (count distinct customer_id), and add running_total_revenue as a window function (sum over order_date).",
        "model_name": "daily_revenue",
        "expected_columns": ["order_date", "num_orders", "total_revenue", "avg_order_value", "num_unique_customers"],
    },
    {
        "prompt": "Write a mart model called `product_performance` with {{ config(materialized='table') }}. Join raw_order_items with raw_products. For each product calculate: total_units_sold, gross_revenue (sum qty * unit_price), num_orders, avg_quantity_per_order. Also join with raw_orders to get revenue_from_completed (only where order status='completed'). Include product_id, name, category, subcategory.",
        "model_name": "product_performance",
        "expected_columns": ["product_id", "name", "category", "total_units_sold", "gross_revenue"],
    },
    {
        "prompt": "Write a mart model called `channel_performance` with {{ config(materialized='table') }}. Read from raw_orders. Group by channel. Calculate total_orders, total_revenue, avg_order_value, completed_orders (count where status='completed'), completion_rate (completed/total * 100), and avg_discount (avg discount_amount).",
        "model_name": "channel_performance",
        "expected_columns": ["channel", "total_orders", "total_revenue", "completion_rate"],
    },
    {
        "prompt": "Write a mart model called `customer_360` with {{ config(materialized='table') }}. This is the most comprehensive customer model. Use CTEs for: (1) customer base from raw_customers, (2) order aggregates from raw_orders, (3) payment aggregates from raw_payments (non-refund, success), (4) favorite product category from raw_order_items + raw_products (most purchased category), (5) default shipping address from raw_addresses. Final output: customer_id, full_name, email, customer_type, referral_source, signup_date, is_active, total_orders, total_spent, avg_order_value, first_order_date, last_order_date, city, state.",
        "model_name": "customer_360",
        "expected_columns": ["customer_id", "full_name", "email", "customer_type", "total_orders", "total_spent"],
    },

    # ==========================================================
    # AGGREGATIONS
    # ==========================================================
    {
        "prompt": "Write a model called `order_totals_by_status` that reads from {{ source('jaffle_shop', 'raw_orders') }} and groups by status, calculating order_count, total_amount, avg_amount, min_amount, and max_amount.",
        "model_name": "order_totals_by_status",
        "expected_columns": ["status", "order_count", "total_amount"],
    },
    {
        "prompt": "Write a model called `payment_method_summary` that reads from {{ source('jaffle_shop', 'raw_payments') }} where is_refund=false and groups by payment_method. Calculate payment_count, total_amount, avg_amount, and success_rate (count where status='success' / total count * 100).",
        "model_name": "payment_method_summary",
        "expected_columns": ["payment_method", "payment_count", "total_amount"],
    },
    {
        "prompt": "Write a model called `monthly_revenue` that reads from {{ source('jaffle_shop', 'raw_orders') }}. Extract year and month from order_date. Group by year and month. Calculate total_revenue, order_count, avg_order_value, and unique_customers.",
        "model_name": "monthly_revenue",
        "expected_columns": ["total_revenue", "order_count"],
    },
    {
        "prompt": "Write a model called `customer_type_summary` that joins {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_orders') }}. Group by customer_type. Calculate num_customers (count distinct customer_id), total_orders, total_revenue, avg_order_value.",
        "model_name": "customer_type_summary",
        "expected_columns": ["customer_type", "num_customers", "total_orders", "total_revenue"],
    },
    {
        "prompt": "Write a model called `referral_source_performance` that joins {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_orders') }}. Group by referral_source. Calculate num_customers, total_orders, total_revenue, and avg_revenue_per_customer.",
        "model_name": "referral_source_performance",
        "expected_columns": ["referral_source", "num_customers", "total_revenue"],
    },
    {
        "prompt": "Write a model called `shipping_method_analysis` that reads from {{ source('jaffle_shop', 'raw_orders') }}. Group by shipping_method. Calculate order_count, total_revenue, avg_order_value, and avg_discount.",
        "model_name": "shipping_method_analysis",
        "expected_columns": ["shipping_method", "order_count", "total_revenue"],
    },
    {
        "prompt": "Write a model called `state_revenue` that joins {{ source('jaffle_shop', 'raw_addresses') }} (where is_default=true and address_type='shipping') with {{ source('jaffle_shop', 'raw_orders') }} via customer_id. Group by state. Calculate num_customers, total_orders, total_revenue.",
        "model_name": "state_revenue",
        "expected_columns": ["state", "num_customers", "total_orders", "total_revenue"],
    },
    {
        "prompt": "Write a model called `top_products_by_revenue` that joins {{ source('jaffle_shop', 'raw_order_items') }} with {{ source('jaffle_shop', 'raw_products') }}. Group by product_id, product name, category. Calculate total_revenue (sum qty * unit_price) and total_quantity. Order by total_revenue DESC. Limit to 10.",
        "model_name": "top_products_by_revenue",
        "expected_columns": ["product_id", "total_revenue", "total_quantity"],
    },

    # ==========================================================
    # WINDOW FUNCTIONS
    # ==========================================================
    {
        "prompt": "Write a model called `orders_with_row_number` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds a row_number column partitioned by customer_id ordered by order_date.",
        "model_name": "orders_with_row_number",
        "expected_columns": ["id", "customer_id", "row_number"],
    },
    {
        "prompt": "Write a model called `orders_running_total` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds running_total as SUM(amount) OVER (ORDER BY order_date) and running_count as COUNT(*) OVER (ORDER BY order_date).",
        "model_name": "orders_running_total",
        "expected_columns": ["id", "amount", "running_total", "running_count"],
    },
    {
        "prompt": "Write a model called `orders_with_lag` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds prev_order_amount using LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) and prev_order_date using LAG(order_date).",
        "model_name": "orders_with_lag",
        "expected_columns": ["id", "customer_id", "prev_order_amount", "prev_order_date"],
    },
    {
        "prompt": "Write a model called `orders_ntile` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds quartile using NTILE(4) OVER (ORDER BY amount) and decile using NTILE(10) OVER (ORDER BY amount).",
        "model_name": "orders_ntile",
        "expected_columns": ["id", "amount", "quartile", "decile"],
    },
    {
        "prompt": "Write a model called `customer_order_sequence` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds order_sequence (ROW_NUMBER partitioned by customer_id ordered by order_date), is_first_order (true when sequence=1), and is_last_order (true when sequence = count of customer's orders).",
        "model_name": "customer_order_sequence",
        "expected_columns": ["id", "customer_id", "order_sequence", "is_first_order"],
    },
    {
        "prompt": "Write a model called `orders_pct_of_total` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds pct_of_customer_total as amount / SUM(amount) OVER (PARTITION BY customer_id) * 100, and pct_of_grand_total as amount / SUM(amount) OVER () * 100.",
        "model_name": "orders_pct_of_total",
        "expected_columns": ["id", "amount", "pct_of_customer_total", "pct_of_grand_total"],
    },
    {
        "prompt": "Write a model called `orders_moving_avg` that selects from {{ source('jaffle_shop', 'raw_orders') }} and calculates a 3-row moving average of amount per customer: AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg.",
        "model_name": "orders_moving_avg",
        "expected_columns": ["id", "customer_id", "amount", "moving_avg"],
    },
    {
        "prompt": "Write a model called `product_rank_by_category` that joins {{ source('jaffle_shop', 'raw_order_items') }} with {{ source('jaffle_shop', 'raw_products') }}. Calculate total_revenue per product (sum qty * price). Then rank products within each category by total_revenue DESC using RANK().",
        "model_name": "product_rank_by_category",
        "expected_columns": ["product_id", "category", "total_revenue"],
    },

    # ==========================================================
    # CASE / CONDITIONAL LOGIC
    # ==========================================================
    {
        "prompt": "Write a model called `order_size_category` that reads from {{ source('jaffle_shop', 'raw_orders') }} and adds order_size: 'small' when amount < 10000, 'medium' when between 10000 and 30000, 'large' when above 30000.",
        "model_name": "order_size_category",
        "expected_columns": ["id", "amount", "order_size"],
    },
    {
        "prompt": "Write a model called `customer_segments` that joins {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_orders') }}. Group by customer. Add segment: 'enterprise' when customer_type='business' and total amount > 50000, 'growth' when total amount > 20000, 'starter' otherwise. Include customer_id, first_name, customer_type, total_amount, segment.",
        "model_name": "customer_segments",
        "expected_columns": ["customer_id", "first_name", "total_amount", "segment"],
    },
    {
        "prompt": "Write a model called `payment_type_flags` that reads from {{ source('jaffle_shop', 'raw_payments') }} and adds boolean columns: is_credit_card (payment_method='credit_card'), is_bank_transfer (payment_method='bank_transfer'), is_gift_card (payment_method='gift_card'), is_successful (status='success').",
        "model_name": "payment_type_flags",
        "expected_columns": ["id", "is_credit_card", "is_bank_transfer", "is_successful"],
    },
    {
        "prompt": "Write a model called `order_channel_type` that reads from {{ source('jaffle_shop', 'raw_orders') }} and adds channel_type: 'digital' when channel IN ('web', 'mobile'), 'offline' when channel = 'phone', 'other' otherwise.",
        "model_name": "order_channel_type",
        "expected_columns": ["id", "channel", "channel_type"],
    },
    {
        "prompt": "Write a model called `refund_risk_flag` that joins {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_refunds') }}. Add risk_level: 'high' if order has an approved refund and amount > 20000, 'medium' if has any refund request, 'low' otherwise. Include order_id, customer_id, amount, risk_level.",
        "model_name": "refund_risk_flag",
        "expected_columns": ["order_id", "customer_id", "risk_level"],
    },

    # ==========================================================
    # NULL / COALESCE / DEFENSIVE PATTERNS
    # ==========================================================
    {
        "prompt": "Write a model called `orders_with_defaults` that selects from {{ source('jaffle_shop', 'raw_orders') }} and uses COALESCE: default NULL discount_amount to 0, NULL tax_amount to 0, NULL notes to 'none', NULL shipping_method to 'standard'.",
        "model_name": "orders_with_defaults",
        "expected_columns": ["id", "discount_amount", "tax_amount", "shipping_method"],
    },
    {
        "prompt": "Write a model called `safe_order_metrics` that reads from {{ source('jaffle_shop', 'raw_orders') }} and calculates effective_amount as amount - COALESCE(discount_amount, 0) + COALESCE(tax_amount, 0). Also calculate discount_rate as COALESCE(discount_amount, 0) / NULLIF(amount, 0) * 100.",
        "model_name": "safe_order_metrics",
        "expected_columns": ["id", "effective_amount", "discount_rate"],
    },
    {
        "prompt": "Write a model called `payments_clean` that selects from {{ source('jaffle_shop', 'raw_payments') }}. COALESCE processor_id to 'unknown', filter where amount IS NOT NULL and amount != 0, and add days_to_update as the date difference between updated_at and created_at.",
        "model_name": "payments_clean",
        "expected_columns": ["id", "processor_id", "days_to_update"],
    },

    # ==========================================================
    # UNION PATTERNS
    # ==========================================================
    {
        "prompt": "Write a model called `all_events` that creates a unified event log. UNION ALL: (1) from raw_orders: id, customer_id as entity_id, order_date as event_date, 'order_placed' as event_type, amount as event_value; (2) from raw_payments where is_refund=false: id, order_id as entity_id, created_at as event_date, 'payment_made' as event_type, amount as event_value; (3) from raw_refunds: id, order_id as entity_id, requested_at as event_date, 'refund_requested' as event_type, refund_amount as event_value.",
        "model_name": "all_events",
        "expected_columns": ["id", "entity_id", "event_type", "event_value"],
    },
    {
        "prompt": "Write a model called `all_amounts` that unions amount from raw_orders (with 'order' as source_type) and amount from raw_payments where is_refund=false (with 'payment' as source_type). Include id, amount, source_type.",
        "model_name": "all_amounts",
        "expected_columns": ["id", "amount", "source_type"],
    },
    {
        "prompt": "Write a model called `all_timestamps` that creates a timeline of all timestamps in the system. UNION ALL: created_at from raw_customers (event='customer_created'), order_date from raw_orders (event='order_placed'), created_at from raw_payments (event='payment_processed'), requested_at from raw_refunds (event='refund_requested'). Cast all to timestamp. Include event_timestamp and event_type.",
        "model_name": "all_timestamps",
        "expected_columns": ["event_type"],
    },

    # ==========================================================
    # INCREMENTAL PATTERNS
    # ==========================================================
    {
        "prompt": "Write an incremental model called `incremental_orders` with {{ config(materialized='incremental', unique_key='id') }}. Select from {{ source('jaffle_shop', 'raw_orders') }}. Use {% if is_incremental() %} to filter where order_date > (select max(order_date) from {{ this }}) {% endif %}.",
        "model_name": "incremental_orders",
        "expected_columns": ["id", "customer_id", "order_date", "status"],
    },
    {
        "prompt": "Write an incremental model called `incremental_payments` with {{ config(materialized='incremental', unique_key='id') }}. Select from {{ source('jaffle_shop', 'raw_payments') }}. Filter incrementally on created_at.",
        "model_name": "incremental_payments",
        "expected_columns": ["id", "order_id", "payment_method", "amount"],
    },
    {
        "prompt": "Write an incremental model called `incremental_order_items` with {{ config(materialized='incremental', unique_key='id') }}. Select from {{ source('jaffle_shop', 'raw_order_items') }}. Filter incrementally on created_at. Include all columns plus a calculated line_total.",
        "model_name": "incremental_order_items",
        "expected_columns": ["id", "order_id", "product_id", "quantity"],
    },

    # ==========================================================
    # CONFIG PATTERNS
    # ==========================================================
    {
        "prompt": "Write a model called `dim_payment_methods` with {{ config(materialized='table') }}. Select DISTINCT payment_method from {{ source('jaffle_shop', 'raw_payments') }} where is_refund=false. Add a payment_method_id using ROW_NUMBER() and a description column using CASE (e.g., 'Credit Card Payment' for 'credit_card').",
        "model_name": "dim_payment_methods",
        "expected_columns": ["payment_method_id", "payment_method"],
    },
    {
        "prompt": "Write a model called `dim_channels` with {{ config(materialized='table') }}. Select DISTINCT channel from {{ source('jaffle_shop', 'raw_orders') }}. Add channel_id using ROW_NUMBER() and channel_type using CASE ('digital' for web/mobile, 'offline' for phone).",
        "model_name": "dim_channels",
        "expected_columns": ["channel_id", "channel"],
    },
    {
        "prompt": "Write a model called `orders_ephemeral` with {{ config(materialized='ephemeral') }}. Select id as order_id, customer_id, order_date, status, amount from {{ source('jaffle_shop', 'raw_orders') }}.",
        "model_name": "orders_ephemeral",
        "expected_columns": ["order_id", "customer_id", "status"],
    },

    # ==========================================================
    # JINJA PATTERNS
    # ==========================================================
    {
        "prompt": "Write a model called `orders_jinja_filter` that selects from {{ source('jaffle_shop', 'raw_orders') }}. Use {% if var('include_cancelled', false) %} to conditionally include or exclude cancelled orders.",
        "model_name": "orders_jinja_filter",
        "expected_columns": ["id", "status"],
    },
    {
        "prompt": "Write a model called `dynamic_order_limit` that selects from {{ source('jaffle_shop', 'raw_orders') }} with LIMIT {{ var('row_limit', 100) }}.",
        "model_name": "dynamic_order_limit",
        "expected_columns": ["id", "customer_id"],
    },
    {
        "prompt": "Write a model called `orders_status_flags` that selects from {{ source('jaffle_shop', 'raw_orders') }}. Use a Jinja {% for %} loop over ['completed', 'shipped', 'returned', 'cancelled'] to generate CASE WHEN status = '<status>' THEN true ELSE false END AS is_<status> columns.",
        "model_name": "orders_status_flags",
        "expected_columns": ["id", "is_completed", "is_returned"],
    },
    {
        "prompt": "Write a model called `configurable_materialization` with {{ config(materialized=var('mat_type', 'view')) }}. Select all columns from {{ source('jaffle_shop', 'raw_orders') }}.",
        "model_name": "configurable_materialization",
        "expected_columns": ["id", "customer_id", "status"],
    },

    # ==========================================================
    # COMPLEX / MULTI-HOP JOINS
    # ==========================================================
    {
        "prompt": "Write a model called `order_detail_full` that joins raw_orders, raw_customers, raw_payments (non-refund, success, aggregated), raw_order_items (aggregated), and raw_addresses (default shipping). This is a 5-source model. Use CTEs for each source. Final output: order_id, customer_name, email, city, state, order_date, status, channel, order_amount, total_paid, total_items, shipping_method.",
        "model_name": "order_detail_full",
        "expected_columns": ["order_id", "order_date", "status", "total_paid", "total_items"],
    },
    {
        "prompt": "Write a model called `customer_product_affinity` that joins raw_customers → raw_orders → raw_order_items → raw_products. Group by customer_id, first_name, category. Calculate times_purchased (count), total_quantity, total_spent. This shows which product categories each customer buys most.",
        "model_name": "customer_product_affinity",
        "expected_columns": ["customer_id", "first_name", "category", "times_purchased", "total_spent"],
    },
    {
        "prompt": "Write a model called `refund_analysis` that joins raw_refunds with raw_orders and raw_customers. Include refund_id, order_id, customer_name (first + last), order_amount, refund_amount, reason, refund_status, days_to_process (date diff between processed_at and requested_at).",
        "model_name": "refund_analysis",
        "expected_columns": ["refund_id", "order_id", "reason"],
    },
    {
        "prompt": "Write a model called `product_customer_matrix` that joins raw_order_items → raw_products and raw_order_items → raw_orders → raw_customers. Create a summary showing product_name, category, num_unique_customers (count distinct customer_id), total_revenue, and avg_quantity_per_customer.",
        "model_name": "product_customer_matrix",
        "expected_columns": ["product_name", "category", "num_unique_customers", "total_revenue"],
    },

    # ==========================================================
    # SUBQUERIES / CORRELATED QUERIES
    # ==========================================================
    {
        "prompt": "Write a model called `above_avg_orders` that selects orders from {{ source('jaffle_shop', 'raw_orders') }} where amount is greater than the average amount across all orders.",
        "model_name": "above_avg_orders",
        "expected_columns": ["id", "customer_id", "amount"],
    },
    {
        "prompt": "Write a model called `customers_with_refunds` that selects from {{ source('jaffle_shop', 'raw_customers') }} where id IN (select distinct customer_id from {{ source('jaffle_shop', 'raw_orders') }} where id IN (select order_id from {{ source('jaffle_shop', 'raw_refunds') }})).",
        "model_name": "customers_with_refunds",
        "expected_columns": ["id", "first_name", "last_name"],
    },
    {
        "prompt": "Write a model called `max_order_per_customer` using a CTE with ROW_NUMBER() partitioned by customer_id ordered by amount DESC from {{ source('jaffle_shop', 'raw_orders') }}. Filter to row_number = 1 to get each customer's largest order.",
        "model_name": "max_order_per_customer",
        "expected_columns": ["customer_id", "amount"],
    },
    {
        "prompt": "Write a model called `orders_above_customer_avg` using CTEs. CTE1: calculate avg_amount per customer_id from raw_orders. CTE2: raw_orders. Final: join and filter where amount > avg_amount.",
        "model_name": "orders_above_customer_avg",
        "expected_columns": ["id", "customer_id", "amount"],
    },

    # ==========================================================
    # STRING / DATE OPERATIONS
    # ==========================================================
    {
        "prompt": "Write a model called `customer_initials` that selects from {{ source('jaffle_shop', 'raw_customers') }} and creates initials by taking first letter of first_name + first letter of last_name. Include id, initials, and email.",
        "model_name": "customer_initials",
        "expected_columns": ["id", "initials", "email"],
    },
    {
        "prompt": "Write a model called `orders_by_quarter` that reads from {{ source('jaffle_shop', 'raw_orders') }}. Add order_quarter ('Q1', 'Q2', etc.) derived from order_date. Group by order_quarter. Calculate total_revenue and order_count.",
        "model_name": "orders_by_quarter",
        "expected_columns": ["order_quarter", "total_revenue", "order_count"],
    },
    {
        "prompt": "Write a model called `payment_processing_time` that selects from {{ source('jaffle_shop', 'raw_payments') }}. Calculate processing_seconds as the difference between updated_at and created_at in seconds. Include payment_id (id), order_id, payment_method, processing_seconds.",
        "model_name": "payment_processing_time",
        "expected_columns": ["order_id", "payment_method", "processing_seconds"],
    },
    {
        "prompt": "Write a model called `customer_email_parts` that selects from {{ source('jaffle_shop', 'raw_customers') }}. Split email into email_username (before @) and email_domain (after @). Include id, first_name, email_username, email_domain.",
        "model_name": "customer_email_parts",
        "expected_columns": ["id", "email_username", "email_domain"],
    },
    {
        "prompt": "Write a model called `orders_month_start` that selects from {{ source('jaffle_shop', 'raw_orders') }} and adds month_start using DATE_TRUNC('month', order_date). Include id, order_date, month_start, amount.",
        "model_name": "orders_month_start",
        "expected_columns": ["id", "order_date", "month_start"],
    },

    # ==========================================================
    # SIMPLE SELECTS (baselines — should score 1.0)
    # ==========================================================
    {
        "prompt": "Write a dbt model called `raw_customers_copy` that selects all columns from {{ source('jaffle_shop', 'raw_customers') }} with no transformations.",
        "model_name": "raw_customers_copy",
        "expected_columns": ["id", "first_name", "last_name", "email", "customer_type"],
    },
    {
        "prompt": "Write a dbt model called `raw_orders_copy` that selects all columns from {{ source('jaffle_shop', 'raw_orders') }} unchanged.",
        "model_name": "raw_orders_copy",
        "expected_columns": ["id", "customer_id", "order_date", "status", "amount", "channel"],
    },
    {
        "prompt": "Write a dbt model called `raw_products_copy` that selects all columns from {{ source('jaffle_shop', 'raw_products') }} unchanged.",
        "model_name": "raw_products_copy",
        "expected_columns": ["id", "name", "category", "unit_price", "sku"],
    },
    {
        "prompt": "Write a model called `ordered_customers` that selects first_name, last_name, email, customer_type from {{ source('jaffle_shop', 'raw_customers') }} ordered by last_name ASC, first_name ASC.",
        "model_name": "ordered_customers",
        "expected_columns": ["first_name", "last_name", "email"],
    },
    {
        "prompt": "Write a model called `limited_orders` that selects id, customer_id, order_date, amount, channel from {{ source('jaffle_shop', 'raw_orders') }} limited to 10 rows.",
        "model_name": "limited_orders",
        "expected_columns": ["id", "customer_id", "amount"],
    },

    # ==========================================================
    # CROSS-TABLE EDGE CASES
    # ==========================================================
    {
        "prompt": "Write a model called `orders_not_paid` that LEFT JOINs {{ source('jaffle_shop', 'raw_orders') }} with {{ source('jaffle_shop', 'raw_payments') }} (where is_refund=false and status='success'). Filter to orders where no successful payment exists (payment.id IS NULL). Include order_id, customer_id, order_date, amount.",
        "model_name": "orders_not_paid",
        "expected_columns": ["customer_id", "amount"],
    },
    {
        "prompt": "Write a model called `customers_without_orders` that LEFT JOINs {{ source('jaffle_shop', 'raw_customers') }} with {{ source('jaffle_shop', 'raw_orders') }} and filters where order id IS NULL. Include customer_id, first_name, last_name, email, signup_date (cast created_at).",
        "model_name": "customers_without_orders",
        "expected_columns": ["first_name", "last_name", "email"],
    },
    {
        "prompt": "Write a model called `products_never_ordered` that LEFT JOINs {{ source('jaffle_shop', 'raw_products') }} with {{ source('jaffle_shop', 'raw_order_items') }} and filters where order_item id IS NULL. Include product_id, name, category, sku.",
        "model_name": "products_never_ordered",
        "expected_columns": ["name", "category", "sku"],
    },
    {
        "prompt": "Write a model called `self_join_customer_orders` that self-joins {{ source('jaffle_shop', 'raw_orders') }} to find pairs of orders from the same customer on different dates. Join on customer_id where o1.id < o2.id. Include customer_id, o1_date, o2_date, o1_amount, o2_amount.",
        "model_name": "self_join_customer_orders",
        "expected_columns": ["customer_id"],
    },
    {
        "prompt": "Write a model called `pivoted_payments` that reads from {{ source('jaffle_shop', 'raw_payments') }} where is_refund=false. Pivot by payment_method: group by order_id and use SUM(CASE WHEN payment_method='credit_card' THEN amount ELSE 0 END) as credit_card_amount, similar for bank_transfer and gift_card.",
        "model_name": "pivoted_payments",
        "expected_columns": ["order_id", "credit_card_amount"],
    },
]


if __name__ == "__main__":
    print(f"Total RL prompts: {len(RL_PROMPTS)}")
    categories = {}
    for p in RL_PROMPTS:
        name = p["model_name"]
        categories[name] = categories.get(name, 0) + 1
    dupes = {k: v for k, v in categories.items() if v > 1}
    if dupes:
        print(f"  WARNING: Duplicate model names: {dupes}")
    for i, p in enumerate(RL_PROMPTS):
        print(f"  {i+1}. [{p['model_name']}] {p['prompt'][:80]}...")
