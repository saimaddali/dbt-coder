"""
Expanded eval prompts for dbt-coder v2.
60 prompts across 9 pattern categories, inspired by Spider2-DBT and ade-bench.
All prompts use the jaffle_shop expanded schema (22 tables).
"""

EVAL_PROMPTS = [
    # ========================================================================
    # STAGING (8 prompts) — rename, cast, filter, basic transforms
    # ========================================================================
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
    {
        "id": "stg_payments",
        "prompt": "Write a dbt staging model called stg_payments from {{ source('jaffle_shop', 'raw_payments') }}. Rename id to payment_id, amount to payment_amount_cents, add payment_amount_dollars (amount/100.0). Cast created_at to date as payment_date. Exclude refund payments (is_refund = true).",
        "model_name": "stg_payments",
        "expected_columns": ["payment_id", "order_id", "payment_amount_dollars", "payment_date"],
    },
    {
        "id": "stg_products",
        "prompt": "Write a dbt staging model called stg_products from {{ source('jaffle_shop', 'raw_products') }}. Rename id to product_id. Calculate margin as (unit_price - cost). Add margin_pct as (unit_price - cost) / unit_price * 100 rounded to 2 decimals. Only include active products.",
        "model_name": "stg_products",
        "expected_columns": ["product_id", "name", "unit_price", "margin", "margin_pct"],
    },
    {
        "id": "stg_subscriptions",
        "prompt": "Write a dbt staging model called stg_subscriptions from {{ source('jaffle_shop', 'raw_subscriptions') }}. Rename id to subscription_id. Calculate months_active as the number of months between start_date and COALESCE(end_date, current_date). Include customer_id, plan, mrr, status.",
        "model_name": "stg_subscriptions",
        "expected_columns": ["subscription_id", "customer_id", "plan", "mrr", "months_active"],
    },
    {
        "id": "stg_shipping",
        "prompt": "Write a dbt staging model called stg_shipping from {{ source('jaffle_shop', 'raw_shipping') }}. Rename id to shipment_id. Calculate delivery_days as delivered_date - shipped_date. Include order_id, carrier, status, shipping_cost.",
        "model_name": "stg_shipping",
        "expected_columns": ["shipment_id", "order_id", "carrier", "delivery_days"],
    },
    {
        "id": "stg_reviews",
        "prompt": "Write a dbt staging model called stg_reviews from {{ source('jaffle_shop', 'raw_reviews') }}. Rename id to review_id. Add sentiment as a CASE: rating >= 4 = 'positive', rating = 3 = 'neutral', rating <= 2 = 'negative'. Cast created_at to date.",
        "model_name": "stg_reviews",
        "expected_columns": ["review_id", "customer_id", "product_id", "rating", "sentiment"],
    },
    {
        "id": "stg_support_tickets",
        "prompt": "Write a dbt staging model called stg_support_tickets from {{ source('jaffle_shop', 'raw_support_tickets') }}. Rename id to ticket_id. Calculate resolution_hours as the hours between created_at and resolved_at. Include customer_id, category, priority, status.",
        "model_name": "stg_support_tickets",
        "expected_columns": ["ticket_id", "customer_id", "category", "priority", "resolution_hours"],
    },

    # ========================================================================
    # MULTI-TABLE JOINS (10 prompts) — 2-4 table joins with CTEs
    # ========================================================================
    {
        "id": "customer_orders",
        "prompt": "Write a dbt model called customer_orders that joins raw_customers and raw_orders on customer_id. Include customer_id, first_name, last_name, order count, and total order amount in dollars. Use CTEs.",
        "model_name": "customer_orders",
        "expected_columns": ["customer_id", "first_name", "order_count", "total_amount"],
    },
    {
        "id": "order_details",
        "prompt": "Write a dbt model called order_details that joins raw_orders, raw_order_items, and raw_products. Include order_id, product_name, quantity, unit_price, and line_total (quantity * unit_price). Use CTEs with {{ source('jaffle_shop', ...) }}.",
        "model_name": "order_details",
        "expected_columns": ["order_id", "product_name", "quantity", "line_total"],
    },
    {
        "id": "order_payments",
        "prompt": "Write a dbt model called order_payments that joins raw_orders with raw_payments on order_id. Include order_id, order_date, status, payment_method, payment_amount (in dollars), and payment_status. Use CTEs.",
        "model_name": "order_payments",
        "expected_columns": ["order_id", "order_date", "payment_method", "payment_amount"],
    },
    {
        "id": "customer_360",
        "prompt": "Write a dbt model called customer_360 that creates a complete customer profile. Join raw_customers, raw_orders, raw_payments, and raw_addresses. For each customer include: customer_id, full_name (first || ' ' || last), email, total_orders, total_payments_dollars, default_city, default_state. Use CTEs.",
        "model_name": "customer_360",
        "expected_columns": ["customer_id", "full_name", "total_orders", "total_payments_dollars"],
    },
    {
        "id": "product_performance",
        "prompt": "Write a dbt model called product_performance joining raw_products, raw_order_items, and raw_reviews. For each product: product_id, name, total_units_sold (sum of quantity), total_revenue (sum of quantity * unit_price), avg_rating, review_count. Use CTEs.",
        "model_name": "product_performance",
        "expected_columns": ["product_id", "name", "total_units_sold", "total_revenue", "avg_rating"],
    },
    {
        "id": "shipping_analysis",
        "prompt": "Write a dbt model called shipping_analysis joining raw_shipping, raw_orders, and raw_customers. Include order_id, customer_id, first_name, carrier, shipped_date, delivered_date, delivery_days (delivered - shipped), shipping_cost, order_amount. Use CTEs.",
        "model_name": "shipping_analysis",
        "expected_columns": ["order_id", "customer_id", "carrier", "delivery_days", "shipping_cost"],
    },
    {
        "id": "support_context",
        "prompt": "Write a dbt model called support_context joining raw_support_tickets, raw_customers, raw_orders, and raw_employees. Include ticket_id, customer first_name, customer email, order_date, order_status, assigned_employee first_name as agent_name, ticket category, ticket priority. Use CTEs.",
        "model_name": "support_context",
        "expected_columns": ["ticket_id", "customer_id", "order_date", "agent_name", "category"],
    },
    {
        "id": "inventory_status",
        "prompt": "Write a dbt model called inventory_status joining raw_inventory, raw_products, raw_warehouses, and raw_suppliers. For each inventory record: product name, warehouse name, warehouse city, quantity_on_hand, reorder_point, supplier name, lead_time_days. Flag is_below_reorder where quantity_on_hand < reorder_point.",
        "model_name": "inventory_status",
        "expected_columns": ["product_id", "warehouse_id", "quantity_on_hand", "is_below_reorder"],
    },
    {
        "id": "campaign_attribution",
        "prompt": "Write a dbt model called campaign_attribution joining raw_campaigns, raw_email_events, raw_customers, and raw_orders. For each campaign: campaign name, channel, total_emails_sent (event_type='sent'), total_opens, total_clicks, and attributed_orders (orders from customers who clicked within 7 days of the click). Use CTEs.",
        "model_name": "campaign_attribution",
        "expected_columns": ["campaign_id", "name", "total_emails_sent", "attributed_orders"],
    },
    {
        "id": "subscription_orders",
        "prompt": "Write a dbt model called subscription_orders joining raw_subscriptions, raw_customers, and raw_orders. For each subscription: subscription_id, customer first_name, plan, mrr, subscription status, total_orders placed during the subscription period (between start_date and COALESCE(end_date, current_date)). Use CTEs.",
        "model_name": "subscription_orders",
        "expected_columns": ["subscription_id", "customer_id", "plan", "mrr", "total_orders"],
    },

    # ========================================================================
    # AGGREGATIONS + GROUP BY (8 prompts)
    # ========================================================================
    {
        "id": "daily_revenue",
        "prompt": "Write a dbt model called daily_revenue that aggregates raw_orders by order_date. Include order_date, order_count, total_revenue_dollars (amount/100), avg_order_value. Only include completed orders (status = 'completed').",
        "model_name": "daily_revenue",
        "expected_columns": ["order_date", "order_count", "total_revenue_dollars", "avg_order_value"],
    },
    {
        "id": "monthly_revenue",
        "prompt": "Write a dbt model called monthly_revenue that aggregates raw_orders by month (DATE_TRUNC('month', order_date) as revenue_month). Include revenue_month, order_count, total_revenue_dollars, unique_customers (count distinct customer_id).",
        "model_name": "monthly_revenue",
        "expected_columns": ["revenue_month", "order_count", "total_revenue_dollars", "unique_customers"],
    },
    {
        "id": "category_sales",
        "prompt": "Write a dbt model called category_sales joining raw_order_items, raw_products. Group by product category. Include category, total_items_sold, total_revenue, avg_unit_price, distinct_products_sold.",
        "model_name": "category_sales",
        "expected_columns": ["category", "total_items_sold", "total_revenue"],
    },
    {
        "id": "carrier_performance",
        "prompt": "Write a dbt model called carrier_performance aggregating raw_shipping by carrier. Include carrier, total_shipments, avg_delivery_days (delivered_date - shipped_date), on_time_pct (% where delivery_days <= 5), avg_shipping_cost.",
        "model_name": "carrier_performance",
        "expected_columns": ["carrier", "total_shipments", "avg_delivery_days", "on_time_pct"],
    },
    {
        "id": "payment_method_summary",
        "prompt": "Write a dbt model called payment_method_summary aggregating raw_payments by payment_method. Include payment_method, transaction_count, total_amount_dollars (amount/100), avg_amount_dollars, success_rate (% where status = 'completed'). Exclude refund records.",
        "model_name": "payment_method_summary",
        "expected_columns": ["payment_method", "transaction_count", "total_amount_dollars", "success_rate"],
    },
    {
        "id": "customer_segments",
        "prompt": "Write a dbt model called customer_segments. Join raw_customers and raw_orders. Segment customers by total_spend into 'VIP' (>10000), 'Regular' (1000-10000), 'Low' (<1000), and 'Inactive' (no orders). Include customer_id, segment, total_spend, order_count.",
        "model_name": "customer_segments",
        "expected_columns": ["customer_id", "segment", "total_spend", "order_count"],
    },
    {
        "id": "warehouse_utilization",
        "prompt": "Write a dbt model called warehouse_utilization joining raw_warehouses and raw_inventory. Group by warehouse. Include warehouse name, city, capacity, total_inventory (sum of quantity_on_hand), utilization_pct (total_inventory / capacity * 100), items_below_reorder (count where quantity < reorder_point).",
        "model_name": "warehouse_utilization",
        "expected_columns": ["warehouse_id", "name", "utilization_pct", "items_below_reorder"],
    },
    {
        "id": "supplier_reliability",
        "prompt": "Write a dbt model called supplier_reliability joining raw_suppliers, raw_products, raw_order_items, and raw_returns. Group by supplier. Include supplier name, total_products, total_units_sold, return_count, return_rate (returns/units_sold). Use CTEs.",
        "model_name": "supplier_reliability",
        "expected_columns": ["supplier_id", "name", "total_units_sold", "return_rate"],
    },

    # ========================================================================
    # WINDOW FUNCTIONS (8 prompts) — running totals, ranking, LAG/LEAD
    # ========================================================================
    {
        "id": "running_revenue",
        "prompt": "Write a dbt model called running_revenue that calculates daily revenue from raw_orders (amount/100 as revenue_dollars) and a running total using a window function ordered by order_date. Include order_date, daily_revenue, and cumulative_revenue.",
        "model_name": "running_revenue",
        "expected_columns": ["order_date", "daily_revenue", "cumulative_revenue"],
    },
    {
        "id": "customer_order_rank",
        "prompt": "Write a dbt model called customer_order_rank from raw_orders. For each customer's orders, add order_number (ROW_NUMBER ordered by order_date), is_first_order (boolean), and days_since_last_order (order_date - LAG of order_date). Include order_id, customer_id, order_date.",
        "model_name": "customer_order_rank",
        "expected_columns": ["order_id", "customer_id", "order_number", "is_first_order"],
    },
    {
        "id": "product_sales_rank",
        "prompt": "Write a dbt model called product_sales_rank joining raw_order_items and raw_products. Rank products by total revenue (sum of quantity * unit_price) using RANK(). Include product_id, product name, total_revenue, sales_rank. Top 20 only.",
        "model_name": "product_sales_rank",
        "expected_columns": ["product_id", "name", "total_revenue", "sales_rank"],
    },
    {
        "id": "mom_revenue",
        "prompt": "Write a dbt model called mom_revenue calculating month-over-month revenue growth from raw_orders. Include revenue_month, monthly_revenue, prev_month_revenue (LAG), mom_growth_pct ((current - prev) / prev * 100). Use CTEs.",
        "model_name": "mom_revenue",
        "expected_columns": ["revenue_month", "monthly_revenue", "prev_month_revenue", "mom_growth_pct"],
    },
    {
        "id": "moving_avg_orders",
        "prompt": "Write a dbt model called moving_avg_orders calculating a 7-day moving average of daily order count from raw_orders. Include order_date, daily_orders, moving_avg_7d (AVG over preceding 6 rows and current row).",
        "model_name": "moving_avg_orders",
        "expected_columns": ["order_date", "daily_orders", "moving_avg_7d"],
    },
    {
        "id": "customer_lifetime",
        "prompt": "Write a dbt model called customer_lifetime that calculates for each customer: first_order_date, most_recent_order_date, lifetime_order_count, lifetime_spend_dollars (amount/100), and days_as_customer (most_recent - first). Join raw_customers and raw_orders.",
        "model_name": "customer_lifetime",
        "expected_columns": ["customer_id", "first_order_date", "lifetime_order_count", "lifetime_spend_dollars"],
    },
    {
        "id": "review_trends",
        "prompt": "Write a dbt model called review_trends from raw_reviews. Group by month (DATE_TRUNC('month', created_at)). Calculate avg_rating, review_count, and a 3-month moving average of avg_rating. Use window functions.",
        "model_name": "review_trends",
        "expected_columns": ["review_month", "avg_rating", "review_count"],
    },
    {
        "id": "session_funnel",
        "prompt": "Write a dbt model called session_funnel joining raw_sessions and raw_page_views. For each session: session_id, customer_id, device_type, page_count, session_duration_minutes (session_end - session_start), and session_rank per customer (ordered by session_start). Use CTEs and window functions.",
        "model_name": "session_funnel",
        "expected_columns": ["session_id", "customer_id", "page_count", "session_duration_minutes"],
    },

    # ========================================================================
    # UNION / TYPE COERCION (7 prompts) — fusion-divergent patterns
    # ========================================================================
    {
        "id": "all_events",
        "prompt": "Write a dbt model called all_events that UNIONs order events and payment events into a single timeline. From raw_orders use order_date as event_date and 'order' as event_type. From raw_payments use created_at as event_date and 'payment' as event_type. Include relevant IDs. Cast dates to the same type.",
        "model_name": "all_events",
        "expected_columns": ["event_date", "event_type"],
    },
    {
        "id": "all_customer_activity",
        "prompt": "Write a dbt model called all_customer_activity that UNIONs three event sources: orders (order_date, 'order'), support tickets (created_at, 'support'), and reviews (created_at, 'review'). Include customer_id, event_date, event_type, and event_id. Ensure consistent date types across the UNION.",
        "model_name": "all_customer_activity",
        "expected_columns": ["customer_id", "event_date", "event_type", "event_id"],
    },
    {
        "id": "combined_financial_events",
        "prompt": "Write a dbt model called combined_financial_events that UNIONs payments (amount, created_at, 'payment'), refunds (refund_amount as amount, requested_at, 'refund'), and order discounts (discount_amount as amount, order_date, 'discount') from their respective source tables. Include event_type, amount, event_date.",
        "model_name": "combined_financial_events",
        "expected_columns": ["event_type", "amount", "event_date"],
    },
    {
        "id": "all_status_changes",
        "prompt": "Write a dbt model called all_status_changes that UNIONs status fields from raw_orders (status, order_date), raw_shipping (status, shipped_date), raw_payments (status, created_at), and raw_refunds (status, requested_at). Include source_table, status, status_date, record_id. Cast all dates consistently.",
        "model_name": "all_status_changes",
        "expected_columns": ["source_table", "status", "status_date", "record_id"],
    },
    {
        "id": "product_feedback_union",
        "prompt": "Write a dbt model called product_feedback_union that UNIONs product reviews (from raw_reviews: rating as score, review_text as feedback, created_at) with return reasons (from raw_returns: NULL as score, reason as feedback, requested_at as created_at). Include product_id, feedback_type ('review' or 'return'), score, feedback, event_date.",
        "model_name": "product_feedback_union",
        "expected_columns": ["product_id", "feedback_type", "score", "feedback"],
    },
    {
        "id": "employee_customer_contacts",
        "prompt": "Write a dbt model called employee_customer_contacts that UNIONs support tickets (assigned_employee_id as employee_id, customer_id, created_at, 'support' as contact_type) with refund processing (processed_by as employee_id, raw_refunds joined to raw_orders for customer_id, processed_at, 'refund'). Use CTEs.",
        "model_name": "employee_customer_contacts",
        "expected_columns": ["employee_id", "customer_id", "contact_date", "contact_type"],
    },
    {
        "id": "all_monetary_transactions",
        "prompt": "Write a dbt model called all_monetary_transactions combining: order amounts (raw_orders.amount/100 as amount_dollars, 'sale'), payment amounts (raw_payments.amount/100, 'payment'), refund amounts (raw_refunds.refund_amount/100 as amount_dollars, 'refund'), shipping costs (raw_shipping.shipping_cost, 'shipping'). UNION ALL with transaction_type, amount_dollars, transaction_date.",
        "model_name": "all_monetary_transactions",
        "expected_columns": ["transaction_type", "amount_dollars", "transaction_date"],
    },

    # ========================================================================
    # INCREMENTAL MODELS (5 prompts)
    # ========================================================================
    {
        "id": "incremental_orders",
        "prompt": "Write a dbt incremental model called incremental_orders that reads from {{ source('jaffle_shop', 'raw_orders') }}. Use the merge strategy, unique on id, and incrementally load based on order_date. Include all columns.",
        "model_name": "incremental_orders",
        "expected_columns": ["id", "customer_id", "order_date"],
    },
    {
        "id": "incremental_payments",
        "prompt": "Write a dbt incremental model called incremental_payments from {{ source('jaffle_shop', 'raw_payments') }}. Merge strategy, unique on id. Incrementally load where created_at > max(created_at) from this model. Rename id to payment_id, add amount_dollars.",
        "model_name": "incremental_payments",
        "expected_columns": ["payment_id", "order_id", "amount_dollars"],
    },
    {
        "id": "incremental_page_views",
        "prompt": "Write a dbt incremental model called incremental_page_views from {{ source('jaffle_shop', 'raw_page_views') }}. Append strategy. Incrementally load where viewed_at is newer than the latest in this model. Include id, session_id, page_url, viewed_at, duration_seconds.",
        "model_name": "incremental_page_views",
        "expected_columns": ["id", "session_id", "page_url", "viewed_at"],
    },
    {
        "id": "incremental_email_events",
        "prompt": "Write a dbt incremental model called incremental_email_events from {{ source('jaffle_shop', 'raw_email_events') }}. Delete+insert strategy with unique_key id. Incrementally load on event_timestamp. Include all columns.",
        "model_name": "incremental_email_events",
        "expected_columns": ["id", "customer_id", "campaign_id", "event_type"],
    },
    {
        "id": "incremental_inventory_snapshot",
        "prompt": "Write a dbt incremental model called incremental_inventory_snapshot from {{ source('jaffle_shop', 'raw_inventory') }}. Merge on composite key (product_id, warehouse_id). Incrementally load on updated_at. Include all columns plus a snapshot_date (current_date).",
        "model_name": "incremental_inventory_snapshot",
        "expected_columns": ["product_id", "warehouse_id", "quantity_on_hand", "snapshot_date"],
    },

    # ========================================================================
    # CASE / CONDITIONAL LOGIC (5 prompts)
    # ========================================================================
    {
        "id": "order_status_summary",
        "prompt": "Write a dbt model called order_status_summary that categorizes orders from raw_orders into 'high_value' (amount > 5000), 'medium_value' (1000-5000), and 'low_value' (< 1000). Include order_id, customer_id, amount, and value_category.",
        "model_name": "order_status_summary",
        "expected_columns": ["order_id", "value_category"],
    },
    {
        "id": "customer_risk_score",
        "prompt": "Write a dbt model called customer_risk_score joining raw_customers, raw_orders, raw_refunds, and raw_support_tickets. Score each customer: +1 per refund, +1 per high-priority ticket, -1 per completed order. Categorize as 'high_risk' (score > 2), 'medium_risk' (1-2), 'low_risk' (<=0). Include customer_id, risk_score, risk_category.",
        "model_name": "customer_risk_score",
        "expected_columns": ["customer_id", "risk_score", "risk_category"],
    },
    {
        "id": "order_channel_analysis",
        "prompt": "Write a dbt model called order_channel_analysis from raw_orders. Use CASE to bucket channels: 'web' and 'mobile' as 'digital', 'phone' and 'in_store' as 'offline', everything else as 'other'. Group by channel_group and month. Include channel_group, order_month, order_count, total_revenue.",
        "model_name": "order_channel_analysis",
        "expected_columns": ["channel_group", "order_month", "order_count", "total_revenue"],
    },
    {
        "id": "delivery_sla",
        "prompt": "Write a dbt model called delivery_sla from raw_shipping. Calculate delivery_days. Categorize: 'on_time' (<=3 days), 'acceptable' (4-5), 'late' (6-7), 'very_late' (>7). Include shipment_id, order_id, carrier, delivery_days, sla_status. Add is_sla_breach boolean.",
        "model_name": "delivery_sla",
        "expected_columns": ["order_id", "carrier", "delivery_days", "sla_status", "is_sla_breach"],
    },
    {
        "id": "subscription_health",
        "prompt": "Write a dbt model called subscription_health from raw_subscriptions. Classify subscriptions: 'new' (started within 30 days), 'active' (status='active' and >30 days), 'at_risk' (active but mrr decreased from initial), 'churned' (status='cancelled'). Include subscription_id, customer_id, plan, mrr, health_status.",
        "model_name": "subscription_health",
        "expected_columns": ["subscription_id", "customer_id", "health_status", "mrr"],
    },

    # ========================================================================
    # JINJA / MACROS (5 prompts)
    # ========================================================================
    {
        "id": "filtered_payments",
        "prompt": "Write a dbt model called filtered_payments that reads from raw_payments. Use a Jinja variable to set a minimum payment amount (default 1000). Filter out payments below that threshold. Include payment_id (renamed from id), order_id, amount, and payment_method.",
        "model_name": "filtered_payments",
        "expected_columns": ["payment_id", "order_id", "amount"],
    },
    {
        "id": "configurable_revenue",
        "prompt": "Write a dbt model called configurable_revenue from raw_orders. Use a Jinja variable 'revenue_start_date' (default '2024-01-01'). Filter orders after that date. Group by month, include revenue_month, order_count, total_revenue. Use {{ var('revenue_start_date') }}.",
        "model_name": "configurable_revenue",
        "expected_columns": ["revenue_month", "order_count", "total_revenue"],
    },
    {
        "id": "dynamic_customer_type",
        "prompt": "Write a dbt model called dynamic_customer_type from raw_customers. Use {% set customer_types = ['business', 'individual'] %} and loop through to create a boolean column for each type (is_business, is_individual). Include customer_id, email, customer_type.",
        "model_name": "dynamic_customer_type",
        "expected_columns": ["customer_id", "customer_type", "is_business", "is_individual"],
    },
    {
        "id": "pivot_payment_methods",
        "prompt": "Write a dbt model called pivot_payment_methods from raw_payments. For each order_id, pivot payment methods into columns: credit_card_amount, bank_transfer_amount, gift_card_amount (sum of amount where payment_method matches). Use Jinja to loop over methods. Include order_id.",
        "model_name": "pivot_payment_methods",
        "expected_columns": ["order_id", "credit_card_amount", "bank_transfer_amount"],
    },
    {
        "id": "conditional_materialization",
        "prompt": "Write a dbt model called conditional_materialization from raw_orders. Use {{ config(materialized='table' if target.name == 'prod' else 'view') }}. Add an is_test boolean column that is TRUE when target.name != 'prod'. Include order_id, customer_id, order_date, amount, is_test.",
        "model_name": "conditional_materialization",
        "expected_columns": ["order_id", "customer_id", "is_test"],
    },

    # ========================================================================
    # COMPLEX CTEs — 5+ steps (4 prompts)
    # ========================================================================
    {
        "id": "customer_cohort_analysis",
        "prompt": "Write a dbt model called customer_cohort_analysis. Step 1: Get each customer's first order month from raw_orders (cohort_month). Step 2: Count orders per customer per month. Step 3: Join to get cohort_month for each customer. Step 4: Calculate months_since_first as order_month - cohort_month. Step 5: Group by cohort_month and months_since_first, count customers and total orders. Use CTEs.",
        "model_name": "customer_cohort_analysis",
        "expected_columns": ["cohort_month", "months_since_first", "customer_count", "total_orders"],
    },
    {
        "id": "refund_analysis",
        "prompt": "Write a dbt model called refund_analysis. CTE 1: Join raw_refunds with raw_orders. CTE 2: Join with raw_order_items to get item details. CTE 3: Calculate order_total per order. CTE 4: Calculate refund_rate (refund_amount / order_total). Final: Include order_id, refund_reason, refund_status, refund_amount, order_total, refund_rate, days_to_process (processed_at - requested_at). Handle NULLs for pending refunds.",
        "model_name": "refund_analysis",
        "expected_columns": ["order_id", "refund_reason", "refund_rate", "days_to_process"],
    },
    {
        "id": "marketing_roi",
        "prompt": "Write a dbt model called marketing_roi. CTE 1: Get campaign details from raw_campaigns. CTE 2: Count email events by type from raw_email_events per campaign. CTE 3: Join email clicks with raw_customers. CTE 4: Find orders placed within 7 days of a click. CTE 5: Calculate attributed_revenue per campaign. Final: campaign_id, name, channel, budget, attributed_revenue, roi ((revenue - budget) / budget). Use CTEs.",
        "model_name": "marketing_roi",
        "expected_columns": ["campaign_id", "name", "budget", "attributed_revenue", "roi"],
    },
    {
        "id": "supply_chain_overview",
        "prompt": "Write a dbt model called supply_chain_overview. CTE 1: Product details from raw_products with supplier from raw_suppliers. CTE 2: Current inventory from raw_inventory with warehouse from raw_warehouses. CTE 3: Sales velocity from raw_order_items (units sold per day). CTE 4: Estimated days_of_stock (quantity_on_hand / daily_sales_rate). Final: product name, supplier name, warehouse city, quantity_on_hand, daily_sales_rate, days_of_stock, needs_reorder (days_of_stock < lead_time_days).",
        "model_name": "supply_chain_overview",
        "expected_columns": ["product_id", "quantity_on_hand", "days_of_stock", "needs_reorder"],
    },
]

# Verify count
assert len(EVAL_PROMPTS) == 60, f"Expected 60 prompts, got {len(EVAL_PROMPTS)}"
