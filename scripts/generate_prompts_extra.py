"""
Generate additional training prompts using templates.
Supplements generate_prompts.py to reach 500+ total.
"""

import json
import os
import random

random.seed(42)

def src(table):
    return "{{ source('jaffle_shop', '" + table + "') }}"

extra_prompts = []

# Table pairs for systematic join generation
TABLE_PAIRS = [
    ("raw_orders", "raw_customers", "customer_id", "customer_id"),
    ("raw_order_items", "raw_orders", "order_id", "order_id"),
    ("raw_payments", "raw_orders", "order_id", "order_id"),
    ("raw_shipping", "raw_orders", "order_id", "order_id"),
    ("raw_refunds", "raw_orders", "order_id", "order_id"),
    ("raw_reviews", "raw_products", "product_id", "product_id"),
    ("raw_reviews", "raw_customers", "customer_id", "customer_id"),
    ("raw_sessions", "raw_customers", "customer_id", "customer_id"),
    ("raw_support_tickets", "raw_customers", "customer_id", "customer_id"),
    ("raw_email_events", "raw_campaigns", "campaign_id", "campaign_id"),
    ("raw_email_events", "raw_customers", "customer_id", "customer_id"),
    ("raw_subscriptions", "raw_customers", "customer_id", "customer_id"),
    ("raw_inventory", "raw_products", "product_id", "product_id"),
    ("raw_inventory", "raw_warehouses", "warehouse_id", "warehouse_id"),
    ("raw_returns", "raw_order_items", "order_item_id", "order_item_id"),
    ("raw_order_items", "raw_products", "product_id", "product_id"),
    ("raw_support_tickets", "raw_orders", "order_id", "order_id"),
    ("raw_page_views", "raw_sessions", "session_id", "session_id"),
]

# Aggregation columns by table
AGG_COLS = {
    "raw_orders": ("amount", "order_date", "status", "channel", "shipping_method"),
    "raw_payments": ("amount", "payment_method", "status"),
    "raw_order_items": ("quantity", "unit_price", "discount_pct"),
    "raw_reviews": ("rating",),
    "raw_sessions": ("device_type", "channel"),
    "raw_shipping": ("shipping_cost", "weight_kg", "carrier"),
    "raw_support_tickets": ("category", "priority", "status"),
    "raw_subscriptions": ("plan", "mrr", "status"),
    "raw_inventory": ("quantity_on_hand", "reorder_point"),
    "raw_email_events": ("event_type",),
    "raw_refunds": ("refund_amount", "reason", "status"),
    "raw_returns": ("refund_amount", "reason", "condition"),
    "raw_promotions": ("discount_value", "times_used", "promotion_type"),
}

# =====================================================================
# ADDITIONAL TWO-TABLE JOINS (16 more)
# =====================================================================

extra_two_table = [
    {
        "prompt": f"Write a dbt model called orders_not_shipped. LEFT JOIN {src('raw_orders')} with {src('raw_shipping')} on order_id. Filter to orders that have no shipping record. Include order_id, customer_id, order_date, amount_dollars, status.",
        "model_name": "orders_not_shipped",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars","status"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called orders_not_paid. LEFT JOIN {src('raw_orders')} with {src('raw_payments')} on order_id. Filter to orders with no successful payment (payment status != 'success' or no payment at all). Include order_id, customer_id, order_date, amount_dollars.",
        "model_name": "orders_not_paid",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_category_revenue. Join {src('raw_order_items')} and {src('raw_products')} on product_id. Group by category and subcategory. Sum line_total_dollars (quantity * unit_price / 100). Include category, subcategory, total_revenue, order_count.",
        "model_name": "product_category_revenue",
        "expected_columns": ["category","subcategory","total_revenue","order_count"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_order_frequency. Join {src('raw_customers')} and {src('raw_orders')} on customer_id. Calculate avg_days_between_orders for each customer using LAG on order dates. Include customer_id, first_name, order_count, avg_days_between_orders.",
        "model_name": "customer_order_frequency",
        "expected_columns": ["customer_id","first_name","order_count","avg_days_between_orders"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called monthly_active_customers. Join {src('raw_orders')} with {src('raw_customers')} on customer_id. Group by month (DATE_TRUNC order_date to month). Count distinct customers per month. Include order_month, active_customers, total_revenue_dollars.",
        "model_name": "monthly_active_customers",
        "expected_columns": ["order_month","active_customers","total_revenue_dollars"],
        "expected_row_count": None,
        "unique_key": "order_month",
    },
    {
        "prompt": f"Write a dbt model called session_conversion. Join {src('raw_sessions')} and {src('raw_orders')} on customer_id, where the order was placed on the same day as the session. Calculate conversion_rate (sessions with orders / total sessions). Group by channel. Include channel, total_sessions, converted_sessions, conversion_rate.",
        "model_name": "session_conversion",
        "expected_columns": ["channel","total_sessions","converted_sessions","conversion_rate"],
        "expected_row_count": None,
        "unique_key": "channel",
    },
    {
        "prompt": f"Write a dbt model called customer_first_last_order. From {src('raw_orders')}, for each customer_id, find first_order_date (MIN), last_order_date (MAX), total_orders (COUNT), total_spend_dollars (SUM amount/100). Include customer_id.",
        "model_name": "customer_first_last_order",
        "expected_columns": ["customer_id","first_order_date","last_order_date","total_orders","total_spend_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called repeat_customers. From {src('raw_orders')}, find customers with more than one order. Include customer_id, order_count, first_order_date, last_order_date, total_spend_dollars. Use HAVING.",
        "model_name": "repeat_customers",
        "expected_columns": ["customer_id","order_count","first_order_date","last_order_date","total_spend_dollars"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called one_time_customers. From {src('raw_orders')}, find customers with exactly one order. Include customer_id, order_date, amount_dollars. Use HAVING COUNT(*) = 1.",
        "model_name": "one_time_customers",
        "expected_columns": ["customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called top_spending_customers. Join {src('raw_customers')} and {src('raw_orders')} on customer_id. Sum order amounts per customer. Filter to top 10 by total spend. Include customer_id, full_name, email, total_spend_dollars. Order by total_spend desc.",
        "model_name": "top_spending_customers",
        "expected_columns": ["customer_id","full_name","email","total_spend_dollars"],
        "expected_row_count": 10,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called avg_order_by_shipping. From {src('raw_orders')}, group by shipping_method. Include shipping_method, order_count, avg_amount_dollars, min_amount_dollars, max_amount_dollars.",
        "model_name": "avg_order_by_shipping",
        "expected_columns": ["shipping_method","order_count","avg_amount_dollars","min_amount_dollars","max_amount_dollars"],
        "expected_row_count": 3,
        "unique_key": "shipping_method",
    },
    {
        "prompt": f"Write a dbt model called product_return_rate. Join {src('raw_order_items')}, {src('raw_products')}, and {src('raw_returns')}. For each product: total_sold (sum qty), total_returned (count returns), return_rate (returned/sold). Include product_id, name, total_sold, total_returned, return_rate. Use CTEs.",
        "model_name": "product_return_rate",
        "expected_columns": ["product_id","name","total_sold","total_returned","return_rate"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_state_summary. Join {src('raw_customers')} and {src('raw_addresses')} on customer_id. Filter to default shipping addresses. Group by state. Include state, customer_count. Order by customer_count desc.",
        "model_name": "customer_state_summary",
        "expected_columns": ["state","customer_count"],
        "expected_row_count": None,
        "unique_key": "state",
    },
    {
        "prompt": f"Write a dbt model called revenue_by_state. Join {src('raw_orders')}, {src('raw_customers')}, and {src('raw_addresses')}. Group by state. Include state, total_orders, total_revenue_dollars, avg_order_dollars. Use CTEs.",
        "model_name": "revenue_by_state",
        "expected_columns": ["state","total_orders","total_revenue_dollars","avg_order_dollars"],
        "expected_row_count": None,
        "unique_key": "state",
    },
    {
        "prompt": f"Write a dbt model called session_channel_summary. From {src('raw_sessions')}, group by channel. Include channel, session_count, avg_duration_minutes, customers_count (distinct customer_id, excluding NULL).",
        "model_name": "session_channel_summary",
        "expected_columns": ["channel","session_count","avg_duration_minutes","customers_count"],
        "expected_row_count": 6,
        "unique_key": "channel",
    },
    {
        "prompt": f"Write a dbt model called promotion_type_summary. From {src('raw_promotions')}, group by promotion_type. Include promotion_type, promo_count, avg_discount_value, total_times_used, active_count.",
        "model_name": "promotion_type_summary",
        "expected_columns": ["promotion_type","promo_count","avg_discount_value","total_times_used","active_count"],
        "expected_row_count": 3,
        "unique_key": "promotion_type",
    },
]
extra_prompts.extend(extra_two_table)

# =====================================================================
# ADDITIONAL THREE-TABLE JOINS (25 more)
# =====================================================================

extra_three_table = [
    {
        "prompt": f"Write a dbt model called customer_order_payments. Join {src('raw_customers')}, {src('raw_orders')}, and {src('raw_payments')}. For each customer: total orders, total payments, total refund_payments (is_refund = true), net_revenue_dollars. Use CTEs.",
        "model_name": "customer_order_payments",
        "expected_columns": ["customer_id","first_name","total_orders","total_payments","net_revenue_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_items_shipped. Join {src('raw_order_items')}, {src('raw_orders')}, and {src('raw_shipping')}. Include order_item_id, product_id, quantity, order_date, carrier, shipped_date, delivered_date. Use CTEs.",
        "model_name": "order_items_shipped",
        "expected_columns": ["order_item_id","product_id","quantity","order_date","carrier","shipped_date"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called customer_reviews_products. Join {src('raw_customers')}, {src('raw_reviews')}, and {src('raw_products')}. Include customer full_name, product_name, rating, review_text, is_verified_purchase. Use CTEs.",
        "model_name": "customer_reviews_products",
        "expected_columns": ["full_name","product_name","rating","review_text","is_verified_purchase"],
        "expected_row_count": 60,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called inventory_alerts. Join {src('raw_inventory')}, {src('raw_products')}, and {src('raw_warehouses')}. Only include items where quantity_on_hand < reorder_point. Include product_name, warehouse_name, quantity_on_hand, reorder_point, shortage (reorder_point - quantity_on_hand). Use CTEs.",
        "model_name": "inventory_alerts",
        "expected_columns": ["product_name","warehouse_name","quantity_on_hand","reorder_point","shortage"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_session_pages. Join {src('raw_customers')}, {src('raw_sessions')}, and {src('raw_page_views')}. For each customer: total_sessions, total_page_views, avg_pages_per_session, most_common_device. Use CTEs.",
        "model_name": "customer_session_pages",
        "expected_columns": ["customer_id","first_name","total_sessions","total_page_views","avg_pages_per_session"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_product_reviews. Join {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_reviews')} (on product_id). For each order, check if any product was reviewed. Include order_id, order_date, items_count, reviewed_items_count. Use CTEs.",
        "model_name": "order_product_reviews",
        "expected_columns": ["order_id","order_date","items_count","reviewed_items_count"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called subscription_order_value. Join {src('raw_subscriptions')}, {src('raw_customers')}, and {src('raw_orders')}. For active subscribers, sum orders during subscription period. Include subscription_id, customer_name, plan, mrr_dollars, orders_during_sub, total_order_value. Use CTEs.",
        "model_name": "subscription_order_value",
        "expected_columns": ["subscription_id","customer_name","plan","mrr_dollars","orders_during_sub"],
        "expected_row_count": None,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called email_campaign_revenue. Join {src('raw_email_events')}, {src('raw_customers')}, and {src('raw_orders')}. For each email event where event_type = 'clicked', check if customer ordered within 7 days. Include event_id, customer_id, campaign_id, click_date, order_id, order_amount. Use CTEs.",
        "model_name": "email_campaign_revenue",
        "expected_columns": ["event_id","customer_id","campaign_id","click_date","order_id"],
        "expected_row_count": None,
        "unique_key": "event_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_order_value. Join {src('raw_support_tickets')}, {src('raw_orders')}, and {src('raw_customers')}. Include ticket_id, customer_name, ticket_category, priority, order_amount_dollars. Calculate if ticket is for a high-value order (>$200). Use CTEs.",
        "model_name": "ticket_order_value",
        "expected_columns": ["ticket_id","customer_name","ticket_category","priority","order_amount_dollars"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called product_supplier_inventory. Join {src('raw_products')}, {src('raw_suppliers')}, and {src('raw_inventory')}. Assume product_id 1-10 come from supplier 1-3, 11-20 from supplier 4-7, 21-30 from supplier 8-10 (use CASE). Include product_name, supplier_name, total_stock, lead_time_days. Use CTEs.",
        "model_name": "product_supplier_inventory",
        "expected_columns": ["product_name","supplier_name","total_stock","lead_time_days"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_address_orders. Join {src('raw_customers')}, {src('raw_addresses')}, and {src('raw_orders')}. For each customer, show their default shipping city/state alongside their order count and total spend. Use CTEs with LEFT JOINs.",
        "model_name": "customer_address_orders",
        "expected_columns": ["customer_id","full_name","city","state","order_count","total_spend_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called daily_channel_revenue. Join {src('raw_orders')} and {src('raw_payments')} on order_id. Group by order_date and channel. Include order_date, channel, order_count, paid_revenue_dollars (only success payments). Use CTEs.",
        "model_name": "daily_channel_revenue",
        "expected_columns": ["order_date","channel","order_count","paid_revenue_dollars"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called warehouse_stock_value. Join {src('raw_inventory')}, {src('raw_products')}, and {src('raw_warehouses')}. For each warehouse, calculate total_stock_value (sum of quantity_on_hand * unit_price / 100). Include warehouse_name, city, total_items, total_stock_value_dollars. Use CTEs.",
        "model_name": "warehouse_stock_value",
        "expected_columns": ["warehouse_name","city","total_items","total_stock_value_dollars"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id",
    },
    {
        "prompt": f"Write a dbt model called customer_refund_history. Join {src('raw_customers')}, {src('raw_orders')}, and {src('raw_refunds')}. For each customer: total_orders, orders_refunded, total_refund_dollars, refund_rate (orders_refunded/total_orders). Use CTEs.",
        "model_name": "customer_refund_history",
        "expected_columns": ["customer_id","first_name","total_orders","orders_refunded","total_refund_dollars","refund_rate"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called product_order_shipping. Join {src('raw_products')}, {src('raw_order_items')}, and {src('raw_shipping')} (via orders). For each product: times_shipped, avg_delivery_days, avg_shipping_cost. Use CTEs.",
        "model_name": "product_order_shipping",
        "expected_columns": ["product_id","product_name","times_shipped","avg_delivery_days","avg_shipping_cost"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
]
extra_prompts.extend(extra_three_table)

# =====================================================================
# ADDITIONAL WINDOW FUNCTIONS (25 more)
# =====================================================================

extra_window = [
    {
        "prompt": f"Write a dbt model called customer_order_gaps using {src('raw_orders')}. For each customer, calculate the gap in days between consecutive orders using LAG. Include order_id, customer_id, order_date, prev_order_date, gap_days.",
        "model_name": "customer_order_gaps",
        "expected_columns": ["order_id","customer_id","order_date","prev_order_date","gap_days"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_revenue_rank. Join {src('raw_order_items')} and {src('raw_products')}. Calculate total_revenue per product, then assign an overall revenue rank using RANK(). Include product_id, name, total_revenue, revenue_rank.",
        "model_name": "product_revenue_rank",
        "expected_columns": ["product_id","name","total_revenue","revenue_rank"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called running_customer_count using {src('raw_customers')}. Order by created_at. Calculate running count of customers signed up over time. Include customer_id, created_at, running_customer_count.",
        "model_name": "running_customer_count",
        "expected_columns": ["customer_id","created_at","running_customer_count"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_amount_vs_avg using {src('raw_orders')}. For each order, calculate the customer's avg order amount using AVG() OVER (PARTITION BY customer_id). Include order_id, customer_id, amount_dollars, customer_avg_dollars, diff_from_avg.",
        "model_name": "order_amount_vs_avg",
        "expected_columns": ["order_id","customer_id","amount_dollars","customer_avg_dollars","diff_from_avg"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called daily_revenue_cumulative. Group {src('raw_orders')} by order_date. Calculate daily_revenue_dollars and cumulative_revenue using SUM() OVER (ORDER BY order_date). Include order_date, daily_revenue, cumulative_revenue.",
        "model_name": "daily_revenue_cumulative",
        "expected_columns": ["order_date","daily_revenue","cumulative_revenue"],
        "expected_row_count": None,
        "unique_key": "order_date",
    },
    {
        "prompt": f"Write a dbt model called review_rank_by_product using {src('raw_reviews')}. For each product, rank reviews by rating desc, then by created_at asc. Include review_id, product_id, rating, created_at, review_rank.",
        "model_name": "review_rank_by_product",
        "expected_columns": ["review_id","product_id","rating","review_rank"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called session_duration_percentile using {src('raw_sessions')}. Calculate session_duration_minutes. Use PERCENT_RANK() to get duration_percentile. Include session_id, customer_id, duration_minutes, duration_percentile.",
        "model_name": "session_duration_percentile",
        "expected_columns": ["session_id","customer_id","duration_minutes","duration_percentile"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_resolution_rank using {src('raw_support_tickets')}. For resolved tickets, rank by resolution time (fastest first). Calculate resolution_hours as diff between resolved_at and created_at. Include ticket_id, category, resolution_hours, speed_rank.",
        "model_name": "ticket_resolution_rank",
        "expected_columns": ["ticket_id","category","resolution_hours","speed_rank"],
        "expected_row_count": None,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called monthly_new_customers using {src('raw_customers')}. Group by month (DATE_TRUNC created_at). Count new customers per month. Use SUM() OVER to calculate running total. Include signup_month, new_customers, cumulative_customers.",
        "model_name": "monthly_new_customers",
        "expected_columns": ["signup_month","new_customers","cumulative_customers"],
        "expected_row_count": None,
        "unique_key": "signup_month",
    },
    {
        "prompt": f"Write a dbt model called order_recency_score using {src('raw_orders')}. For each customer, use ROW_NUMBER() desc on order_date to get recency (1 = most recent). Also calculate days_since_order as current_date - order_date. Include order_id, customer_id, order_date, recency_rank, days_since_order.",
        "model_name": "order_recency_score",
        "expected_columns": ["order_id","customer_id","order_date","recency_rank","days_since_order"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called page_view_sequence using {src('raw_page_views')}. For each session, number the page views in order (ROW_NUMBER by viewed_at). Include page_view_id, session_id, page_url, viewed_at, view_sequence.",
        "model_name": "page_view_sequence",
        "expected_columns": ["page_view_id","session_id","page_url","viewed_at","view_sequence"],
        "expected_row_count": 300,
        "unique_key": "page_view_id",
    },
    {
        "prompt": f"Write a dbt model called payment_amount_rank using {src('raw_payments')}. Rank all payments by amount desc using DENSE_RANK. Include payment_id, order_id, amount_dollars, payment_method, amount_rank.",
        "model_name": "payment_amount_rank",
        "expected_columns": ["payment_id","order_id","amount_dollars","payment_method","amount_rank"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_delivery_rank using {src('raw_shipping')}. For delivered shipments, rank by delivery speed (delivery_days asc) within each carrier. Include shipment_id, carrier, delivery_days, speed_rank_within_carrier.",
        "model_name": "shipping_delivery_rank",
        "expected_columns": ["shipment_id","carrier","delivery_days","speed_rank_within_carrier"],
        "expected_row_count": None,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_change_detection using {src('raw_inventory')}. Partition by product_id. Use LAG on quantity_on_hand (ordered by updated_at) to detect changes. Include inventory_id, product_id, quantity_on_hand, prev_quantity, quantity_change.",
        "model_name": "inventory_change_detection",
        "expected_columns": ["inventory_id","product_id","quantity_on_hand","prev_quantity","quantity_change"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
    {
        "prompt": f"Write a dbt model called email_engagement_sequence using {src('raw_email_events')}. For each customer-campaign pair, number events in order. Include event_id, customer_id, campaign_id, event_type, event_timestamp, event_sequence.",
        "model_name": "email_engagement_sequence",
        "expected_columns": ["event_id","customer_id","campaign_id","event_type","event_sequence"],
        "expected_row_count": 100,
        "unique_key": "event_id",
    },
]
extra_prompts.extend(extra_window)

# =====================================================================
# ADDITIONAL CASE/CONDITIONAL (15 more)
# =====================================================================

extra_case = [
    {
        "prompt": f"Write a dbt model called order_discount_flag using {src('raw_orders')}. Add boolean columns: has_discount (discount_amount > 0), has_notes (notes IS NOT NULL AND notes != ''), is_high_tax (tax_amount > 2000). Include order_id, amount_dollars.",
        "model_name": "order_discount_flag",
        "expected_columns": ["order_id","amount_dollars","has_discount","has_notes","is_high_tax"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_value_segment. Join {src('raw_customers')} and {src('raw_orders')}. Segment: 'platinum' (total_spend > $500), 'gold' ($200-$500), 'silver' ($50-$200), 'bronze' (< $50 or no orders). Include customer_id, first_name, total_spend_dollars, segment.",
        "model_name": "customer_value_segment",
        "expected_columns": ["customer_id","first_name","total_spend_dollars","segment"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called product_margin_tier using {src('raw_products')}. Calculate margin_pct = (unit_price - cost) / unit_price * 100. Categorize: 'high_margin' (>50%), 'medium_margin' (30-50%), 'low_margin' (<30%). Include product_id, name, margin_pct, margin_tier.",
        "model_name": "product_margin_tier",
        "expected_columns": ["product_id","name","margin_pct","margin_tier"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called review_quality using {src('raw_reviews')}. Categorize: 'detailed' (review_text length > 50 chars), 'brief' (1-50 chars), 'no_text' (NULL or empty). Include review_id, product_id, rating, review_quality, text_length.",
        "model_name": "review_quality",
        "expected_columns": ["review_id","product_id","rating","review_quality","text_length"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called order_fiscal_quarter using {src('raw_orders')}. Map order_date to fiscal quarter (fiscal year starts April 1). Include order_id, order_date, fiscal_year, fiscal_quarter, amount_dollars.",
        "model_name": "order_fiscal_quarter",
        "expected_columns": ["order_id","order_date","fiscal_year","fiscal_quarter","amount_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_churn_risk. Join {src('raw_customers')} and {src('raw_orders')}. Calculate days_since_last_order. Flag: 'high_risk' (>180 days), 'medium_risk' (90-180), 'low_risk' (<90), 'no_orders' (NULL). Include customer_id, first_name, days_since_last_order, churn_risk.",
        "model_name": "customer_churn_risk",
        "expected_columns": ["customer_id","first_name","days_since_last_order","churn_risk"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called payment_risk_flag using {src('raw_payments')}. Flag payments: 'suspicious' (amount > 30000 or payment_method = 'gift_card' with amount > 5000), 'normal' otherwise. Include payment_id, order_id, amount_dollars, payment_method, risk_flag.",
        "model_name": "payment_risk_flag",
        "expected_columns": ["payment_id","order_id","amount_dollars","payment_method","risk_flag"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called session_time_of_day using {src('raw_sessions')}. Extract hour from session_start. Categorize: 'morning' (6-11), 'afternoon' (12-17), 'evening' (18-22), 'night' (23-5). Include session_id, session_start, time_of_day, device_type.",
        "model_name": "session_time_of_day",
        "expected_columns": ["session_id","session_start","time_of_day","device_type"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called order_priority using {src('raw_orders')}. Assign priority: 'rush' (overnight shipping), 'expedited' (express), 'standard' (standard). If status is 'cancelled' or 'returned', set priority to 'n/a'. Include order_id, shipping_method, status, priority.",
        "model_name": "order_priority",
        "expected_columns": ["order_id","shipping_method","status","priority"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_weight_class using {src('raw_products')}. Categorize: 'lightweight' (<1kg), 'medium' (1-5kg), 'heavy' (>5kg). Include product_id, name, weight_kg, weight_class.",
        "model_name": "product_weight_class",
        "expected_columns": ["product_id","name","weight_kg","weight_class"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
]
extra_prompts.extend(extra_case)

# =====================================================================
# ADDITIONAL UNIONs (10 more)
# =====================================================================

extra_unions = [
    {
        "prompt": f"Write a dbt model called all_money_movements. UNION payments (positive amounts), refunds (negative), and returns (negative). Each row: movement_date (DATE), movement_type, amount_dollars, reference_id. Cast all dates consistently.",
        "model_name": "all_money_movements",
        "expected_columns": ["movement_date","movement_type","amount_dollars","reference_id"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_user_actions. UNION orders (customer_id, order_date, 'purchase'), reviews (customer_id, created_at, 'review'), support_tickets (customer_id, created_at, 'support_request'). Cast all dates to DATE. Include action_type.",
        "model_name": "all_user_actions",
        "expected_columns": ["customer_id","action_date","action_type"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_product_events. UNION order_items (product_id, created_at, 'sold'), returns (product_id via order_items, requested_at, 'returned'), reviews (product_id, created_at, 'reviewed'). Cast all dates to DATE. Include event_type.",
        "model_name": "all_product_events",
        "expected_columns": ["product_id","event_date","event_type"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called combined_addresses. UNION customer addresses (from raw_addresses) and warehouse addresses (from raw_warehouses). Include entity_type ('customer' or 'warehouse'), entity_id, city, state. Standardize column names.",
        "model_name": "combined_addresses",
        "expected_columns": ["entity_type","entity_id","city","state"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_notifications. UNION email events (event_type = sent/opened/clicked, event_timestamp) and support ticket notifications (status changes, created_at). Include notification_type, customer_id, notification_date. Cast to DATE.",
        "model_name": "all_notifications",
        "expected_columns": ["notification_type","customer_id","notification_date"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called daily_event_counts. First UNION all events (orders, payments, sessions, tickets) with a date and type. Then GROUP BY date and type. Include event_date, event_type, event_count. Use a CTE for the UNION, then aggregate.",
        "model_name": "daily_event_counts",
        "expected_columns": ["event_date","event_type","event_count"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_entity_names. UNION customer names (first_name || ' ' || last_name), employee names (first_name || ' ' || last_name), supplier names (name), and campaign names (name). Include entity_type and full_name.",
        "model_name": "all_entity_names",
        "expected_columns": ["entity_type","full_name"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called revenue_and_costs. UNION order revenue (order_date, amount/100 as amount, 'revenue') with shipping costs (shipped_date, shipping_cost/100, 'cost') and refund costs (requested_at, refund_amount/100, 'refund'). All as amount_dollars. Cast dates to DATE.",
        "model_name": "revenue_and_costs",
        "expected_columns": ["transaction_date","amount_dollars","transaction_type"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_ids. UNION all id columns from all tables with their source table name. Include source_table, id_value. This creates a master ID registry. Use UNION ALL.",
        "model_name": "all_ids",
        "expected_columns": ["source_table","id_value"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_timeline. UNION signup (created_at, 'signup'), first_order (min order_date, 'first_order'), first_review (min review created_at, 'first_review'), first_ticket (min ticket created_at, 'first_ticket'). For each customer. Cast all to DATE. Use CTEs.",
        "model_name": "customer_timeline",
        "expected_columns": ["customer_id","event_date","event_type"],
        "expected_row_count": None,
        "unique_key": None,
    },
]
extra_prompts.extend(extra_unions)

# =====================================================================
# ADDITIONAL INCREMENTAL (10 more)
# =====================================================================

extra_incremental = [
    {
        "prompt": f"Write a dbt incremental model called incremental_orders_v2 from {src('raw_orders')}. Use delete+insert strategy with unique_key = 'id'. Load based on order_date. Convert amount to dollars. Include order_id (renamed from id), customer_id, order_date, status, amount_dollars.",
        "model_name": "incremental_orders_v2",
        "expected_columns": ["order_id","customer_id","order_date","status","amount_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_reviews from {src('raw_reviews')}. Merge on id. Load based on created_at. Include all columns. Add a loaded_at column with current_timestamp.",
        "model_name": "incremental_reviews",
        "expected_columns": ["id","customer_id","product_id","rating","created_at","loaded_at"],
        "expected_row_count": 60,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_support_tickets from {src('raw_support_tickets')}. Merge on id. Load based on created_at. Include all columns.",
        "model_name": "incremental_support_tickets",
        "expected_columns": ["id","customer_id","category","priority","status","created_at"],
        "expected_row_count": 40,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_shipping from {src('raw_shipping')}. Merge on id. Load based on shipped_date. Include all columns.",
        "model_name": "incremental_shipping",
        "expected_columns": ["id","order_id","carrier","shipped_date","status"],
        "expected_row_count": 75,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_inventory from {src('raw_inventory')}. Merge on id. Load based on updated_at. Include all columns.",
        "model_name": "incremental_inventory",
        "expected_columns": ["id","product_id","warehouse_id","quantity_on_hand","updated_at"],
        "expected_row_count": 50,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_returns from {src('raw_returns')}. Merge on id. Load based on requested_at. Include all columns.",
        "model_name": "incremental_returns",
        "expected_columns": ["id","order_item_id","reason","status","requested_at"],
        "expected_row_count": 30,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_subscriptions from {src('raw_subscriptions')}. Merge on id. Load based on start_date. Include all columns plus a loaded_at timestamp.",
        "model_name": "incremental_subscriptions",
        "expected_columns": ["id","customer_id","plan","start_date","status","loaded_at"],
        "expected_row_count": 25,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_order_items from {src('raw_order_items')}. Merge on id. Load based on created_at. Include all columns.",
        "model_name": "incremental_order_items",
        "expected_columns": ["id","order_id","product_id","quantity","unit_price","created_at"],
        "expected_row_count": 200,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_refunds from {src('raw_refunds')}. Merge on id. Load based on requested_at. Include all columns.",
        "model_name": "incremental_refunds",
        "expected_columns": ["id","order_id","reason","refund_amount","status","requested_at"],
        "expected_row_count": 25,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_campaigns from {src('raw_campaigns')}. Merge on id. Load based on start_date. Include all columns.",
        "model_name": "incremental_campaigns",
        "expected_columns": ["id","name","channel","start_date","end_date","budget","status"],
        "expected_row_count": 12,
        "unique_key": "id",
    },
]
extra_prompts.extend(extra_incremental)

# =====================================================================
# ADDITIONAL JINJA PATTERNS (10 more)
# =====================================================================

extra_jinja = [
    {
        "prompt": f"Write a dbt model called parameterized_customers from {src('raw_customers')}. Use {{{{ var('customer_type_filter', 'individual') }}}} to filter by customer_type. Include customer_id, first_name, last_name, email, customer_type.",
        "model_name": "parameterized_customers",
        "expected_columns": ["customer_id","first_name","last_name","email","customer_type"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_target_check from {src('raw_orders')}. Use {{%- if target.name == 'prod' -%}} to filter to completed orders only in prod, but include all orders in dev. Include order_id, customer_id, order_date, status, amount_dollars.",
        "model_name": "orders_with_target_check",
        "expected_columns": ["order_id","customer_id","order_date","status","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called pivoted_ticket_priorities from {src('raw_support_tickets')}. Use Jinja to pivot priorities into columns. For each category, show low_count, medium_count, high_count, urgent_count. Use a Jinja for loop over priority values.",
        "model_name": "pivoted_ticket_priorities",
        "expected_columns": ["category","low_count","medium_count","high_count","urgent_count"],
        "expected_row_count": 7,
        "unique_key": "category",
    },
    {
        "prompt": f"Write a dbt model called dynamic_aggregation from {src('raw_orders')}. Use {{{{ var('group_by_col', 'channel') }}}} to dynamically group orders. Include the grouping column, order_count, total_amount_dollars.",
        "model_name": "dynamic_aggregation",
        "expected_columns": ["group_key","order_count","total_amount_dollars"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called multi_config from {src('raw_products')}. Use {{{{ config(materialized='table', tags=['marts', 'products']) }}}}. Include product_id, name, category, unit_price_dollars, is_active. Filter to active products.",
        "model_name": "multi_config",
        "expected_columns": ["product_id","name","category","unit_price_dollars","is_active"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called cents_to_dollars from {src('raw_orders')}. Write a Jinja macro called cents_to_dollars that divides a column by 100.0. Use it for amount, discount_amount, and tax_amount. Include order_id, amount_dollars, discount_dollars, tax_dollars.",
        "model_name": "cents_to_dollars",
        "expected_columns": ["order_id","amount_dollars","discount_dollars","tax_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called star_rating from {src('raw_reviews')}. Use Jinja to generate a star_display column that shows rating as stars (e.g., rating 4 = '****'). Use REPEAT('*', rating). Include review_id, product_id, rating, star_display.",
        "model_name": "star_rating",
        "expected_columns": ["review_id","product_id","rating","star_display"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called ephemeral_customer_base. Use {{{{ config(materialized='ephemeral') }}}}. Select active customers from {src('raw_customers')} where is_active = true. Include customer_id, first_name, last_name, email, customer_type.",
        "model_name": "ephemeral_customer_base",
        "expected_columns": ["customer_id","first_name","last_name","email","customer_type"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called date_range_orders from {src('raw_orders')}. Use two Jinja variables: start_date (default '2023-01-01') and end_date (default '2024-12-31'). Filter orders between these dates. Include order_id, customer_id, order_date, amount_dollars.",
        "model_name": "date_range_orders",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called generated_columns from {src('raw_orders')}. Use a Jinja for loop to generate boolean columns for each channel value: is_web, is_mobile, is_phone, is_in_store. Include order_id and amount_dollars.",
        "model_name": "generated_columns",
        "expected_columns": ["order_id","amount_dollars","is_web","is_mobile","is_phone"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
]
extra_prompts.extend(extra_jinja)

# =====================================================================
# ADDITIONAL DATE/STRING (15 more)
# =====================================================================

extra_date_string = [
    {
        "prompt": f"Write a dbt model called customer_full_name from {src('raw_customers')}. Create full_name (first_name || ' ' || last_name), uppercase_name (UPPER(full_name)), email_username (part before @ in email). Include customer_id.",
        "model_name": "customer_full_name",
        "expected_columns": ["customer_id","full_name","uppercase_name","email_username"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_year_month from {src('raw_orders')}. Extract year, month, and create year_month string (YYYY-MM format). Include order_id, order_date, order_year, order_month, year_month.",
        "model_name": "orders_year_month",
        "expected_columns": ["order_id","order_date","order_year","order_month","year_month"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_signup_day from {src('raw_customers')}. Extract day of week from created_at. Include customer_id, first_name, created_at, signup_day_name (Monday, Tuesday, etc.).",
        "model_name": "customer_signup_day",
        "expected_columns": ["customer_id","first_name","created_at","signup_day_name"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called review_text_length from {src('raw_reviews')}. Calculate text_length (LENGTH of review_text, 0 if NULL). Include review_id, product_id, rating, text_length, has_text (boolean).",
        "model_name": "review_text_length",
        "expected_columns": ["review_id","product_id","rating","text_length","has_text"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called order_age_days from {src('raw_orders')}. Calculate order_age_days as current_date - order_date. Calculate order_age_months as order_age_days / 30. Include order_id, order_date, order_age_days, order_age_months.",
        "model_name": "order_age_days",
        "expected_columns": ["order_id","order_date","order_age_days","order_age_months"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called session_date_parts from {src('raw_sessions')}. Extract from session_start: date, hour, day_of_week, month, year. Include session_id, session_date, session_hour, session_day_of_week, session_month, session_year.",
        "model_name": "session_date_parts",
        "expected_columns": ["session_id","session_date","session_hour","session_day_of_week","session_month","session_year"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called product_name_parts from {src('raw_products')}. Use string functions to extract: name_length (LENGTH), first_word (SPLIT_PART or SUBSTRING), has_parentheses (LIKE '%(%'). Include product_id, name, name_length, first_word, has_parentheses.",
        "model_name": "product_name_parts",
        "expected_columns": ["product_id","name","name_length","first_word","has_parentheses"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called employee_tenure from {src('raw_employees')}. Calculate tenure_days (current_date - hire_date), tenure_years (tenure_days / 365.25). Include employee_id, full_name, hire_date, tenure_days, tenure_years.",
        "model_name": "employee_tenure",
        "expected_columns": ["employee_id","full_name","hire_date","tenure_days","tenure_years"],
        "expected_row_count": 15,
        "unique_key": "employee_id",
    },
    {
        "prompt": f"Write a dbt model called campaign_duration from {src('raw_campaigns')}. Calculate duration_days (end_date - start_date). Include campaign_id, name, start_date, end_date, duration_days, is_active (status = 'active').",
        "model_name": "campaign_duration",
        "expected_columns": ["campaign_id","name","start_date","end_date","duration_days","is_active"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called subscription_duration from {src('raw_subscriptions')}. Calculate duration_days (end_date - start_date, NULL if no end_date). Calculate duration_months (duration_days / 30). Include subscription_id, customer_id, plan, duration_days, duration_months.",
        "model_name": "subscription_duration",
        "expected_columns": ["subscription_id","customer_id","plan","duration_days","duration_months"],
        "expected_row_count": 25,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_tracking_parsed from {src('raw_shipping')}. Extract carrier_code (first 2 chars of tracking_number) and tracking_digits (remaining digits). Include shipment_id, tracking_number, carrier_code, tracking_digits, carrier.",
        "model_name": "shipping_tracking_parsed",
        "expected_columns": ["shipment_id","tracking_number","carrier_code","tracking_digits","carrier"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called orders_week_number from {src('raw_orders')}. Extract ISO week number and year from order_date. Include order_id, order_date, order_year, week_number, year_week (format: 'YYYY-W##').",
        "model_name": "orders_week_number",
        "expected_columns": ["order_id","order_date","order_year","week_number","year_week"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called address_full from {src('raw_addresses')}. Concatenate street, city, state, zip_code into full_address with comma separators. Include address_id, customer_id, full_address, address_type.",
        "model_name": "address_full",
        "expected_columns": ["address_id","customer_id","full_address","address_type"],
        "expected_row_count": 60,
        "unique_key": "address_id",
    },
    {
        "prompt": f"Write a dbt model called promotion_validity from {src('raw_promotions')}. Calculate days_remaining (end_date - current_date, NULL if expired). Add is_expired boolean (end_date < current_date). Include promotion_id, code, start_date, end_date, days_remaining, is_expired.",
        "model_name": "promotion_validity",
        "expected_columns": ["promotion_id","code","start_date","end_date","days_remaining","is_expired"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_response_time from {src('raw_support_tickets')}. Calculate response_hours as EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600. Handle NULLs for unresolved. Include ticket_id, category, priority, response_hours.",
        "model_name": "ticket_response_time",
        "expected_columns": ["ticket_id","category","priority","response_hours"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
]
extra_prompts.extend(extra_date_string)

# =====================================================================
# ADDITIONAL NULL/DEFENSIVE (8 more)
# =====================================================================

extra_null = [
    {
        "prompt": f"Write a dbt model called shipping_safe from {src('raw_shipping')}. COALESCE delivered_date with 'Not delivered'. Calculate delivery_days only when delivered_date is not NULL, else NULL. COALESCE shipping_cost with 0. Include shipment_id, order_id, carrier, delivered_date, delivery_days, shipping_cost_dollars.",
        "model_name": "shipping_safe",
        "expected_columns": ["shipment_id","order_id","carrier","delivery_days","shipping_cost_dollars"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called promotions_clean from {src('raw_promotions')}. COALESCE max_uses with 999999 (unlimited). Calculate usage_pct as times_used / NULLIF(max_uses, 0). COALESCE min_order_amount with 0. Include promotion_id, code, max_uses, usage_pct.",
        "model_name": "promotions_clean",
        "expected_columns": ["promotion_id","code","max_uses","usage_pct"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt model called returns_safe from {src('raw_returns')}. COALESCE processed_at with 'Pending'. Calculate processing_days only when processed_at is not NULL (processed_at - requested_at). COALESCE refund_amount with 0. Include return_id, status, processing_days, refund_amount_dollars.",
        "model_name": "returns_safe",
        "expected_columns": ["return_id","status","processing_days","refund_amount_dollars"],
        "expected_row_count": 30,
        "unique_key": "return_id",
    },
    {
        "prompt": f"Write a dbt model called subscriptions_clean from {src('raw_subscriptions')}. COALESCE end_date with '9999-12-31' for active subscriptions. Calculate is_active (end_date IS NULL or end_date > current_date). COALESCE mrr with 0. Include subscription_id, customer_id, plan, end_date, is_active.",
        "model_name": "subscriptions_clean",
        "expected_columns": ["subscription_id","customer_id","plan","end_date","is_active"],
        "expected_row_count": 25,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called safe_division_metrics from {src('raw_orders')}. Calculate discount_rate as discount_amount / NULLIF(amount, 0), tax_rate as tax_amount / NULLIF(amount, 0). COALESCE both with 0. Include order_id, amount_dollars, discount_rate, tax_rate.",
        "model_name": "safe_division_metrics",
        "expected_columns": ["order_id","amount_dollars","discount_rate","tax_rate"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called reviews_clean from {src('raw_reviews')}. COALESCE review_text with 'No review text'. Add has_review_text boolean. COALESCE is_verified_purchase with false. Include review_id, customer_id, product_id, rating, review_text, has_review_text.",
        "model_name": "reviews_clean",
        "expected_columns": ["review_id","customer_id","product_id","rating","review_text","has_review_text"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called customers_clean from {src('raw_customers')}. COALESCE phone with 'No phone'. Replace empty string phone with 'No phone'. COALESCE referral_source with 'unknown'. Include customer_id, first_name, last_name, email, phone, referral_source.",
        "model_name": "customers_clean",
        "expected_columns": ["customer_id","first_name","last_name","email","phone","referral_source"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_notes_clean from {src('raw_orders')}. COALESCE notes with 'No notes'. TRIM whitespace from notes. Add has_notes boolean. Include order_id, customer_id, order_date, notes, has_notes.",
        "model_name": "orders_notes_clean",
        "expected_columns": ["order_id","customer_id","order_date","notes","has_notes"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
]
extra_prompts.extend(extra_null)

# =====================================================================
# ADDITIONAL MART MODELS (25 more)
# =====================================================================

extra_marts = [
    {
        "prompt": f"Write a dbt model called dim_employees from {src('raw_employees')}. Include employee_id, full_name (first || ' ' || last), role, department, hire_date, tenure_days (current_date - hire_date).",
        "model_name": "dim_employees",
        "expected_columns": ["employee_id","full_name","role","department","hire_date","tenure_days"],
        "expected_row_count": 15,
        "unique_key": "employee_id",
    },
    {
        "prompt": f"Write a dbt model called dim_warehouses from {src('raw_warehouses')}. Include warehouse_id, name, city, state, capacity. Add a location column (city || ', ' || state).",
        "model_name": "dim_warehouses",
        "expected_columns": ["warehouse_id","name","city","state","capacity","location"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id",
    },
    {
        "prompt": f"Write a dbt model called dim_suppliers from {src('raw_suppliers')}. Include supplier_id, name, contact_email, country, lead_time_days. Add is_domestic (country = 'US').",
        "model_name": "dim_suppliers",
        "expected_columns": ["supplier_id","name","contact_email","country","lead_time_days","is_domestic"],
        "expected_row_count": 10,
        "unique_key": "supplier_id",
    },
    {
        "prompt": f"Write a dbt model called dim_campaigns. Join {src('raw_campaigns')} with {src('raw_email_events')}. Include campaign_id, name, channel, start_date, end_date, budget_dollars, status, total_emails_sent, total_opens, total_clicks. Use CTEs.",
        "model_name": "dim_campaigns",
        "expected_columns": ["campaign_id","name","channel","budget_dollars","total_emails_sent","total_opens"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called dim_promotions from {src('raw_promotions')}. Include promotion_id, code, promotion_type, discount_value, start_date, end_date, max_uses, times_used, is_active, usage_rate (times_used / NULLIF(max_uses, 0)).",
        "model_name": "dim_promotions",
        "expected_columns": ["promotion_id","code","promotion_type","discount_value","is_active","usage_rate"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt model called fct_returns. Join {src('raw_returns')}, {src('raw_order_items')}, and {src('raw_products')}. Include return_id, order_item_id, product_id, product_name, reason, condition, refund_amount_dollars, status, requested_at, processed_at. Use CTEs.",
        "model_name": "fct_returns",
        "expected_columns": ["return_id","product_id","product_name","reason","refund_amount_dollars","status"],
        "expected_row_count": 30,
        "unique_key": "return_id",
    },
    {
        "prompt": f"Write a dbt model called fct_inventory. Join {src('raw_inventory')}, {src('raw_products')}, and {src('raw_warehouses')}. Include inventory_id, product_id, product_name, warehouse_id, warehouse_name, quantity_on_hand, reorder_point, needs_reorder, last_restock_date. Use CTEs.",
        "model_name": "fct_inventory",
        "expected_columns": ["inventory_id","product_name","warehouse_name","quantity_on_hand","needs_reorder"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
    {
        "prompt": f"Write a dbt model called fct_subscriptions. Join {src('raw_subscriptions')} and {src('raw_customers')}. Include subscription_id, customer_id, customer_name, plan, start_date, end_date, mrr_dollars, status, is_active (status = 'active'). Use CTEs.",
        "model_name": "fct_subscriptions",
        "expected_columns": ["subscription_id","customer_id","customer_name","plan","mrr_dollars","is_active"],
        "expected_row_count": 25,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called fct_shipping. Join {src('raw_shipping')} and {src('raw_orders')}. Include shipment_id, order_id, customer_id, carrier, shipped_date, delivered_date, delivery_days, shipping_cost_dollars, order_amount_dollars. Use CTEs.",
        "model_name": "fct_shipping",
        "expected_columns": ["shipment_id","order_id","customer_id","carrier","delivery_days","shipping_cost_dollars"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called fct_reviews. Join {src('raw_reviews')}, {src('raw_products')}, and {src('raw_customers')}. Include review_id, customer_id, customer_name, product_id, product_name, category, rating, review_text, review_date, is_verified_purchase. Use CTEs.",
        "model_name": "fct_reviews",
        "expected_columns": ["review_id","customer_name","product_name","category","rating","is_verified_purchase"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called monthly_revenue_report. Group {src('raw_orders')} by month. Include order_month, total_orders, total_revenue_dollars, avg_order_dollars, completed_orders, cancelled_orders. Use DATE_TRUNC.",
        "model_name": "monthly_revenue_report",
        "expected_columns": ["order_month","total_orders","total_revenue_dollars","avg_order_dollars","completed_orders"],
        "expected_row_count": None,
        "unique_key": "order_month",
    },
    {
        "prompt": f"Write a dbt model called quarterly_business_review. Group {src('raw_orders')} by quarter. Include quarter (YYYY-QN format), total_orders, total_revenue_dollars, unique_customers, avg_order_value. Use CTEs.",
        "model_name": "quarterly_business_review",
        "expected_columns": ["quarter","total_orders","total_revenue_dollars","unique_customers","avg_order_value"],
        "expected_row_count": None,
        "unique_key": "quarter",
    },
    {
        "prompt": f"Write a dbt model called annual_summary. Group {src('raw_orders')} by year. Include order_year, total_orders, total_revenue_dollars, unique_customers, avg_order_value, new_customers (from {src('raw_customers')} where created_at year matches). Use CTEs.",
        "model_name": "annual_summary",
        "expected_columns": ["order_year","total_orders","total_revenue_dollars","unique_customers","new_customers"],
        "expected_row_count": 3,
        "unique_key": "order_year",
    },
    {
        "prompt": f"Write a dbt model called product_daily_sales. Join {src('raw_order_items')} and {src('raw_orders')}. Group by order_date and product_id. Include order_date, product_id, units_sold, daily_revenue_dollars. Use CTEs.",
        "model_name": "product_daily_sales",
        "expected_columns": ["order_date","product_id","units_sold","daily_revenue_dollars"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_rfm. Join {src('raw_customers')} and {src('raw_orders')}. For each customer calculate: recency_days (current_date - max order_date), frequency (order count), monetary (total spend dollars). Include customer_id, first_name. Use CTEs.",
        "model_name": "customer_rfm",
        "expected_columns": ["customer_id","first_name","recency_days","frequency","monetary"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
]
extra_prompts.extend(extra_marts)

# =====================================================================
# ADDITIONAL SIMPLE SELECTS (7 more)
# =====================================================================

extra_simple = [
    {
        "prompt": f"Write a dbt model called business_customers that selects only business customers from {src('raw_customers')} where customer_type = 'business'. Include all columns.",
        "model_name": "business_customers",
        "expected_columns": ["id","first_name","last_name","email","customer_type"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called expensive_products from {src('raw_products')} where unit_price > 10000 (> $100). Include product_id (renamed from id), name, category, unit_price_dollars.",
        "model_name": "expensive_products",
        "expected_columns": ["product_id","name","category","unit_price_dollars"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called delivered_shipments from {src('raw_shipping')} where status = 'delivered'. Include all columns.",
        "model_name": "delivered_shipments",
        "expected_columns": ["id","order_id","carrier","shipped_date","delivered_date","status"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called active_promotions from {src('raw_promotions')} where is_active = true. Include all columns.",
        "model_name": "active_promotions",
        "expected_columns": ["id","code","promotion_type","discount_value","is_active"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called verified_reviews from {src('raw_reviews')} where is_verified_purchase = true. Include all columns.",
        "model_name": "verified_reviews",
        "expected_columns": ["id","customer_id","product_id","rating","is_verified_purchase"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called open_tickets from {src('raw_support_tickets')} where status in ('open', 'in_progress'). Include all columns.",
        "model_name": "open_tickets",
        "expected_columns": ["id","customer_id","category","priority","status"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called active_subscriptions from {src('raw_subscriptions')} where status = 'active'. Include all columns.",
        "model_name": "active_subscriptions",
        "expected_columns": ["id","customer_id","plan","start_date","mrr","status"],
        "expected_row_count": None,
        "unique_key": "id",
    },
]
extra_prompts.extend(extra_simple)

# =====================================================================
# ADDITIONAL CROSS-DOMAIN (25 more)
# =====================================================================

extra_cross = [
    {
        "prompt": f"Write a dbt model called product_profitability. Join {src('raw_products')}, {src('raw_order_items')}, {src('raw_returns')}, and {src('raw_inventory')}. For each product: total_revenue_dollars, total_cost_dollars (cost * quantity), total_returns_dollars, gross_profit, profit_margin_pct. Use CTEs.",
        "model_name": "product_profitability",
        "expected_columns": ["product_id","name","total_revenue","total_cost","gross_profit","profit_margin_pct"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_cohort_analysis. Join {src('raw_customers')} and {src('raw_orders')}. Group customers by signup month (cohort). For each cohort: customer_count, total_orders, avg_orders_per_customer, total_revenue. Use CTEs.",
        "model_name": "customer_cohort_analysis",
        "expected_columns": ["cohort_month","customer_count","total_orders","avg_orders_per_customer","total_revenue"],
        "expected_row_count": None,
        "unique_key": "cohort_month",
    },
    {
        "prompt": f"Write a dbt model called order_fulfillment_metrics. Join {src('raw_orders')}, {src('raw_payments')}, and {src('raw_shipping')}. For each order: has_payment (bool), has_shipment (bool), days_to_ship (shipped_date - order_date), days_to_deliver. Use CTEs and LEFT JOINs.",
        "model_name": "order_fulfillment_metrics",
        "expected_columns": ["order_id","has_payment","has_shipment","days_to_ship","days_to_deliver"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called device_conversion. Join {src('raw_sessions')}, {src('raw_customers')}, and {src('raw_orders')}. Group by device_type. For each: session_count, unique_customers, orders_placed, conversion_rate (orders/sessions). Use CTEs.",
        "model_name": "device_conversion",
        "expected_columns": ["device_type","session_count","unique_customers","orders_placed","conversion_rate"],
        "expected_row_count": 3,
        "unique_key": "device_type",
    },
    {
        "prompt": f"Write a dbt model called referral_roi. Join {src('raw_customers')} and {src('raw_orders')} on customer_id. Group by referral_source. For each: customer_count, total_orders, total_revenue_dollars, avg_revenue_per_customer. Use CTEs.",
        "model_name": "referral_roi",
        "expected_columns": ["referral_source","customer_count","total_orders","total_revenue","avg_revenue_per_customer"],
        "expected_row_count": 6,
        "unique_key": "referral_source",
    },
    {
        "prompt": f"Write a dbt model called category_customer_analysis. Join {src('raw_order_items')}, {src('raw_products')}, {src('raw_orders')}, and {src('raw_customers')}. For each category: unique_customers, repeat_customers (bought >1 time), avg_spend_per_customer. Use CTEs.",
        "model_name": "category_customer_analysis",
        "expected_columns": ["category","unique_customers","repeat_customers","avg_spend_per_customer"],
        "expected_row_count": None,
        "unique_key": "category",
    },
    {
        "prompt": f"Write a dbt model called support_impact. Join {src('raw_support_tickets')}, {src('raw_customers')}, {src('raw_orders')}, and {src('raw_reviews')}. For customers with tickets: avg_review_rating vs customers without tickets. Include has_tickets (bool), customer_count, avg_rating, avg_spend. Use CTEs.",
        "model_name": "support_impact",
        "expected_columns": ["has_tickets","customer_count","avg_rating","avg_spend"],
        "expected_row_count": 2,
        "unique_key": "has_tickets",
    },
    {
        "prompt": f"Write a dbt model called new_vs_returning. Join {src('raw_orders')} with a CTE that finds each customer's first order date. Flag each order as 'new' (customer's first order) or 'returning'. Group by flag. Include customer_type, order_count, total_revenue_dollars, avg_order_dollars.",
        "model_name": "new_vs_returning",
        "expected_columns": ["customer_type","order_count","total_revenue_dollars","avg_order_dollars"],
        "expected_row_count": 2,
        "unique_key": "customer_type",
    },
    {
        "prompt": f"Write a dbt model called geographic_revenue. Join {src('raw_orders')}, {src('raw_customers')}, and {src('raw_addresses')}. Group by state. Include state, customer_count, total_orders, total_revenue_dollars, avg_order_dollars. Use CTEs.",
        "model_name": "geographic_revenue",
        "expected_columns": ["state","customer_count","total_orders","total_revenue_dollars"],
        "expected_row_count": None,
        "unique_key": "state",
    },
    {
        "prompt": f"Write a dbt model called email_to_purchase. Join {src('raw_email_events')}, {src('raw_customers')}, and {src('raw_orders')}. For clicked emails, check if customer ordered within 3 days. Calculate click_to_purchase_rate. Group by campaign_id. Use CTEs.",
        "model_name": "email_to_purchase",
        "expected_columns": ["campaign_id","total_clicks","purchases_within_3d","click_to_purchase_rate"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
]
extra_prompts.extend(extra_cross)

# =====================================================================
# OUTPUT: Merge with existing prompts
# =====================================================================

# Load existing prompts
existing_path = os.path.join(os.path.dirname(__file__), "..", "rl_sandbox", "prompts.py")

# Read existing file and extract the list
import importlib.util
spec = importlib.util.spec_from_file_location("prompts", existing_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
existing = mod.RL_PROMPTS

# Check for duplicate model names
existing_names = {p["model_name"] for p in existing}
extra_filtered = []
for p in extra_prompts:
    if p["model_name"] not in existing_names:
        extra_filtered.append(p)
        existing_names.add(p["model_name"])
    else:
        print(f"  SKIP duplicate: {p['model_name']}")

all_prompts = existing + extra_filtered

# Rewrite the file
with open(existing_path, "w") as f:
    f.write('"""\n')
    f.write(f"Training prompts for dbt-coder RL. {len(all_prompts)} prompts across 15 categories.\n")
    f.write(f"Auto-generated by scripts/generate_prompts.py + scripts/generate_prompts_extra.py\n")
    f.write('"""\n\n')
    f.write("RL_PROMPTS = [\n")
    for p in all_prompts:
        f.write("    {\n")
        for key in ["prompt", "model_name", "expected_columns", "expected_row_count", "unique_key"]:
            val = p.get(key)
            if isinstance(val, str):
                escaped = val.replace("\\", "\\\\").replace('"', '\\"')
                f.write(f'        "{key}": "{escaped}",\n')
            elif isinstance(val, list):
                f.write(f'        "{key}": {json.dumps(val)},\n')
            elif val is None:
                f.write(f'        "{key}": None,\n')
            else:
                f.write(f'        "{key}": {val},\n')
        f.write("    },\n")
    f.write("]\n")

print(f"\nTotal: {len(all_prompts)} prompts ({len(existing)} existing + {len(extra_filtered)} new)")
