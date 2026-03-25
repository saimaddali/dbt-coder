"""
Generate 500+ training prompts for the expanded jaffle_shop_v2 dataset.

Each prompt has:
- prompt: instruction text
- model_name: expected dbt model name
- expected_columns: list of column names the output must have
- expected_row_count: approximate expected row count (for reward function)
- unique_key: column(s) that should be unique (for duplicate detection)

All prompts reference {{ source('jaffle_shop', 'raw_*') }} tables.
"""

import json
import os

# Schema reference for all 22 tables
TABLES = {
    "raw_customers": {
        "columns": ["id","first_name","last_name","email","phone","created_at","updated_at","is_active","customer_type","referral_source"],
        "rows": 50
    },
    "raw_orders": {
        "columns": ["id","customer_id","order_date","status","amount","shipping_method","discount_amount","tax_amount","currency","channel","notes"],
        "rows": 100
    },
    "raw_payments": {
        "columns": ["id","order_id","payment_method","amount","status","created_at","updated_at","processor_id","is_refund"],
        "rows": 119
    },
    "raw_products": {
        "columns": ["id","name","category","subcategory","unit_price","cost","is_active","created_at","updated_at","weight_kg","sku"],
        "rows": 30
    },
    "raw_order_items": {
        "columns": ["id","order_id","product_id","quantity","unit_price","discount_pct","created_at"],
        "rows": 200
    },
    "raw_addresses": {
        "columns": ["id","customer_id","address_type","street","city","state","zip_code","country","is_default","created_at","updated_at"],
        "rows": 60
    },
    "raw_refunds": {
        "columns": ["id","order_id","reason","refund_amount","status","requested_at","processed_at","processed_by"],
        "rows": 25
    },
    "raw_inventory": {
        "columns": ["id","product_id","warehouse_id","quantity_on_hand","reorder_point","last_restock_date","updated_at"],
        "rows": 50
    },
    "raw_promotions": {
        "columns": ["id","code","promotion_type","discount_value","start_date","end_date","max_uses","times_used","min_order_amount","is_active"],
        "rows": 20
    },
    "raw_shipping": {
        "columns": ["id","order_id","carrier","tracking_number","shipped_date","delivered_date","status","weight_kg","shipping_cost"],
        "rows": 75
    },
    "raw_reviews": {
        "columns": ["id","customer_id","product_id","rating","review_text","created_at","is_verified_purchase"],
        "rows": 60
    },
    "raw_sessions": {
        "columns": ["id","customer_id","session_start","session_end","device_type","channel"],
        "rows": 200
    },
    "raw_page_views": {
        "columns": ["id","session_id","page_url","viewed_at","duration_seconds"],
        "rows": 300
    },
    "raw_support_tickets": {
        "columns": ["id","customer_id","order_id","category","priority","status","created_at","resolved_at","assigned_employee_id"],
        "rows": 40
    },
    "raw_employees": {
        "columns": ["id","first_name","last_name","role","department","hire_date"],
        "rows": 15
    },
    "raw_suppliers": {
        "columns": ["id","name","contact_email","country","lead_time_days"],
        "rows": 10
    },
    "raw_returns": {
        "columns": ["id","order_item_id","reason","condition","refund_amount","status","requested_at","processed_at"],
        "rows": 30
    },
    "raw_subscriptions": {
        "columns": ["id","customer_id","plan","start_date","end_date","mrr","status"],
        "rows": 25
    },
    "raw_categories": {
        "columns": ["id","name","parent_category_id"],
        "rows": 8
    },
    "raw_warehouses": {
        "columns": ["id","name","city","state","capacity"],
        "rows": 4
    },
    "raw_campaigns": {
        "columns": ["id","name","channel","start_date","end_date","budget","status"],
        "rows": 12
    },
    "raw_email_events": {
        "columns": ["id","customer_id","campaign_id","event_type","event_timestamp"],
        "rows": 100
    },
}

def src(table):
    return "{{ source('jaffle_shop', '" + table + "') }}"

prompts = []

# =============================================================================
# CATEGORY 1: STAGING (22 prompts — one per table)
# =============================================================================

staging_prompts = [
    {
        "prompt": f"Write a dbt staging model called stg_customers that reads from {src('raw_customers')}. Rename 'id' to 'customer_id', cast 'created_at' to date as 'signup_date', include first_name, last_name, email, is_active, customer_type, referral_source.",
        "model_name": "stg_customers",
        "expected_columns": ["customer_id","first_name","last_name","email","is_active","customer_type","referral_source","signup_date"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_orders that reads from {src('raw_orders')}. Rename 'id' to 'order_id', convert amount from cents to dollars (divide by 100.0) as 'amount_dollars', cast order_date to date, include customer_id, status, shipping_method, channel.",
        "model_name": "stg_orders",
        "expected_columns": ["order_id","customer_id","order_date","status","amount_dollars","shipping_method","channel"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_payments that reads from {src('raw_payments')}. Rename 'id' to 'payment_id', convert amount from cents to dollars, cast created_at to date as 'payment_date', include order_id, payment_method, status, is_refund.",
        "model_name": "stg_payments",
        "expected_columns": ["payment_id","order_id","payment_method","amount","payment_date","status","is_refund"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_products that reads from {src('raw_products')}. Rename 'id' to 'product_id', convert unit_price and cost from cents to dollars, calculate margin_pct as (unit_price - cost) / unit_price * 100. Include name, category, subcategory, is_active, sku.",
        "model_name": "stg_products",
        "expected_columns": ["product_id","name","category","subcategory","unit_price","cost","margin_pct","is_active","sku"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_order_items that reads from {src('raw_order_items')}. Rename 'id' to 'order_item_id', convert unit_price from cents to dollars, calculate line_total as quantity * unit_price / 100.0 * (1 - discount_pct / 100.0). Include order_id, product_id, quantity.",
        "model_name": "stg_order_items",
        "expected_columns": ["order_item_id","order_id","product_id","quantity","unit_price","line_total"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_addresses that reads from {src('raw_addresses')}. Rename 'id' to 'address_id'. Include customer_id, address_type, city, state, zip_code, is_default.",
        "model_name": "stg_addresses",
        "expected_columns": ["address_id","customer_id","address_type","city","state","zip_code","is_default"],
        "expected_row_count": 60,
        "unique_key": "address_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_refunds that reads from {src('raw_refunds')}. Rename 'id' to 'refund_id', convert refund_amount from cents to dollars. Cast requested_at to date. Include order_id, reason, status.",
        "model_name": "stg_refunds",
        "expected_columns": ["refund_id","order_id","reason","refund_amount","status"],
        "expected_row_count": 25,
        "unique_key": "refund_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_inventory that reads from {src('raw_inventory')}. Rename 'id' to 'inventory_id'. Include product_id, warehouse_id, quantity_on_hand, reorder_point. Add a boolean column 'needs_reorder' that is true when quantity_on_hand <= reorder_point.",
        "model_name": "stg_inventory",
        "expected_columns": ["inventory_id","product_id","warehouse_id","quantity_on_hand","reorder_point","needs_reorder"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_promotions that reads from {src('raw_promotions')}. Rename 'id' to 'promotion_id'. Convert discount_value and min_order_amount from cents to dollars where promotion_type is 'fixed_amount'. Include code, promotion_type, start_date, end_date, is_active.",
        "model_name": "stg_promotions",
        "expected_columns": ["promotion_id","code","promotion_type","discount_value","start_date","end_date","is_active"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_shipping that reads from {src('raw_shipping')}. Rename 'id' to 'shipment_id'. Convert shipping_cost from cents to dollars. Calculate delivery_days as the difference between delivered_date and shipped_date (NULL if not delivered). Include order_id, carrier, status.",
        "model_name": "stg_shipping",
        "expected_columns": ["shipment_id","order_id","carrier","shipped_date","delivered_date","status","shipping_cost","delivery_days"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_reviews that reads from {src('raw_reviews')}. Rename 'id' to 'review_id'. Cast created_at to date as 'review_date'. Include customer_id, product_id, rating, review_text, is_verified_purchase.",
        "model_name": "stg_reviews",
        "expected_columns": ["review_id","customer_id","product_id","rating","review_date","is_verified_purchase"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_sessions that reads from {src('raw_sessions')}. Rename 'id' to 'session_id'. Calculate session_duration_minutes as the difference between session_end and session_start in minutes. Include customer_id, device_type, channel.",
        "model_name": "stg_sessions",
        "expected_columns": ["session_id","customer_id","device_type","channel","session_duration_minutes"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_page_views that reads from {src('raw_page_views')}. Rename 'id' to 'page_view_id'. Include session_id, page_url, viewed_at, duration_seconds.",
        "model_name": "stg_page_views",
        "expected_columns": ["page_view_id","session_id","page_url","viewed_at","duration_seconds"],
        "expected_row_count": 300,
        "unique_key": "page_view_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_support_tickets that reads from {src('raw_support_tickets')}. Rename 'id' to 'ticket_id'. Calculate resolution_hours as the difference between resolved_at and created_at in hours (NULL if unresolved). Include customer_id, order_id, category, priority, status.",
        "model_name": "stg_support_tickets",
        "expected_columns": ["ticket_id","customer_id","category","priority","status","resolution_hours"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_employees that reads from {src('raw_employees')}. Rename 'id' to 'employee_id'. Concatenate first_name and last_name as 'full_name'. Include role, department, hire_date.",
        "model_name": "stg_employees",
        "expected_columns": ["employee_id","full_name","role","department","hire_date"],
        "expected_row_count": 15,
        "unique_key": "employee_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_suppliers that reads from {src('raw_suppliers')}. Rename 'id' to 'supplier_id'. Include name, contact_email, country, lead_time_days.",
        "model_name": "stg_suppliers",
        "expected_columns": ["supplier_id","name","contact_email","country","lead_time_days"],
        "expected_row_count": 10,
        "unique_key": "supplier_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_returns that reads from {src('raw_returns')}. Rename 'id' to 'return_id'. Convert refund_amount from cents to dollars. Include order_item_id, reason, condition, status.",
        "model_name": "stg_returns",
        "expected_columns": ["return_id","order_item_id","reason","condition","refund_amount","status"],
        "expected_row_count": 30,
        "unique_key": "return_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_subscriptions that reads from {src('raw_subscriptions')}. Rename 'id' to 'subscription_id'. Convert mrr from cents to dollars. Include customer_id, plan, start_date, end_date, status.",
        "model_name": "stg_subscriptions",
        "expected_columns": ["subscription_id","customer_id","plan","start_date","end_date","mrr","status"],
        "expected_row_count": 25,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_categories that reads from {src('raw_categories')}. Rename 'id' to 'category_id'. Include name, parent_category_id.",
        "model_name": "stg_categories",
        "expected_columns": ["category_id","name","parent_category_id"],
        "expected_row_count": 8,
        "unique_key": "category_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_warehouses that reads from {src('raw_warehouses')}. Rename 'id' to 'warehouse_id'. Include name, city, state, capacity.",
        "model_name": "stg_warehouses",
        "expected_columns": ["warehouse_id","name","city","state","capacity"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_campaigns that reads from {src('raw_campaigns')}. Rename 'id' to 'campaign_id'. Convert budget from cents to dollars. Include name, channel, start_date, end_date, status.",
        "model_name": "stg_campaigns",
        "expected_columns": ["campaign_id","name","channel","start_date","end_date","budget","status"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt staging model called stg_email_events that reads from {src('raw_email_events')}. Rename 'id' to 'event_id'. Cast event_timestamp to date as 'event_date'. Include customer_id, campaign_id, event_type.",
        "model_name": "stg_email_events",
        "expected_columns": ["event_id","customer_id","campaign_id","event_type","event_date"],
        "expected_row_count": 100,
        "unique_key": "event_id",
    },
]
prompts.extend(staging_prompts)

# =============================================================================
# CATEGORY 2: TWO-TABLE JOINS (60 prompts)
# =============================================================================

two_table_joins = [
    {
        "prompt": f"Write a dbt model called customer_orders that joins {src('raw_customers')} and {src('raw_orders')} on customer_id. Include customer_id, first_name, last_name, order count as 'order_count', and total order amount in dollars as 'total_amount'. Use CTEs.",
        "model_name": "customer_orders",
        "expected_columns": ["customer_id","first_name","last_name","order_count","total_amount"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_payments that joins {src('raw_orders')} and {src('raw_payments')} on order_id. Include order_id, order_date, order status, total payment amount in dollars, number of payments, and whether any payment is a refund.",
        "model_name": "order_payments",
        "expected_columns": ["order_id","order_date","status","total_payment_dollars","payment_count"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called order_items_enriched that joins {src('raw_order_items')} and {src('raw_products')} on product_id. Include order_item_id, order_id, product_name, category, quantity, unit_price in dollars, and line_total (quantity * unit_price / 100.0).",
        "model_name": "order_items_enriched",
        "expected_columns": ["order_item_id","order_id","product_name","category","quantity","line_total"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called customer_addresses that joins {src('raw_customers')} and {src('raw_addresses')}. For each customer, get their default shipping address. Include customer_id, full_name (first || ' ' || last), city, state, zip_code. Use a LEFT JOIN so customers without addresses are included.",
        "model_name": "customer_addresses",
        "expected_columns": ["customer_id","full_name","city","state","zip_code"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_shipping that joins {src('raw_orders')} and {src('raw_shipping')} on order_id. Include order_id, order_date, carrier, shipped_date, delivered_date, shipping_cost in dollars, and delivery_days (delivered_date - shipped_date).",
        "model_name": "order_shipping",
        "expected_columns": ["order_id","order_date","carrier","shipped_date","delivered_date","shipping_cost"],
        "expected_row_count": 75,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_reviews that joins {src('raw_products')} and {src('raw_reviews')} on product_id. For each product, calculate avg_rating, review_count, and pct_verified (percentage of verified reviews). Include product_id, product name, category.",
        "model_name": "product_reviews",
        "expected_columns": ["product_id","name","category","avg_rating","review_count"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called product_inventory that joins {src('raw_products')} and {src('raw_inventory')} on product_id. Sum quantity_on_hand across all warehouses for each product. Include product_id, name, total_stock, total_reorder_point, and needs_reorder (true if total_stock < total_reorder_point).",
        "model_name": "product_inventory",
        "expected_columns": ["product_id","name","total_stock","total_reorder_point","needs_reorder"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_sessions that joins {src('raw_customers')} and {src('raw_sessions')} on customer_id. For each customer, count total sessions, calculate avg session duration in minutes, and find their most common device_type. Include customer_id, first_name.",
        "model_name": "customer_sessions",
        "expected_columns": ["customer_id","first_name","total_sessions","avg_session_minutes"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called session_page_views that joins {src('raw_sessions')} and {src('raw_page_views')} on session_id. For each session, count pages viewed and sum total duration. Include session_id, customer_id, device_type, pages_viewed, total_duration_seconds.",
        "model_name": "session_page_views",
        "expected_columns": ["session_id","customer_id","device_type","pages_viewed","total_duration_seconds"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called customer_tickets that joins {src('raw_customers')} and {src('raw_support_tickets')} on customer_id. For each customer, count open tickets, resolved tickets, and avg resolution time in hours. Include customer_id, first_name, email.",
        "model_name": "customer_tickets",
        "expected_columns": ["customer_id","first_name","email","open_tickets","resolved_tickets"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_refunds that joins {src('raw_orders')} and {src('raw_refunds')} on order_id. Include order_id, order_date, order amount in dollars, refund_amount in dollars, refund_reason, refund_status. Use a LEFT JOIN so all orders appear.",
        "model_name": "order_refunds",
        "expected_columns": ["order_id","order_date","order_amount","refund_amount","reason","refund_status"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_subscriptions that joins {src('raw_customers')} and {src('raw_subscriptions')} on customer_id. Include customer_id, first_name, email, plan, mrr in dollars, subscription status. Use LEFT JOIN so customers without subscriptions show NULLs.",
        "model_name": "customer_subscriptions",
        "expected_columns": ["customer_id","first_name","email","plan","mrr","subscription_status"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_warehouses that joins {src('raw_inventory')} and {src('raw_warehouses')} on warehouse_id. Include inventory_id, product_id, warehouse_name, city, state, quantity_on_hand, reorder_point.",
        "model_name": "inventory_warehouses",
        "expected_columns": ["inventory_id","product_id","warehouse_name","city","state","quantity_on_hand","reorder_point"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
    {
        "prompt": f"Write a dbt model called campaign_emails that joins {src('raw_campaigns')} and {src('raw_email_events')} on campaign_id. For each campaign, count total sent, opened, clicked, bounced, and unsubscribed events. Include campaign_id, campaign name, channel.",
        "model_name": "campaign_emails",
        "expected_columns": ["campaign_id","name","channel","sent_count","opened_count","clicked_count"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_employees that joins {src('raw_support_tickets')} and {src('raw_employees')} where assigned_employee_id = employee id. Include ticket_id, customer_id, category, priority, ticket status, employee full_name, department. Use LEFT JOIN.",
        "model_name": "ticket_employees",
        "expected_columns": ["ticket_id","customer_id","category","priority","status","employee_name","department"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called order_item_returns that joins {src('raw_order_items')} and {src('raw_returns')} on order_item_id. Include order_item_id, order_id, product_id, quantity, return_reason, return_status, refund_amount in dollars. LEFT JOIN so items without returns show NULLs.",
        "model_name": "order_item_returns",
        "expected_columns": ["order_item_id","order_id","product_id","quantity","return_reason","return_status","refund_amount"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called customer_reviews that joins {src('raw_customers')} and {src('raw_reviews')} on customer_id. For each customer, count reviews, calculate avg_rating, and find their most recent review date. Include customer_id, first_name, email.",
        "model_name": "customer_reviews",
        "expected_columns": ["customer_id","first_name","email","review_count","avg_rating"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called active_customer_orders that joins {src('raw_customers')} and {src('raw_orders')}. Only include customers where is_active = true. Include customer_id, first_name, order_count, total_amount_dollars, most_recent_order_date.",
        "model_name": "active_customer_orders",
        "expected_columns": ["customer_id","first_name","order_count","total_amount_dollars","most_recent_order_date"],
        "expected_row_count": 43,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_carriers that joins {src('raw_shipping')} and {src('raw_orders')} on order_id. For each carrier, calculate total shipments, avg delivery days, avg shipping cost in dollars, and on_time_pct (delivered within 5 days). Group by carrier.",
        "model_name": "shipping_carriers",
        "expected_columns": ["carrier","total_shipments","avg_delivery_days","avg_shipping_cost"],
        "expected_row_count": 4,
        "unique_key": "carrier",
    },
    {
        "prompt": f"Write a dbt model called payment_methods_summary that joins {src('raw_payments')} and {src('raw_orders')} on order_id. For each payment_method, calculate total payments, sum of amounts in dollars, avg payment amount, and count of failed payments. Exclude refund payments.",
        "model_name": "payment_methods_summary",
        "expected_columns": ["payment_method","total_payments","total_amount","avg_amount","failed_count"],
        "expected_row_count": 4,
        "unique_key": "payment_method",
    },
    # More two-table joins
    {
        "prompt": f"Write a dbt model called product_orders that joins {src('raw_order_items')} and {src('raw_products')} on product_id. For each product, count times ordered, sum quantity sold, and total revenue in dollars. Include product name and category.",
        "model_name": "product_orders",
        "expected_columns": ["product_id","name","category","times_ordered","quantity_sold","total_revenue"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_email_engagement that joins {src('raw_customers')} and {src('raw_email_events')} on customer_id. For each customer, count emails sent, opened, and clicked. Calculate open_rate (opened/sent) and click_rate (clicked/sent). Include customer_id, email.",
        "model_name": "customer_email_engagement",
        "expected_columns": ["customer_id","email","emails_sent","emails_opened","open_rate"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called warehouse_inventory_summary that joins {src('raw_inventory')} and {src('raw_warehouses')} on warehouse_id. For each warehouse, calculate total items in stock, count of products needing reorder (quantity_on_hand <= reorder_point), and capacity utilization. Include warehouse name, city.",
        "model_name": "warehouse_inventory_summary",
        "expected_columns": ["warehouse_id","warehouse_name","city","total_items","products_needing_reorder"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_status that joins {src('raw_orders')} with itself (self-join) to find orders placed by the same customer within 30 days of each other. Include order_id, customer_id, order_date, related_order_id, related_order_date.",
        "model_name": "orders_with_status",
        "expected_columns": ["order_id","customer_id","order_date","related_order_id","related_order_date"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called subscription_customers that joins {src('raw_subscriptions')} and {src('raw_customers')} on customer_id. Only include active subscriptions. Include subscription_id, customer full name, email, plan, mrr in dollars, start_date.",
        "model_name": "subscription_customers",
        "expected_columns": ["subscription_id","full_name","email","plan","mrr","start_date"],
        "expected_row_count": None,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called employee_tickets that joins {src('raw_employees')} and {src('raw_support_tickets')} where employee id = assigned_employee_id. For each employee, count total tickets assigned, resolved tickets, avg resolution hours. Include employee full name, department.",
        "model_name": "employee_tickets",
        "expected_columns": ["employee_id","full_name","department","total_assigned","resolved_count"],
        "expected_row_count": 15,
        "unique_key": "employee_id",
    },
    {
        "prompt": f"Write a dbt model called category_products that joins {src('raw_categories')} and {src('raw_products')} where category name matches the product category. Include category_id, category name, product count, avg price in dollars, total products active.",
        "model_name": "category_products",
        "expected_columns": ["category_id","category_name","product_count","avg_price"],
        "expected_row_count": 8,
        "unique_key": "category_id",
    },
    {
        "prompt": f"Write a dbt model called orders_by_channel that groups {src('raw_orders')} by channel. For each channel, count orders, sum amount in dollars, avg amount, and count of completed orders. Include channel.",
        "model_name": "orders_by_channel",
        "expected_columns": ["channel","order_count","total_amount","avg_amount","completed_count"],
        "expected_row_count": 4,
        "unique_key": "channel",
    },
    {
        "prompt": f"Write a dbt model called promotion_usage that joins {src('raw_promotions')} with itself. Calculate usage_rate as times_used / max_uses. Flag promotions where usage_rate > 0.8 as 'nearly_exhausted'. Include promotion_id, code, promotion_type, usage_rate.",
        "model_name": "promotion_usage",
        "expected_columns": ["promotion_id","code","promotion_type","usage_rate"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt model called review_products that joins {src('raw_reviews')} and {src('raw_products')} on product_id. Include review_id, product_name, category, rating, review_text, is_verified_purchase. Only include verified purchases.",
        "model_name": "review_products",
        "expected_columns": ["review_id","product_name","category","rating","review_text","is_verified_purchase"],
        "expected_row_count": None,
        "unique_key": "review_id",
    },
    # Even more two-table joins for diversity
    {
        "prompt": f"Write a dbt model called customers_by_type that groups {src('raw_customers')} by customer_type. Include customer_type, customer_count, active_count, inactive_count. Calculate active_pct.",
        "model_name": "customers_by_type",
        "expected_columns": ["customer_type","customer_count","active_count","inactive_count"],
        "expected_row_count": 2,
        "unique_key": "customer_type",
    },
    {
        "prompt": f"Write a dbt model called customers_by_referral that groups {src('raw_customers')} by referral_source. Count customers per source. Include referral_source, customer_count. Order by customer_count desc.",
        "model_name": "customers_by_referral",
        "expected_columns": ["referral_source","customer_count"],
        "expected_row_count": 6,
        "unique_key": "referral_source",
    },
    {
        "prompt": f"Write a dbt model called orders_by_status that groups {src('raw_orders')} by status. Include status, order_count, total_amount_dollars, avg_amount_dollars.",
        "model_name": "orders_by_status",
        "expected_columns": ["status","order_count","total_amount_dollars","avg_amount_dollars"],
        "expected_row_count": 5,
        "unique_key": "status",
    },
    {
        "prompt": f"Write a dbt model called shipping_by_method that groups {src('raw_orders')} by shipping_method. Include shipping_method, order_count, total_amount_dollars, avg_amount_dollars.",
        "model_name": "shipping_by_method",
        "expected_columns": ["shipping_method","order_count","total_amount_dollars","avg_amount_dollars"],
        "expected_row_count": 3,
        "unique_key": "shipping_method",
    },
    {
        "prompt": f"Write a dbt model called daily_orders that groups {src('raw_orders')} by order_date. Include order_date, order_count, total_amount_dollars. Cast order_date to date.",
        "model_name": "daily_orders",
        "expected_columns": ["order_date","order_count","total_amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_date",
    },
    {
        "prompt": f"Write a dbt model called monthly_orders that extracts year and month from order_date in {src('raw_orders')}. Group by year-month. Include order_month, order_count, total_amount_dollars, avg_amount_dollars.",
        "model_name": "monthly_orders",
        "expected_columns": ["order_month","order_count","total_amount_dollars","avg_amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_month",
    },
    {
        "prompt": f"Write a dbt model called refund_reasons that groups {src('raw_refunds')} by reason. Include reason, refund_count, total_refund_dollars, avg_refund_dollars, approved_count, denied_count.",
        "model_name": "refund_reasons",
        "expected_columns": ["reason","refund_count","total_refund_dollars","approved_count","denied_count"],
        "expected_row_count": 6,
        "unique_key": "reason",
    },
    {
        "prompt": f"Write a dbt model called ticket_categories that groups {src('raw_support_tickets')} by category. Include category, ticket_count, high_priority_count, avg_resolution_hours (only for resolved tickets).",
        "model_name": "ticket_categories",
        "expected_columns": ["category","ticket_count","high_priority_count"],
        "expected_row_count": 7,
        "unique_key": "category",
    },
    {
        "prompt": f"Write a dbt model called device_sessions that groups {src('raw_sessions')} by device_type. Include device_type, session_count, avg_duration_minutes. Calculate avg_duration_minutes from session_start and session_end.",
        "model_name": "device_sessions",
        "expected_columns": ["device_type","session_count","avg_duration_minutes"],
        "expected_row_count": 3,
        "unique_key": "device_type",
    },
    {
        "prompt": f"Write a dbt model called page_popularity that groups {src('raw_page_views')} by page_url. Include page_url, view_count, avg_duration_seconds, total_duration_seconds. Order by view_count desc.",
        "model_name": "page_popularity",
        "expected_columns": ["page_url","view_count","avg_duration_seconds","total_duration_seconds"],
        "expected_row_count": None,
        "unique_key": "page_url",
    },
    {
        "prompt": f"Write a dbt model called subscription_plans that groups {src('raw_subscriptions')} by plan. Include plan, subscriber_count, active_count, cancelled_count, total_mrr_dollars, avg_mrr_dollars.",
        "model_name": "subscription_plans",
        "expected_columns": ["plan","subscriber_count","active_count","cancelled_count","total_mrr_dollars"],
        "expected_row_count": 4,
        "unique_key": "plan",
    },
    {
        "prompt": f"Write a dbt model called email_event_summary that groups {src('raw_email_events')} by event_type. Include event_type and event_count. Order by event_count desc.",
        "model_name": "email_event_summary",
        "expected_columns": ["event_type","event_count"],
        "expected_row_count": 5,
        "unique_key": "event_type",
    },
    {
        "prompt": f"Write a dbt model called return_reasons that groups {src('raw_returns')} by reason. Include reason, return_count, total_refund_dollars, processed_count, pending_count.",
        "model_name": "return_reasons",
        "expected_columns": ["reason","return_count","total_refund_dollars","processed_count","pending_count"],
        "expected_row_count": 6,
        "unique_key": "reason",
    },
    {
        "prompt": f"Write a dbt model called supplier_lead_times that reads from {src('raw_suppliers')}. Categorize suppliers into 'fast' (lead_time_days <= 7), 'medium' (8-21), 'slow' (>21). Include supplier_id, name, country, lead_time_days, speed_category.",
        "model_name": "supplier_lead_times",
        "expected_columns": ["supplier_id","name","country","lead_time_days","speed_category"],
        "expected_row_count": 10,
        "unique_key": "supplier_id",
    },
]
prompts.extend(two_table_joins)

# =============================================================================
# CATEGORY 3: THREE-TABLE JOINS (40 prompts)
# =============================================================================

three_table_joins = [
    {
        "prompt": f"Write a dbt model called order_detail_full that joins {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_products')}. Include order_id, order_date, product_name, category, quantity, unit_price in dollars, line_total (qty * price / 100). Use CTEs.",
        "model_name": "order_detail_full",
        "expected_columns": ["order_id","order_date","product_name","category","quantity","line_total"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called customer_order_products that joins {src('raw_customers')}, {src('raw_orders')}, and {src('raw_order_items')}. For each customer, list unique products ordered (count), total items purchased (sum of quantity), and total spend in dollars.",
        "model_name": "customer_order_products",
        "expected_columns": ["customer_id","first_name","unique_products","total_items","total_spend"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_shipping_payments that joins {src('raw_orders')}, {src('raw_shipping')}, and {src('raw_payments')}. Include order_id, order_date, carrier, delivery_days, total_paid_dollars, shipping_cost_dollars. Use CTEs.",
        "model_name": "order_shipping_payments",
        "expected_columns": ["order_id","order_date","carrier","total_paid_dollars","shipping_cost_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_review_inventory that joins {src('raw_products')}, {src('raw_reviews')}, and {src('raw_inventory')}. For each product, show name, avg_rating, review_count, and total_stock (sum across warehouses). Use CTEs and LEFT JOINs.",
        "model_name": "product_review_inventory",
        "expected_columns": ["product_id","name","avg_rating","review_count","total_stock"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_order_shipping that joins {src('raw_customers')}, {src('raw_orders')}, and {src('raw_shipping')}. For each customer, calculate avg_delivery_days, total_shipping_cost_dollars, and orders_delivered (count where shipping status = 'delivered').",
        "model_name": "customer_order_shipping",
        "expected_columns": ["customer_id","first_name","avg_delivery_days","total_shipping_cost","orders_delivered"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called session_customer_orders that joins {src('raw_sessions')}, {src('raw_customers')}, and {src('raw_orders')}. For each session, check if the customer placed an order on the same day. Include session_id, customer_id, session_start, device_type, has_order_same_day (boolean).",
        "model_name": "session_customer_orders",
        "expected_columns": ["session_id","customer_id","session_start","device_type","has_order_same_day"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_order_customer that joins {src('raw_support_tickets')}, {src('raw_orders')}, and {src('raw_customers')}. Include ticket_id, customer full name, email, ticket category, priority, order_date, order_amount_dollars. Use LEFT JOINs.",
        "model_name": "ticket_order_customer",
        "expected_columns": ["ticket_id","full_name","email","category","priority","order_date","order_amount"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called campaign_email_customers that joins {src('raw_campaigns')}, {src('raw_email_events')}, and {src('raw_customers')}. For each campaign, count unique customers reached, unique opens, unique clicks. Include campaign name, channel.",
        "model_name": "campaign_email_customers",
        "expected_columns": ["campaign_id","campaign_name","channel","unique_customers","unique_opens","unique_clicks"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_product_warehouse that joins {src('raw_inventory')}, {src('raw_products')}, and {src('raw_warehouses')}. Include product_name, warehouse_name, warehouse_city, quantity_on_hand, reorder_point, needs_reorder (qty <= reorder_point).",
        "model_name": "inventory_product_warehouse",
        "expected_columns": ["product_name","warehouse_name","warehouse_city","quantity_on_hand","reorder_point","needs_reorder"],
        "expected_row_count": 50,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called return_order_product that joins {src('raw_returns')}, {src('raw_order_items')}, and {src('raw_products')}. Include return_id, product_name, category, return_reason, condition, refund_amount_dollars. Use CTEs.",
        "model_name": "return_order_product",
        "expected_columns": ["return_id","product_name","category","return_reason","condition","refund_amount"],
        "expected_row_count": 30,
        "unique_key": "return_id",
    },
    {
        "prompt": f"Write a dbt model called customer_subscription_orders that joins {src('raw_customers')}, {src('raw_subscriptions')}, and {src('raw_orders')}. For subscribers, compare subscription MRR vs actual monthly order amount. Include customer_id, plan, mrr_dollars, avg_monthly_order_dollars.",
        "model_name": "customer_subscription_orders",
        "expected_columns": ["customer_id","plan","mrr_dollars","avg_monthly_order_dollars"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called page_session_customer that joins {src('raw_page_views')}, {src('raw_sessions')}, and {src('raw_customers')}. Show most viewed pages by customer type. Group by customer_type and page_url. Include view_count.",
        "model_name": "page_session_customer",
        "expected_columns": ["customer_type","page_url","view_count"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called order_items_with_returns that joins {src('raw_order_items')}, {src('raw_products')}, and {src('raw_returns')}. Include order_item_id, product_name, quantity, unit_price_dollars, has_return (boolean), return_reason. LEFT JOIN returns.",
        "model_name": "order_items_with_returns",
        "expected_columns": ["order_item_id","product_name","quantity","has_return","return_reason"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called refund_order_customer that joins {src('raw_refunds')}, {src('raw_orders')}, and {src('raw_customers')}. Include refund_id, customer full name, order_date, refund_reason, refund_amount_dollars, refund_status, days_to_process.",
        "model_name": "refund_order_customer",
        "expected_columns": ["refund_id","full_name","order_date","refund_reason","refund_amount","refund_status"],
        "expected_row_count": 25,
        "unique_key": "refund_id",
    },
    {
        "prompt": f"Write a dbt model called employee_ticket_resolution that joins {src('raw_employees')}, {src('raw_support_tickets')}, and {src('raw_customers')}. For each support employee, show tickets handled, unique customers helped, avg_resolution_hours. Filter to support department only.",
        "model_name": "employee_ticket_resolution",
        "expected_columns": ["employee_id","employee_name","tickets_handled","unique_customers","avg_resolution_hours"],
        "expected_row_count": None,
        "unique_key": "employee_id",
    },
]
prompts.extend(three_table_joins)

# =============================================================================
# CATEGORY 4: WINDOW FUNCTIONS (40 prompts)
# =============================================================================

window_functions = [
    {
        "prompt": f"Write a dbt model called orders_running_total that reads from {src('raw_orders')}. For each order, calculate running_total_dollars (cumulative sum of amount/100 ordered by order_date). Include order_id, order_date, amount_dollars, running_total_dollars.",
        "model_name": "orders_running_total",
        "expected_columns": ["order_id","order_date","amount_dollars","running_total_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_order_rank that reads from {src('raw_orders')}. Use ROW_NUMBER() to rank each customer's orders by order_date. Include order_id, customer_id, order_date, order_rank.",
        "model_name": "customer_order_rank",
        "expected_columns": ["order_id","customer_id","order_date","order_rank"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_lag that reads from {src('raw_orders')}. For each customer's orders, calculate days_since_last_order using LAG on order_date. Include order_id, customer_id, order_date, days_since_last_order (NULL for first order).",
        "model_name": "orders_with_lag",
        "expected_columns": ["order_id","customer_id","order_date","days_since_last_order"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_rank_by_revenue that joins {src('raw_order_items')} and {src('raw_products')}. Calculate total revenue per product (qty * price / 100), then rank products within each category using RANK(). Include product_id, name, category, total_revenue, category_rank.",
        "model_name": "product_rank_by_revenue",
        "expected_columns": ["product_id","name","category","total_revenue","category_rank"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called daily_revenue_moving_avg that groups {src('raw_orders')} by order_date. Calculate daily_revenue (sum amount/100) and a 7-day moving average using AVG() OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW).",
        "model_name": "daily_revenue_moving_avg",
        "expected_columns": ["order_date","daily_revenue","moving_avg_7d"],
        "expected_row_count": None,
        "unique_key": "order_date",
    },
    {
        "prompt": f"Write a dbt model called order_pct_of_customer_total using {src('raw_orders')}. For each order, calculate what percentage it is of the customer's total spend. Use SUM() OVER (PARTITION BY customer_id). Include order_id, customer_id, amount_dollars, pct_of_total.",
        "model_name": "order_pct_of_customer_total",
        "expected_columns": ["order_id","customer_id","amount_dollars","pct_of_total"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_order_ntile using {src('raw_orders')}. Assign each customer's orders to quartiles using NTILE(4) based on amount. Include order_id, customer_id, amount_dollars, spend_quartile.",
        "model_name": "customer_order_ntile",
        "expected_columns": ["order_id","customer_id","amount_dollars","spend_quartile"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called first_orders that reads from {src('raw_orders')}. Use ROW_NUMBER() partitioned by customer_id ordered by order_date to find each customer's first order. Filter to only the first order per customer. Include order_id, customer_id, order_date, amount_dollars.",
        "model_name": "first_orders",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called session_rank_by_duration using {src('raw_sessions')}. Calculate session duration in minutes (session_end - session_start). Rank sessions per customer by duration desc using DENSE_RANK. Include session_id, customer_id, duration_minutes, duration_rank.",
        "model_name": "session_rank_by_duration",
        "expected_columns": ["session_id","customer_id","duration_minutes","duration_rank"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called cumulative_reviews using {src('raw_reviews')}. For each product, calculate cumulative review count and cumulative avg rating over time (ordered by created_at). Include review_id, product_id, rating, cumulative_count, cumulative_avg_rating.",
        "model_name": "cumulative_reviews",
        "expected_columns": ["review_id","product_id","rating","cumulative_count","cumulative_avg_rating"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_lead using {src('raw_orders')}. For each customer, use LEAD to find the next order date and next order amount. Include order_id, customer_id, order_date, amount_dollars, next_order_date, next_order_amount.",
        "model_name": "orders_with_lead",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars","next_order_date","next_order_amount"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called payment_running_balance using {src('raw_payments')}. Calculate a running balance per order (cumulative sum of amount, accounting for refunds with negative amounts). Order by created_at. Include payment_id, order_id, amount_dollars, running_balance.",
        "model_name": "payment_running_balance",
        "expected_columns": ["payment_id","order_id","amount_dollars","running_balance"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called monthly_revenue_growth using {src('raw_orders')}. Group by month, calculate monthly revenue dollars, previous month revenue using LAG, and month_over_month_growth_pct. Include order_month, revenue, prev_month_revenue, growth_pct.",
        "model_name": "monthly_revenue_growth",
        "expected_columns": ["order_month","revenue","prev_month_revenue","growth_pct"],
        "expected_row_count": None,
        "unique_key": "order_month",
    },
    {
        "prompt": f"Write a dbt model called top_products_per_category that joins {src('raw_order_items')} and {src('raw_products')}. Rank products by total quantity sold within each category. Filter to top 3 per category. Include product_name, category, total_sold, category_rank.",
        "model_name": "top_products_per_category",
        "expected_columns": ["product_name","category","total_sold","category_rank"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_spend_percentile using {src('raw_orders')}. For each customer, sum total spend in dollars. Use PERCENT_RANK() to assign a spend percentile. Include customer_id, total_spend, spend_percentile.",
        "model_name": "customer_spend_percentile",
        "expected_columns": ["customer_id","total_spend","spend_percentile"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
]
prompts.extend(window_functions)

# =============================================================================
# CATEGORY 5: CASE/CONDITIONAL (30 prompts)
# =============================================================================

case_conditional = [
    {
        "prompt": f"Write a dbt model called order_value_tier using {src('raw_orders')}. Categorize orders: 'high_value' (amount > 25000), 'medium_value' (10000-25000), 'low_value' (< 10000). Include order_id, customer_id, amount_dollars, value_tier.",
        "model_name": "order_value_tier",
        "expected_columns": ["order_id","customer_id","amount_dollars","value_tier"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_segments using {src('raw_customers')}. Segment customers: 'new' (created_at in last 6 months), 'established' (6-18 months), 'veteran' (>18 months). Include customer_id, first_name, customer_type, signup_age_days, segment.",
        "model_name": "customer_segments",
        "expected_columns": ["customer_id","first_name","customer_type","segment"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called payment_type_flags using {src('raw_payments')}. Create boolean columns: is_credit_card, is_bank_transfer, is_gift_card, is_paypal. Include payment_id, order_id, payment_method, amount_dollars.",
        "model_name": "payment_type_flags",
        "expected_columns": ["payment_id","order_id","payment_method","is_credit_card","is_bank_transfer"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_speed using {src('raw_shipping')}. Categorize: 'same_day' (0 days), 'fast' (1-3 days), 'standard' (4-7 days), 'slow' (>7 days), 'pending' (no delivered_date). Include shipment_id, order_id, carrier, delivery_days, speed_category.",
        "model_name": "shipping_speed",
        "expected_columns": ["shipment_id","order_id","carrier","speed_category"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called review_sentiment using {src('raw_reviews')}. Categorize: 'positive' (rating >= 4), 'neutral' (rating = 3), 'negative' (rating <= 2). Include review_id, product_id, rating, sentiment.",
        "model_name": "review_sentiment",
        "expected_columns": ["review_id","product_id","rating","sentiment"],
        "expected_row_count": 60,
        "unique_key": "review_id",
    },
    {
        "prompt": f"Write a dbt model called subscription_health using {src('raw_subscriptions')}. Flag subscriptions: 'healthy' (active, started > 3 months ago), 'at_risk' (active, started < 3 months ago), 'churned' (cancelled/expired), 'paused'. Include subscription_id, customer_id, plan, health_status.",
        "model_name": "subscription_health",
        "expected_columns": ["subscription_id","customer_id","plan","health_status"],
        "expected_row_count": 25,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called ticket_urgency using {src('raw_support_tickets')}. Create an urgency_score: urgent=4, high=3, medium=2, low=1. Multiply by 1.5 if ticket is still open. Include ticket_id, category, priority, status, urgency_score.",
        "model_name": "ticket_urgency",
        "expected_columns": ["ticket_id","category","priority","status","urgency_score"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called product_price_tier using {src('raw_products')}. Categorize: 'budget' (< $25), 'mid_range' ($25-$100), 'premium' ($100-$200), 'luxury' (>$200). Based on unit_price/100. Include product_id, name, price_dollars, price_tier.",
        "model_name": "product_price_tier",
        "expected_columns": ["product_id","name","price_dollars","price_tier"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called campaign_performance using {src('raw_campaigns')}. Calculate days_active (end_date - start_date), daily_budget (budget/days_active). Categorize budget_size: 'small' (<$5000), 'medium' ($5000-$15000), 'large' (>$15000). Include campaign_id, name, budget_size.",
        "model_name": "campaign_performance",
        "expected_columns": ["campaign_id","name","days_active","daily_budget","budget_size"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_status using {src('raw_inventory')}. Categorize: 'out_of_stock' (qty = 0), 'critical' (qty < reorder_point), 'low' (qty < 2 * reorder_point), 'healthy' (qty >= 2 * reorder_point). Include inventory_id, product_id, warehouse_id, quantity_on_hand, stock_status.",
        "model_name": "inventory_status",
        "expected_columns": ["inventory_id","product_id","warehouse_id","quantity_on_hand","stock_status"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
    {
        "prompt": f"Write a dbt model called order_channel_type using {src('raw_orders')}. Map channels: 'web' and 'mobile' → 'digital', 'phone' → 'voice', 'in_store' → 'physical'. Include order_id, channel, channel_type, amount_dollars.",
        "model_name": "order_channel_type",
        "expected_columns": ["order_id","channel","channel_type","amount_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customer_activity using {src('raw_customers')}. Flag customers: 'recently_active' (updated_at within 90 days of current_date), 'dormant' (91-365 days), 'inactive' (>365 days). Include customer_id, first_name, last_name, activity_status.",
        "model_name": "customer_activity",
        "expected_columns": ["customer_id","first_name","last_name","activity_status"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called refund_risk using {src('raw_refunds')}. Flag refunds: 'auto_approve' (amount < 5000), 'review_needed' (5000-20000), 'escalate' (>20000). Also flag if status is 'pending' as 'needs_attention'. Include refund_id, order_id, refund_amount_dollars, risk_level.",
        "model_name": "refund_risk",
        "expected_columns": ["refund_id","order_id","refund_amount","risk_level"],
        "expected_row_count": 25,
        "unique_key": "refund_id",
    },
    {
        "prompt": f"Write a dbt model called promotion_effectiveness using {src('raw_promotions')}. Calculate utilization_pct (times_used / max_uses * 100, NULL if no max). Categorize: 'underperforming' (<25%), 'moderate' (25-75%), 'popular' (>75%). Include promotion_id, code, utilization_pct, effectiveness.",
        "model_name": "promotion_effectiveness",
        "expected_columns": ["promotion_id","code","utilization_pct","effectiveness"],
        "expected_row_count": 20,
        "unique_key": "promotion_id",
    },
    {
        "prompt": f"Write a dbt model called session_engagement using {src('raw_sessions')}. Calculate duration minutes. Categorize: 'bounce' (<2 min), 'light' (2-10 min), 'engaged' (10-30 min), 'deep' (>30 min). Include session_id, customer_id, duration_minutes, engagement_level.",
        "model_name": "session_engagement",
        "expected_columns": ["session_id","customer_id","duration_minutes","engagement_level"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
]
prompts.extend(case_conditional)

# =============================================================================
# CATEGORY 6: UNIONs (20 prompts — fusion-divergent potential)
# =============================================================================

union_prompts = [
    {
        "prompt": f"Write a dbt model called all_events that UNIONs order events and payment events into a single timeline. From {src('raw_orders')} use order_date as event_date and 'order' as event_type. From {src('raw_payments')} use created_at as event_date and 'payment' as event_type. Include relevant IDs. Cast dates to the same type.",
        "model_name": "all_events",
        "expected_columns": ["event_date","event_type"],
        "expected_row_count": 219,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_financial_events that UNIONs payments, refunds, and returns into a single ledger. Each should have: event_date, event_type ('payment', 'refund', 'return'), amount_dollars, reference_id. Cast all dates to DATE type. Use UNION ALL.",
        "model_name": "all_financial_events",
        "expected_columns": ["event_date","event_type","amount_dollars","reference_id"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_customer_interactions that UNIONs orders, support tickets, reviews, and email events. Each row: customer_id, interaction_date (cast to DATE), interaction_type ('order', 'ticket', 'review', 'email'). Use UNION ALL.",
        "model_name": "all_customer_interactions",
        "expected_columns": ["customer_id","interaction_date","interaction_type"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_timestamps that UNIONs created_at from {src('raw_customers')}, order_date from {src('raw_orders')}, and session_start from {src('raw_sessions')}. Cast all to TIMESTAMP. Include entity_type and entity_id.",
        "model_name": "all_timestamps",
        "expected_columns": ["event_timestamp","entity_type","entity_id"],
        "expected_row_count": 350,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_amounts that UNIONs amounts from orders (amount), payments (amount), and refunds (refund_amount). All in dollars. Include source_table, source_id, amount_dollars. Use UNION ALL.",
        "model_name": "all_amounts",
        "expected_columns": ["source_table","source_id","amount_dollars"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_status_changes. UNION order statuses (order_id, status, order_date), shipping statuses (order_id, status, shipped_date), and payment statuses (order_id, status, created_at). Rename date columns to 'status_date', cast to DATE.",
        "model_name": "all_status_changes",
        "expected_columns": ["order_id","status","status_date","source"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called product_feedback that UNIONs reviews and returns as product feedback. From reviews: product_id, 'review' as feedback_type, rating as score. From returns (joined with order_items): product_id, 'return' as feedback_type, -1 as score. Cast all to same types.",
        "model_name": "product_feedback",
        "expected_columns": ["product_id","feedback_type","score"],
        "expected_row_count": 90,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_touchpoints that UNIONs sessions, email events, and support tickets. Each row: customer_id, touchpoint_date (DATE), touchpoint_type, channel. Use UNION ALL. Filter to known customers only (customer_id IS NOT NULL).",
        "model_name": "customer_touchpoints",
        "expected_columns": ["customer_id","touchpoint_date","touchpoint_type","channel"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called all_dates. Extract all unique dates from orders (order_date), payments (created_at), shipping (shipped_date, delivered_date), and sessions (session_start). UNION them into a date spine. Cast all to DATE. Deduplicate with UNION (not UNION ALL).",
        "model_name": "all_dates",
        "expected_columns": ["date_value"],
        "expected_row_count": None,
        "unique_key": "date_value",
    },
    {
        "prompt": f"Write a dbt model called employee_customer_directory. UNION employees (id, full name, role as title, department) and customers (id, full name, customer_type as title, 'customer' as department). Include person_type ('employee' or 'customer').",
        "model_name": "employee_customer_directory",
        "expected_columns": ["person_type","person_id","full_name","title","department"],
        "expected_row_count": 65,
        "unique_key": None,
    },
]
prompts.extend(union_prompts)

# =============================================================================
# CATEGORY 7: INCREMENTAL MODELS (15 prompts)
# =============================================================================

incremental_prompts = [
    {
        "prompt": f"Write a dbt incremental model called incremental_orders that reads from {src('raw_orders')}. Use merge strategy, unique_key is 'id'. Incrementally load based on order_date. Include all columns.",
        "model_name": "incremental_orders",
        "expected_columns": ["id","customer_id","order_date","status","amount"],
        "expected_row_count": 100,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_payments that reads from {src('raw_payments')}. Use merge strategy, unique_key is 'id'. Incrementally load based on created_at. Include all columns.",
        "model_name": "incremental_payments",
        "expected_columns": ["id","order_id","payment_method","amount","status"],
        "expected_row_count": 119,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_sessions that reads from {src('raw_sessions')}. Use append strategy (no unique_key). Incrementally load based on session_start. Include all columns.",
        "model_name": "incremental_sessions",
        "expected_columns": ["id","customer_id","session_start","session_end","device_type","channel"],
        "expected_row_count": 200,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_page_views that reads from {src('raw_page_views')}. Merge on id. Incrementally load based on viewed_at. Include all columns.",
        "model_name": "incremental_page_views",
        "expected_columns": ["id","session_id","page_url","viewed_at","duration_seconds"],
        "expected_row_count": 300,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt incremental model called incremental_email_events that reads from {src('raw_email_events')}. Merge on id. Incrementally load based on event_timestamp. Include all columns.",
        "model_name": "incremental_email_events",
        "expected_columns": ["id","customer_id","campaign_id","event_type","event_timestamp"],
        "expected_row_count": 100,
        "unique_key": "id",
    },
]
prompts.extend(incremental_prompts)

# =============================================================================
# CATEGORY 8: JINJA PATTERNS (20 prompts)
# =============================================================================

jinja_prompts = [
    {
        "prompt": f"Write a dbt model called filtered_orders that reads from {src('raw_orders')}. Use a Jinja variable to set a minimum order amount (default 5000). Filter orders below that threshold. Include order_id, customer_id, order_date, amount_dollars.",
        "model_name": "filtered_orders",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called filtered_payments that reads from {src('raw_payments')}. Use a Jinja variable for minimum payment amount (default 1000). Filter payments below threshold. Include payment_id, order_id, amount_dollars, payment_method.",
        "model_name": "filtered_payments",
        "expected_columns": ["payment_id","order_id","amount_dollars","payment_method"],
        "expected_row_count": None,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called dynamic_order_limit that reads from {src('raw_orders')}. Use a Jinja variable 'limit_rows' (default 50) to limit the output. Order by order_date desc. Include order_id, customer_id, order_date, amount_dollars.",
        "model_name": "dynamic_order_limit",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": 50,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called orders_status_flags that reads from {src('raw_orders')}. Use a Jinja for loop to generate boolean columns for each status: is_completed, is_shipped, is_cancelled, is_returned, is_processing. Include order_id and amount.",
        "model_name": "orders_status_flags",
        "expected_columns": ["order_id","amount","is_completed","is_shipped","is_cancelled"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called configurable_products. Use {{{{ config(materialized='table') }}}}. Read from {src('raw_products')}. Use a Jinja variable 'include_inactive' (default false). If false, filter to is_active = true only. Include product_id, name, category, unit_price_dollars.",
        "model_name": "configurable_products",
        "expected_columns": ["product_id","name","category","unit_price_dollars"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called pivoted_payments from {src('raw_payments')}. Use Jinja to pivot payment_method values into columns. For each order_id, show credit_card_amount, bank_transfer_amount, gift_card_amount, paypal_amount (all in dollars). Use a Jinja for loop.",
        "model_name": "pivoted_payments",
        "expected_columns": ["order_id","credit_card_amount","bank_transfer_amount"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_env_filter from {src('raw_orders')}. Use {{{{ var('target_status', 'completed') }}}} to filter orders by status. Include order_id, customer_id, order_date, amount_dollars, status.",
        "model_name": "orders_with_env_filter",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars","status"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called date_filtered_sessions from {src('raw_sessions')}. Use {{{{ var('start_date', '2023-01-01') }}}} and {{{{ var('end_date', '2024-12-31') }}}} to filter sessions. Include session_id, customer_id, session_start, device_type.",
        "model_name": "date_filtered_sessions",
        "expected_columns": ["session_id","customer_id","session_start","device_type"],
        "expected_row_count": None,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called conditional_materialization from {src('raw_orders')}. Use {{{{ config(materialized=var('mat_type', 'view')) }}}}. Include order_id, customer_id, order_date, amount_dollars. This demonstrates configurable materialization.",
        "model_name": "conditional_materialization",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called multi_source_union. Use a Jinja for loop to UNION ALL three tables: {src('raw_orders')}, {src('raw_payments')}, {src('raw_refunds')}. For each, select id as source_id, the table name as source_name. Use a Jinja list variable.",
        "model_name": "multi_source_union",
        "expected_columns": ["source_id","source_name"],
        "expected_row_count": None,
        "unique_key": None,
    },
]
prompts.extend(jinja_prompts)

# =============================================================================
# CATEGORY 9: COMPLEX MULTI-HOP JOINS (40 prompts)
# =============================================================================

complex_joins = [
    {
        "prompt": f"Write a dbt model called customer_360 that creates a complete customer profile. Join {src('raw_customers')}, {src('raw_orders')}, {src('raw_payments')}, and {src('raw_addresses')}. For each customer: customer_id, full_name, email, total_orders, total_payments_dollars, default_city, default_state. Use CTEs.",
        "model_name": "customer_360",
        "expected_columns": ["customer_id","full_name","email","total_orders","total_payments_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called order_complete_detail. Join {src('raw_orders')}, {src('raw_order_items')}, {src('raw_products')}, and {src('raw_shipping')}. For each order: order_id, order_date, item_count, total_items_dollars, carrier, delivery_days, shipping_cost_dollars. Use CTEs.",
        "model_name": "order_complete_detail",
        "expected_columns": ["order_id","order_date","item_count","total_items_dollars","carrier"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called product_full_profile. Join {src('raw_products')}, {src('raw_order_items')}, {src('raw_reviews')}, and {src('raw_inventory')}. For each product: product_id, name, category, total_revenue_dollars, avg_rating, review_count, total_stock. Use CTEs.",
        "model_name": "product_full_profile",
        "expected_columns": ["product_id","name","category","total_revenue","avg_rating","review_count","total_stock"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customer_lifetime_value. Join {src('raw_customers')}, {src('raw_orders')}, {src('raw_payments')}, and {src('raw_subscriptions')}. For each customer: customer_id, full_name, first_order_date, most_recent_order_date, lifetime_order_count, lifetime_spend_dollars, subscription_mrr_dollars, days_as_customer. Use CTEs.",
        "model_name": "customer_lifetime_value",
        "expected_columns": ["customer_id","full_name","first_order_date","lifetime_order_count","lifetime_spend_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called marketing_attribution. Join {src('raw_campaigns')}, {src('raw_email_events')}, {src('raw_customers')}, and {src('raw_orders')}. For each campaign, find customers who received an email AND placed an order within 7 days. Include campaign_id, campaign_name, attributed_orders, attributed_revenue_dollars.",
        "model_name": "marketing_attribution",
        "expected_columns": ["campaign_id","campaign_name","attributed_orders","attributed_revenue_dollars"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called support_resolution_detail. Join {src('raw_support_tickets')}, {src('raw_customers')}, {src('raw_orders')}, and {src('raw_employees')}. Include ticket_id, customer_name, order_date, order_amount_dollars, ticket_category, assigned_employee_name, resolution_hours. Use CTEs.",
        "model_name": "support_resolution_detail",
        "expected_columns": ["ticket_id","customer_name","ticket_category","assigned_employee_name","resolution_hours"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called refund_analysis. Join {src('raw_refunds')}, {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_products')}. Calculate refund_rate (refund_amount / order total). Include order_id, product_names, refund_reason, refund_status, refund_rate. Use CTEs.",
        "model_name": "refund_analysis",
        "expected_columns": ["order_id","refund_reason","refund_status","refund_rate"],
        "expected_row_count": 25,
        "unique_key": "refund_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_reorder_report. Join {src('raw_inventory')}, {src('raw_products')}, {src('raw_warehouses')}, and {src('raw_suppliers')}. Show products needing reorder with supplier info. Include product_name, warehouse_name, quantity_on_hand, reorder_point, supplier_name, lead_time_days. Use CTEs.",
        "model_name": "inventory_reorder_report",
        "expected_columns": ["product_name","warehouse_name","quantity_on_hand","reorder_point","supplier_name","lead_time_days"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_journey. Join {src('raw_customers')}, {src('raw_sessions')}, {src('raw_page_views')}, and {src('raw_orders')}. For each customer: first session date, total sessions, total page views, first order date, total orders. Calculate days_to_first_order (first order - first session). Use CTEs.",
        "model_name": "customer_journey",
        "expected_columns": ["customer_id","first_name","first_session_date","total_sessions","total_page_views","first_order_date","days_to_first_order"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_delivery_analysis. Join {src('raw_shipping')}, {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_products')}. For each shipment: carrier, total items, total weight, delivery_days, on_time (delivered within 5 days), order_value_dollars. Use CTEs.",
        "model_name": "shipping_delivery_analysis",
        "expected_columns": ["shipment_id","carrier","total_items","delivery_days","on_time","order_value_dollars"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called customer_product_affinity. Join {src('raw_customers')}, {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_products')}. For each customer-product pair, count times purchased and total quantity. Include customer_id, first_name, product_name, times_purchased, total_quantity. Use CTEs.",
        "model_name": "customer_product_affinity",
        "expected_columns": ["customer_id","first_name","product_name","times_purchased","total_quantity"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called return_analysis_full. Join {src('raw_returns')}, {src('raw_order_items')}, {src('raw_products')}, and {src('raw_orders')}. Include return_id, product_name, category, return_reason, condition, refund_dollars, order_date, days_to_return (return requested - order date). Use CTEs.",
        "model_name": "return_analysis_full",
        "expected_columns": ["return_id","product_name","category","return_reason","refund_dollars","days_to_return"],
        "expected_row_count": 30,
        "unique_key": "return_id",
    },
    {
        "prompt": f"Write a dbt model called email_conversion_funnel. Join {src('raw_email_events')}, {src('raw_campaigns')}, {src('raw_customers')}, and {src('raw_orders')}. For each campaign: emails_sent, emails_opened, emails_clicked, orders_placed_within_7_days. Calculate conversion_rate (orders/clicked). Use CTEs.",
        "model_name": "email_conversion_funnel",
        "expected_columns": ["campaign_id","campaign_name","emails_sent","emails_opened","emails_clicked","orders_within_7d"],
        "expected_row_count": 12,
        "unique_key": "campaign_id",
    },
    {
        "prompt": f"Write a dbt model called warehouse_product_demand. Join {src('raw_inventory')}, {src('raw_warehouses')}, {src('raw_order_items')}, and {src('raw_products')}. For each warehouse-product combo: current stock, 30-day demand (orders in last 30 days), days_of_stock (stock / daily_demand). Use CTEs.",
        "model_name": "warehouse_product_demand",
        "expected_columns": ["warehouse_name","product_name","current_stock","demand_30d","days_of_stock"],
        "expected_row_count": None,
        "unique_key": None,
    },
    {
        "prompt": f"Write a dbt model called customer_support_orders. Join {src('raw_customers')}, {src('raw_support_tickets')}, {src('raw_orders')}, and {src('raw_refunds')}. For each customer: total_orders, total_tickets, total_refunds, ticket_rate (tickets/orders), refund_rate (refunds/orders). Use CTEs.",
        "model_name": "customer_support_orders",
        "expected_columns": ["customer_id","full_name","total_orders","total_tickets","total_refunds","ticket_rate","refund_rate"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
]
prompts.extend(complex_joins)

# =============================================================================
# CATEGORY 10: SUBQUERIES (20 prompts)
# =============================================================================

subqueries = [
    {
        "prompt": f"Write a dbt model called above_avg_orders from {src('raw_orders')}. Select orders where amount > the average amount across all orders. Include order_id, customer_id, order_date, amount_dollars, avg_amount_dollars.",
        "model_name": "above_avg_orders",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called customers_with_refunds. Select customers from {src('raw_customers')} who have at least one refund in {src('raw_refunds')} (via {src('raw_orders')}). Use EXISTS or IN. Include customer_id, first_name, email.",
        "model_name": "customers_with_refunds",
        "expected_columns": ["customer_id","first_name","email"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called max_order_per_customer from {src('raw_orders')}. For each customer, find their largest order using a correlated subquery or CTE. Include customer_id, order_id, order_date, max_amount_dollars.",
        "model_name": "max_order_per_customer",
        "expected_columns": ["customer_id","order_id","order_date","max_amount_dollars"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_above_customer_avg from {src('raw_orders')}. For each order, check if its amount exceeds the customer's average. Include order_id, customer_id, amount_dollars, customer_avg_dollars, is_above_avg.",
        "model_name": "orders_above_customer_avg",
        "expected_columns": ["order_id","customer_id","amount_dollars","customer_avg_dollars","is_above_avg"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called products_never_ordered. Select products from {src('raw_products')} that have no matching rows in {src('raw_order_items')}. Use NOT EXISTS or LEFT JOIN. Include product_id, name, category.",
        "model_name": "products_never_ordered",
        "expected_columns": ["product_id","name","category"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called customers_without_orders. Select customers from {src('raw_customers')} who have no orders in {src('raw_orders')}. Use NOT EXISTS. Include customer_id, first_name, last_name, email, created_at.",
        "model_name": "customers_without_orders",
        "expected_columns": ["customer_id","first_name","last_name","email"],
        "expected_row_count": None,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_with_multiple_payments. Select orders from {src('raw_orders')} that have more than one payment in {src('raw_payments')}. Use a subquery to count payments. Include order_id, order_date, amount_dollars, payment_count.",
        "model_name": "orders_with_multiple_payments",
        "expected_columns": ["order_id","order_date","amount_dollars","payment_count"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called high_value_customers. Select customers whose total spend (from {src('raw_orders')}) is in the top 10%. Use a CTE to calculate total spend per customer, then filter. Include customer_id, first_name, total_spend_dollars.",
        "model_name": "high_value_customers",
        "expected_columns": ["customer_id","first_name","total_spend_dollars"],
        "expected_row_count": 5,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called products_with_returns. Select products from {src('raw_products')} that have at least one return in {src('raw_returns')} (via {src('raw_order_items')}). Use EXISTS. Include product_id, name, category.",
        "model_name": "products_with_returns",
        "expected_columns": ["product_id","name","category"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called sessions_without_pageviews. Select sessions from {src('raw_sessions')} that have no page views in {src('raw_page_views')}. Use NOT EXISTS. Include session_id, customer_id, session_start, device_type.",
        "model_name": "sessions_without_pageviews",
        "expected_columns": ["session_id","customer_id","session_start","device_type"],
        "expected_row_count": None,
        "unique_key": "session_id",
    },
]
prompts.extend(subqueries)

# =============================================================================
# CATEGORY 11: DATE/STRING OPERATIONS (25 prompts)
# =============================================================================

date_string_ops = [
    {
        "prompt": f"Write a dbt model called orders_by_quarter from {src('raw_orders')}. Extract year and quarter from order_date. Group by year-quarter. Include order_quarter (e.g., '2023-Q1'), order_count, total_amount_dollars.",
        "model_name": "orders_by_quarter",
        "expected_columns": ["order_quarter","order_count","total_amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_quarter",
    },
    {
        "prompt": f"Write a dbt model called customer_email_domains from {src('raw_customers')}. Extract the domain from email (everything after @). Group by domain. Include email_domain, customer_count. Order by customer_count desc.",
        "model_name": "customer_email_domains",
        "expected_columns": ["email_domain","customer_count"],
        "expected_row_count": None,
        "unique_key": "email_domain",
    },
    {
        "prompt": f"Write a dbt model called customer_initials from {src('raw_customers')}. Create initials column as first letter of first_name + first letter of last_name (uppercase). Include customer_id, first_name, last_name, initials, email.",
        "model_name": "customer_initials",
        "expected_columns": ["customer_id","first_name","last_name","initials","email"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called orders_day_of_week from {src('raw_orders')}. Extract day of week from order_date (Monday=1). Group by day_of_week. Include day_name, order_count, avg_amount_dollars. Order by day_of_week.",
        "model_name": "orders_day_of_week",
        "expected_columns": ["day_of_week","day_name","order_count","avg_amount_dollars"],
        "expected_row_count": 7,
        "unique_key": "day_of_week",
    },
    {
        "prompt": f"Write a dbt model called orders_month_start from {src('raw_orders')}. Truncate order_date to first of month using DATE_TRUNC. Group by month_start. Include month_start, order_count, total_amount_dollars.",
        "model_name": "orders_month_start",
        "expected_columns": ["month_start","order_count","total_amount_dollars"],
        "expected_row_count": None,
        "unique_key": "month_start",
    },
    {
        "prompt": f"Write a dbt model called payment_processing_time from {src('raw_payments')}. Calculate processing_seconds as the difference between updated_at and created_at in seconds. Include payment_id, order_id, payment_method, processing_seconds.",
        "model_name": "payment_processing_time",
        "expected_columns": ["payment_id","order_id","payment_method","processing_seconds"],
        "expected_row_count": 119,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called customer_tenure from {src('raw_customers')}. Calculate tenure_days as current_date - created_at. Calculate tenure_months as tenure_days / 30. Include customer_id, first_name, created_at, tenure_days, tenure_months.",
        "model_name": "customer_tenure",
        "expected_columns": ["customer_id","first_name","tenure_days","tenure_months"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called product_sku_parts from {src('raw_products')}. Split the SKU (format: CAT-SUB-NUM) into sku_category, sku_subcategory, sku_number using string functions. Include product_id, name, sku, sku_category, sku_subcategory, sku_number.",
        "model_name": "product_sku_parts",
        "expected_columns": ["product_id","name","sku","sku_category","sku_subcategory","sku_number"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called session_hour_distribution from {src('raw_sessions')}. Extract hour from session_start. Group by hour. Include session_hour, session_count, avg_duration_minutes. Order by session_hour.",
        "model_name": "session_hour_distribution",
        "expected_columns": ["session_hour","session_count","avg_duration_minutes"],
        "expected_row_count": None,
        "unique_key": "session_hour",
    },
    {
        "prompt": f"Write a dbt model called shipping_duration_days from {src('raw_shipping')}. Calculate days between shipped_date and delivered_date. Handle NULLs for undelivered. Include shipment_id, order_id, carrier, shipped_date, delivered_date, transit_days.",
        "model_name": "shipping_duration_days",
        "expected_columns": ["shipment_id","order_id","carrier","shipped_date","delivered_date","transit_days"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
]
prompts.extend(date_string_ops)

# =============================================================================
# CATEGORY 12: CROSS-DOMAIN ANALYTICS (40 prompts)
# =============================================================================

cross_domain = [
    {
        "prompt": f"Write a dbt model called customer_health_score. Join {src('raw_customers')}, {src('raw_orders')}, {src('raw_support_tickets')}, {src('raw_reviews')}, and {src('raw_subscriptions')}. Score = order_count*10 + avg_rating*5 - ticket_count*3 + (active_subscription ? 20 : 0). Include customer_id, full_name, health_score. Use CTEs.",
        "model_name": "customer_health_score",
        "expected_columns": ["customer_id","full_name","health_score"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called product_demand_supply. Join {src('raw_products')}, {src('raw_order_items')}, {src('raw_inventory')}, and {src('raw_returns')}. For each product: total_sold (sum qty), total_returned, net_sold, current_stock, stock_coverage_days (stock / avg daily demand). Use CTEs.",
        "model_name": "product_demand_supply",
        "expected_columns": ["product_id","name","total_sold","total_returned","net_sold","current_stock"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called channel_performance. Join {src('raw_orders')}, {src('raw_sessions')}, and {src('raw_customers')}. For each channel (from orders): total_orders, total_revenue_dollars, unique_customers, avg_order_value. Compare to session channels. Use CTEs.",
        "model_name": "channel_performance",
        "expected_columns": ["channel","total_orders","total_revenue","unique_customers","avg_order_value"],
        "expected_row_count": 4,
        "unique_key": "channel",
    },
    {
        "prompt": f"Write a dbt model called daily_business_metrics. Join {src('raw_orders')}, {src('raw_sessions')}, and {src('raw_support_tickets')}. Group by date. Include metric_date, orders_count, revenue_dollars, sessions_count, tickets_opened. Use CTEs and full outer join on date.",
        "model_name": "daily_business_metrics",
        "expected_columns": ["metric_date","orders_count","revenue_dollars","sessions_count","tickets_opened"],
        "expected_row_count": None,
        "unique_key": "metric_date",
    },
    {
        "prompt": f"Write a dbt model called fct_orders. This is the core fact table. Join {src('raw_orders')}, {src('raw_order_items')}, {src('raw_payments')}, and {src('raw_shipping')}. Include order_id, customer_id, order_date, status, item_count, gross_amount_dollars, discount_dollars, net_amount_dollars, payment_status, shipping_status, carrier. Use CTEs.",
        "model_name": "fct_orders",
        "expected_columns": ["order_id","customer_id","order_date","status","item_count","gross_amount_dollars","net_amount_dollars"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called dim_customers. Join {src('raw_customers')}, {src('raw_addresses')}, {src('raw_orders')}, and {src('raw_subscriptions')}. Include customer_id, full_name, email, customer_type, default_city, default_state, first_order_date, total_orders, has_subscription. Use CTEs.",
        "model_name": "dim_customers",
        "expected_columns": ["customer_id","full_name","email","customer_type","first_order_date","total_orders","has_subscription"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called dim_products. Join {src('raw_products')}, {src('raw_categories')}, {src('raw_inventory')}, and {src('raw_reviews')}. Include product_id, name, category, subcategory, price_dollars, cost_dollars, margin_pct, total_stock, avg_rating, review_count. Use CTEs.",
        "model_name": "dim_products",
        "expected_columns": ["product_id","name","category","price_dollars","margin_pct","total_stock","avg_rating"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called fct_sessions. Join {src('raw_sessions')}, {src('raw_page_views')}, and {src('raw_customers')}. Include session_id, customer_id, customer_type, device_type, channel, session_duration_minutes, page_view_count, most_viewed_page. Use CTEs.",
        "model_name": "fct_sessions",
        "expected_columns": ["session_id","customer_id","device_type","channel","session_duration_minutes","page_view_count"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called review_impact. Join {src('raw_reviews')}, {src('raw_products')}, {src('raw_order_items')}, and {src('raw_orders')}. For each product, compare avg_rating to total_revenue and order_count. Include product_id, name, avg_rating, total_revenue_dollars, total_orders. Use CTEs.",
        "model_name": "review_impact",
        "expected_columns": ["product_id","name","avg_rating","total_revenue","total_orders"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called subscription_churn_analysis. Join {src('raw_subscriptions')}, {src('raw_customers')}, {src('raw_orders')}, and {src('raw_support_tickets')}. For churned subscribers: customer_id, full_name, plan, subscription_duration_days, orders_during_subscription, tickets_during_subscription. Use CTEs.",
        "model_name": "subscription_churn_analysis",
        "expected_columns": ["customer_id","full_name","plan","subscription_duration_days","orders_during_subscription"],
        "expected_row_count": None,
        "unique_key": "subscription_id",
    },
    {
        "prompt": f"Write a dbt model called shipping_cost_analysis. Join {src('raw_shipping')}, {src('raw_orders')}, {src('raw_order_items')}, and {src('raw_products')}. For each shipment: total_item_weight, actual_weight, shipping_cost_dollars, order_value_dollars, shipping_pct_of_order (shipping_cost / order_value * 100). Use CTEs.",
        "model_name": "shipping_cost_analysis",
        "expected_columns": ["shipment_id","total_item_weight","shipping_cost","order_value","shipping_pct_of_order"],
        "expected_row_count": 75,
        "unique_key": "shipment_id",
    },
    {
        "prompt": f"Write a dbt model called customer_engagement_score. Join {src('raw_customers')}, {src('raw_sessions')}, {src('raw_email_events')}, and {src('raw_reviews')}. Score = sessions*1 + emails_opened*2 + reviews_written*5. Include customer_id, full_name, engagement_score. Use CTEs.",
        "model_name": "customer_engagement_score",
        "expected_columns": ["customer_id","full_name","engagement_score"],
        "expected_row_count": 50,
        "unique_key": "customer_id",
    },
    {
        "prompt": f"Write a dbt model called weekly_kpis. Join {src('raw_orders')}, {src('raw_sessions')}, {src('raw_support_tickets')}, and {src('raw_email_events')}. Group by week (DATE_TRUNC to week). Include week_start, orders, revenue_dollars, sessions, tickets, emails_sent. Use CTEs.",
        "model_name": "weekly_kpis",
        "expected_columns": ["week_start","orders","revenue_dollars","sessions","tickets"],
        "expected_row_count": None,
        "unique_key": "week_start",
    },
    {
        "prompt": f"Write a dbt model called inventory_turnover. Join {src('raw_inventory')}, {src('raw_order_items')}, and {src('raw_products')}. For each product: current_stock, units_sold_30d (last 30 days), turnover_rate (units_sold / current_stock). Include product_id, name. Use CTEs.",
        "model_name": "inventory_turnover",
        "expected_columns": ["product_id","name","current_stock","units_sold_30d","turnover_rate"],
        "expected_row_count": 30,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called category_revenue_share. Join {src('raw_order_items')} and {src('raw_products')}. Calculate each category's share of total revenue (category_revenue / total_revenue * 100). Include category, total_revenue_dollars, revenue_share_pct. Use CTEs.",
        "model_name": "category_revenue_share",
        "expected_columns": ["category","total_revenue","revenue_share_pct"],
        "expected_row_count": None,
        "unique_key": "category",
    },
]
prompts.extend(cross_domain)

# =============================================================================
# CATEGORY 13: NULL/DEFENSIVE PATTERNS (15 prompts)
# =============================================================================

null_defensive = [
    {
        "prompt": f"Write a dbt model called orders_with_defaults from {src('raw_orders')}. Use COALESCE to replace: NULL notes with 'No notes', NULL discount_amount with 0. Include order_id, customer_id, order_date, amount_dollars, discount_dollars, notes.",
        "model_name": "orders_with_defaults",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars","discount_dollars","notes"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called safe_order_metrics from {src('raw_orders')}. Calculate avg_order_value using NULLIF to avoid division by zero. Calculate discount_rate as discount_amount / NULLIF(amount, 0). Include order_id, amount_dollars, discount_rate.",
        "model_name": "safe_order_metrics",
        "expected_columns": ["order_id","amount_dollars","discount_rate"],
        "expected_row_count": 100,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called payments_clean from {src('raw_payments')}. Filter out rows where status = 'failed'. Replace NULL processor_id with 'unknown'. Include payment_id, order_id, payment_method, amount_dollars, status, processor_id.",
        "model_name": "payments_clean",
        "expected_columns": ["payment_id","order_id","payment_method","amount_dollars","status","processor_id"],
        "expected_row_count": None,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called sessions_with_customer from {src('raw_sessions')}. Sessions have NULL customer_id for anonymous users. Use COALESCE(customer_id, -1) as customer_id_safe. Add is_anonymous boolean. Include session_id, customer_id_safe, is_anonymous, device_type.",
        "model_name": "sessions_with_customer",
        "expected_columns": ["session_id","customer_id_safe","is_anonymous","device_type"],
        "expected_row_count": 200,
        "unique_key": "session_id",
    },
    {
        "prompt": f"Write a dbt model called tickets_with_defaults from {src('raw_support_tickets')}. Replace NULL order_id with -1, NULL resolved_at with current_timestamp, NULL assigned_employee_id with -1. Include ticket_id, customer_id, order_id, category, status.",
        "model_name": "tickets_with_defaults",
        "expected_columns": ["ticket_id","customer_id","order_id","category","status"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called safe_review_stats from {src('raw_reviews')}. Group by product_id. Calculate avg_rating, but use CASE to return NULL if review_count < 3 (not enough data). Include product_id, review_count, avg_rating.",
        "model_name": "safe_review_stats",
        "expected_columns": ["product_id","review_count","avg_rating"],
        "expected_row_count": None,
        "unique_key": "product_id",
    },
    {
        "prompt": f"Write a dbt model called inventory_safe from {src('raw_inventory')}. Calculate days_since_restock as current_date - last_restock_date. Use COALESCE(quantity_on_hand, 0) for safety. Flag stale_inventory where days_since_restock > 90. Include inventory_id, product_id, quantity_on_hand, days_since_restock, stale_inventory.",
        "model_name": "inventory_safe",
        "expected_columns": ["inventory_id","product_id","quantity_on_hand","days_since_restock","stale_inventory"],
        "expected_row_count": 50,
        "unique_key": "inventory_id",
    },
]
prompts.extend(null_defensive)

# =============================================================================
# CATEGORY 14: MART/FACT/DIM MODELS (additional)
# =============================================================================

mart_models = [
    {
        "prompt": f"Write a dbt model called fct_payments. Join {src('raw_payments')} and {src('raw_orders')}. Include payment_id, order_id, customer_id, payment_date, payment_method, amount_dollars, is_refund, order_status. Only include successful payments. Use CTEs.",
        "model_name": "fct_payments",
        "expected_columns": ["payment_id","order_id","customer_id","payment_date","payment_method","amount_dollars","is_refund"],
        "expected_row_count": None,
        "unique_key": "payment_id",
    },
    {
        "prompt": f"Write a dbt model called fct_order_items. Join {src('raw_order_items')}, {src('raw_orders')}, and {src('raw_products')}. Include order_item_id, order_id, customer_id, order_date, product_id, product_name, category, quantity, unit_price_dollars, line_total_dollars. Use CTEs.",
        "model_name": "fct_order_items",
        "expected_columns": ["order_item_id","order_id","customer_id","order_date","product_name","quantity","line_total_dollars"],
        "expected_row_count": 200,
        "unique_key": "order_item_id",
    },
    {
        "prompt": f"Write a dbt model called fct_support_tickets. Join {src('raw_support_tickets')}, {src('raw_customers')}, and {src('raw_employees')}. Include ticket_id, customer_id, customer_name, order_id, category, priority, status, created_at, resolved_at, resolution_hours, assigned_employee_name. Use CTEs.",
        "model_name": "fct_support_tickets",
        "expected_columns": ["ticket_id","customer_id","customer_name","category","priority","status","resolution_hours"],
        "expected_row_count": 40,
        "unique_key": "ticket_id",
    },
    {
        "prompt": f"Write a dbt model called fct_email_events. Join {src('raw_email_events')}, {src('raw_campaigns')}, and {src('raw_customers')}. Include event_id, customer_id, customer_email, campaign_id, campaign_name, campaign_channel, event_type, event_date. Use CTEs.",
        "model_name": "fct_email_events",
        "expected_columns": ["event_id","customer_id","campaign_id","campaign_name","event_type","event_date"],
        "expected_row_count": 100,
        "unique_key": "event_id",
    },
    {
        "prompt": f"Write a dbt model called dim_dates. Generate a date spine from '2022-01-01' to '2024-12-31'. Include date_day, day_of_week, day_name, month_number, month_name, quarter, year, is_weekend. Use generate_series or recursive CTE.",
        "model_name": "dim_dates",
        "expected_columns": ["date_day","day_of_week","day_name","month_number","quarter","year","is_weekend"],
        "expected_row_count": 1096,
        "unique_key": "date_day",
    },
]
prompts.extend(mart_models)

# =============================================================================
# CATEGORY 15: SIMPLE SELECT / COPY BASELINES (10 prompts)
# =============================================================================

simple_selects = [
    {
        "prompt": f"Write a dbt model called raw_customers_copy that selects all columns from {src('raw_customers')}. Do not transform anything.",
        "model_name": "raw_customers_copy",
        "expected_columns": ["id","first_name","last_name","email"],
        "expected_row_count": 50,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called raw_orders_copy that selects all columns from {src('raw_orders')}.",
        "model_name": "raw_orders_copy",
        "expected_columns": ["id","customer_id","order_date","status","amount"],
        "expected_row_count": 100,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called ordered_customers that selects all columns from {src('raw_customers')}, ordered by created_at desc.",
        "model_name": "ordered_customers",
        "expected_columns": ["id","first_name","last_name","email","created_at"],
        "expected_row_count": 50,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called recent_orders that selects orders from {src('raw_orders')} where order_date >= '2024-01-01'. Include all columns.",
        "model_name": "recent_orders",
        "expected_columns": ["id","customer_id","order_date","status","amount"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called active_products that selects products from {src('raw_products')} where is_active = true. Include all columns.",
        "model_name": "active_products",
        "expected_columns": ["id","name","category","subcategory","unit_price","is_active"],
        "expected_row_count": None,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called completed_orders that selects only completed orders from {src('raw_orders')}. Include order_id (renamed from id), customer_id, order_date, amount_dollars (amount / 100.0).",
        "model_name": "completed_orders",
        "expected_columns": ["order_id","customer_id","order_date","amount_dollars"],
        "expected_row_count": None,
        "unique_key": "order_id",
    },
    {
        "prompt": f"Write a dbt model called limited_sessions that selects the 100 most recent sessions from {src('raw_sessions')}, ordered by session_start desc. Include all columns.",
        "model_name": "limited_sessions",
        "expected_columns": ["id","customer_id","session_start","session_end","device_type","channel"],
        "expected_row_count": 100,
        "unique_key": "id",
    },
    {
        "prompt": f"Write a dbt model called high_rated_reviews that selects reviews from {src('raw_reviews')} where rating >= 4. Include all columns.",
        "model_name": "high_rated_reviews",
        "expected_columns": ["id","customer_id","product_id","rating","review_text"],
        "expected_row_count": None,
        "unique_key": "id",
    },
]
prompts.extend(simple_selects)

# =============================================================================
# OUTPUT
# =============================================================================

# Write as Python module
output_path = os.path.join(os.path.dirname(__file__), "..", "rl_sandbox", "prompts.py")
with open(output_path, "w") as f:
    f.write('"""\n')
    f.write(f"Training prompts for dbt-coder RL. {len(prompts)} prompts across 15 categories.\n")
    f.write("\n")
    f.write("Categories:\n")
    categories = {
        "Staging": len(staging_prompts),
        "Two-table joins / aggregations": len(two_table_joins),
        "Three-table joins": len(three_table_joins),
        "Window functions": len(window_functions),
        "CASE/conditional": len(case_conditional),
        "UNIONs (fusion-divergent)": len(union_prompts),
        "Incremental models": len(incremental_prompts),
        "Jinja patterns": len(jinja_prompts),
        "Complex multi-hop joins": len(complex_joins),
        "Subqueries": len(subqueries),
        "Date/string operations": len(date_string_ops),
        "Cross-domain analytics": len(cross_domain),
        "NULL/defensive patterns": len(null_defensive),
        "Mart/fact/dim models": len(mart_models),
        "Simple selects": len(simple_selects),
    }
    for cat, count in categories.items():
        f.write(f"  - {cat}: {count}\n")
    f.write('"""\n\n')
    f.write("RL_PROMPTS = ")
    # Pretty-print the list
    f.write("[\n")
    for i, p in enumerate(prompts):
        f.write("    {\n")
        for key in ["prompt", "model_name", "expected_columns", "expected_row_count", "unique_key"]:
            val = p.get(key)
            if isinstance(val, str):
                # Escape any quotes in the string
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

print(f"Generated {len(prompts)} prompts")
for cat, count in categories.items():
    print(f"  {cat}: {count}")
