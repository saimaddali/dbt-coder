#!/usr/bin/env python3
"""Generate batch 3 of prompts to reach 500+ total."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rl_sandbox'))
from prompts import RL_PROMPTS

BATCH3 = [
    # === More two-table joins (20) ===
    {
        "prompt": "Create a dbt model that joins raw_orders with raw_shipping to show order delivery times in days.",
        "model_name": "order_delivery_times",
        "expected_columns": ["order_id", "customer_id", "order_date", "shipped_date", "delivered_date", "delivery_days"],
        "expected_row_count": 75,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model joining raw_products with raw_suppliers to show each product's supplier info.",
        "model_name": "product_suppliers",
        "expected_columns": ["product_id", "product_name", "supplier_id", "supplier_name", "supplier_country"],
        "expected_row_count": 30,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model joining raw_customers with raw_subscriptions to show active subscriber details.",
        "model_name": "active_subscribers",
        "expected_columns": ["customer_id", "first_name", "last_name", "plan", "start_date", "mrr"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_orders with raw_refunds to show refund rates per order.",
        "model_name": "order_refund_details",
        "expected_columns": ["order_id", "order_date", "refund_amount", "refund_reason"],
        "expected_row_count": 25,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_campaigns with raw_email_events to count events per campaign.",
        "model_name": "campaign_email_stats",
        "expected_columns": ["campaign_id", "campaign_name", "total_sent", "total_opened", "total_clicked"],
        "expected_row_count": 12,
        "unique_key": "campaign_id"
    },
    {
        "prompt": "Create a dbt model joining raw_warehouses with raw_inventory to show total stock per warehouse.",
        "model_name": "warehouse_stock_levels",
        "expected_columns": ["warehouse_id", "warehouse_name", "warehouse_city", "total_quantity", "product_count"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt model joining raw_order_items with raw_returns to show return rate per product.",
        "model_name": "product_return_rates",
        "expected_columns": ["product_id", "total_ordered", "total_returned", "return_rate"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model joining raw_customers with raw_reviews to show average rating per customer.",
        "model_name": "customer_avg_ratings",
        "expected_columns": ["customer_id", "first_name", "last_name", "review_count", "avg_rating"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_sessions with raw_customers to show sessions with customer names.",
        "model_name": "customer_sessions_detail",
        "expected_columns": ["session_id", "customer_id", "first_name", "session_start", "session_end", "device", "channel"],
        "expected_row_count": 200,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt model joining raw_orders with raw_promotions to show which orders used promo codes.",
        "model_name": "orders_with_promos",
        "expected_columns": ["order_id", "customer_id", "order_date", "promo_code", "discount_type"],
        "expected_row_count": None,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model joining raw_employees with raw_support_tickets to count tickets handled per employee.",
        "model_name": "employee_ticket_counts",
        "expected_columns": ["employee_id", "employee_name", "department", "ticket_count"],
        "expected_row_count": None,
        "unique_key": "employee_id"
    },
    {
        "prompt": "Create a dbt model joining raw_products with raw_categories to show product category hierarchy.",
        "model_name": "product_category_detail",
        "expected_columns": ["product_id", "product_name", "category_id", "category_name"],
        "expected_row_count": 30,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model joining raw_shipping with raw_warehouses to show shipments by warehouse.",
        "model_name": "warehouse_shipments",
        "expected_columns": ["warehouse_id", "warehouse_name", "shipment_count", "avg_shipping_cost"],
        "expected_row_count": None,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt model joining raw_subscriptions with raw_payments to show subscription payment history.",
        "model_name": "subscription_payments",
        "expected_columns": ["customer_id", "plan", "payment_id", "payment_method", "amount"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_page_views with raw_sessions to show page views with session context.",
        "model_name": "enriched_page_views",
        "expected_columns": ["page_view_id", "session_id", "customer_id", "page_url", "view_timestamp", "device"],
        "expected_row_count": 300,
        "unique_key": "page_view_id"
    },
    {
        "prompt": "Create a dbt model joining raw_reviews with raw_products to show reviews with product names.",
        "model_name": "product_reviews_detail",
        "expected_columns": ["review_id", "product_name", "customer_id", "rating", "review_text", "created_at"],
        "expected_row_count": 60,
        "unique_key": "review_id"
    },
    {
        "prompt": "Create a dbt model joining raw_returns with raw_orders to show returns with order context.",
        "model_name": "returns_with_orders",
        "expected_columns": ["return_id", "order_id", "customer_id", "order_date", "return_reason", "refund_amount"],
        "expected_row_count": 30,
        "unique_key": "return_id"
    },
    {
        "prompt": "Create a dbt model joining raw_inventory with raw_products to show low-stock products below reorder point.",
        "model_name": "low_stock_products",
        "expected_columns": ["product_id", "product_name", "quantity_on_hand", "reorder_point"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model joining raw_email_events with raw_customers to show email engagement per customer.",
        "model_name": "customer_email_engagement",
        "expected_columns": ["customer_id", "first_name", "last_name", "emails_sent", "emails_opened", "emails_clicked"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_support_tickets with raw_orders to show tickets related to specific orders.",
        "model_name": "order_support_tickets",
        "expected_columns": ["ticket_id", "order_id", "customer_id", "ticket_category", "priority", "status"],
        "expected_row_count": None,
        "unique_key": "ticket_id"
    },

    # === More three-table joins (15) ===
    {
        "prompt": "Create a dbt model joining raw_orders, raw_order_items, and raw_shipping to show shipped item details.",
        "model_name": "shipped_item_details",
        "expected_columns": ["order_id", "product_id", "quantity", "carrier", "shipped_date", "delivered_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_customers, raw_orders, and raw_returns to show customer return history.",
        "model_name": "customer_return_history",
        "expected_columns": ["customer_id", "first_name", "order_id", "return_reason", "refund_amount"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_products, raw_order_items, and raw_reviews to show product performance with ratings.",
        "model_name": "product_performance_ratings",
        "expected_columns": ["product_id", "product_name", "total_sold", "total_revenue", "avg_rating", "review_count"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model joining raw_sessions, raw_page_views, and raw_customers to show browsing behavior by customer.",
        "model_name": "customer_browsing_behavior",
        "expected_columns": ["customer_id", "first_name", "total_sessions", "total_page_views", "avg_pages_per_session"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_orders, raw_payments, and raw_customers to show payment summary per customer.",
        "model_name": "customer_payment_summary_v2",
        "expected_columns": ["customer_id", "first_name", "last_name", "total_orders", "total_paid", "avg_payment"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_inventory, raw_products, and raw_warehouses to show stock distribution across warehouses.",
        "model_name": "stock_distribution",
        "expected_columns": ["product_id", "product_name", "warehouse_name", "warehouse_city", "quantity_on_hand"],
        "expected_row_count": 50,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_campaigns, raw_email_events, and raw_customers to show campaign reach by customer segment.",
        "model_name": "campaign_customer_reach",
        "expected_columns": ["campaign_id", "campaign_name", "customer_id", "first_name", "event_count"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model joining raw_support_tickets, raw_customers, and raw_employees to show ticket resolution details.",
        "model_name": "ticket_resolution_details",
        "expected_columns": ["ticket_id", "customer_name", "employee_name", "ticket_category", "priority", "status"],
        "expected_row_count": 40,
        "unique_key": "ticket_id"
    },
    {
        "prompt": "Create a dbt model joining raw_subscriptions, raw_customers, and raw_payments to show subscriber payment activity.",
        "model_name": "subscriber_payment_activity",
        "expected_columns": ["customer_id", "first_name", "plan", "mrr", "total_payments", "total_amount_paid"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_order_items, raw_products, and raw_categories to show sales by category.",
        "model_name": "sales_by_category",
        "expected_columns": ["category_id", "category_name", "total_items_sold", "total_revenue"],
        "expected_row_count": None,
        "unique_key": "category_id"
    },
    {
        "prompt": "Create a dbt model joining raw_orders, raw_shipping, and raw_customers to show late deliveries per customer.",
        "model_name": "late_deliveries_by_customer",
        "expected_columns": ["customer_id", "first_name", "last_name", "late_delivery_count", "avg_delay_days"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model joining raw_products, raw_suppliers, and raw_inventory to show supplier stock coverage.",
        "model_name": "supplier_stock_coverage",
        "expected_columns": ["supplier_id", "supplier_name", "product_count", "total_stock", "avg_stock_per_product"],
        "expected_row_count": None,
        "unique_key": "supplier_id"
    },
    {
        "prompt": "Create a dbt model joining raw_reviews, raw_customers, and raw_products to show detailed review feed.",
        "model_name": "review_feed",
        "expected_columns": ["review_id", "customer_name", "product_name", "rating", "review_text", "created_at"],
        "expected_row_count": 60,
        "unique_key": "review_id"
    },
    {
        "prompt": "Create a dbt model joining raw_orders, raw_order_items, and raw_refunds to calculate net revenue per order.",
        "model_name": "order_net_revenue",
        "expected_columns": ["order_id", "gross_revenue", "total_refunds", "net_revenue"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model joining raw_page_views, raw_sessions, and raw_orders to identify sessions that led to purchases.",
        "model_name": "conversion_sessions",
        "expected_columns": ["session_id", "customer_id", "session_start", "page_view_count", "order_id", "order_total"],
        "expected_row_count": None,
        "unique_key": "session_id"
    },

    # === More aggregations / GROUP BY (20) ===
    {
        "prompt": "Create a dbt model showing total revenue per product category.",
        "model_name": "revenue_by_category",
        "expected_columns": ["category_name", "total_revenue", "order_count"],
        "expected_row_count": None,
        "unique_key": "category_name"
    },
    {
        "prompt": "Create a dbt model showing average order value by customer acquisition channel.",
        "model_name": "aov_by_channel",
        "expected_columns": ["channel", "avg_order_value", "order_count"],
        "expected_row_count": None,
        "unique_key": "channel"
    },
    {
        "prompt": "Create a dbt model showing monthly recurring revenue (MRR) trend from subscriptions.",
        "model_name": "mrr_trend",
        "expected_columns": ["month", "total_mrr", "subscriber_count"],
        "expected_row_count": None,
        "unique_key": "month"
    },
    {
        "prompt": "Create a dbt model showing support ticket volume by category and priority.",
        "model_name": "ticket_volume_by_category",
        "expected_columns": ["ticket_category", "priority", "ticket_count", "avg_resolution_hours"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model showing email campaign performance with open and click rates.",
        "model_name": "campaign_performance",
        "expected_columns": ["campaign_id", "campaign_name", "send_count", "open_rate", "click_rate"],
        "expected_row_count": 12,
        "unique_key": "campaign_id"
    },
    {
        "prompt": "Create a dbt model showing daily page view counts and unique visitors.",
        "model_name": "daily_traffic",
        "expected_columns": ["view_date", "total_page_views", "unique_visitors"],
        "expected_row_count": None,
        "unique_key": "view_date"
    },
    {
        "prompt": "Create a dbt model showing inventory turnover rate per product.",
        "model_name": "inventory_turnover",
        "expected_columns": ["product_id", "product_name", "quantity_sold", "quantity_on_hand", "turnover_rate"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model showing shipping cost analysis by carrier.",
        "model_name": "shipping_cost_by_carrier",
        "expected_columns": ["carrier", "shipment_count", "avg_shipping_cost", "total_shipping_cost"],
        "expected_row_count": None,
        "unique_key": "carrier"
    },
    {
        "prompt": "Create a dbt model showing customer lifetime value (total spend) ranked by value.",
        "model_name": "customer_ltv",
        "expected_columns": ["customer_id", "first_name", "last_name", "total_spend", "order_count", "first_order_date", "last_order_date"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing return reasons distribution with counts and percentages.",
        "model_name": "return_reason_distribution",
        "expected_columns": ["return_reason", "return_count", "pct_of_total"],
        "expected_row_count": None,
        "unique_key": "return_reason"
    },
    {
        "prompt": "Create a dbt model showing weekly order volume with week-over-week change.",
        "model_name": "weekly_order_volume",
        "expected_columns": ["order_week", "order_count", "total_revenue", "wow_change_pct"],
        "expected_row_count": None,
        "unique_key": "order_week"
    },
    {
        "prompt": "Create a dbt model showing product review sentiment summary by rating bucket.",
        "model_name": "review_sentiment_summary",
        "expected_columns": ["rating_bucket", "review_count", "pct_of_total"],
        "expected_row_count": None,
        "unique_key": "rating_bucket"
    },
    {
        "prompt": "Create a dbt model showing average session duration by device type.",
        "model_name": "session_duration_by_device",
        "expected_columns": ["device", "session_count", "avg_duration_minutes"],
        "expected_row_count": None,
        "unique_key": "device"
    },
    {
        "prompt": "Create a dbt model showing top 10 most viewed pages by total view count.",
        "model_name": "top_pages",
        "expected_columns": ["page_url", "view_count", "unique_viewers", "avg_duration_seconds"],
        "expected_row_count": None,
        "unique_key": "page_url"
    },
    {
        "prompt": "Create a dbt model showing supplier performance metrics (products supplied, total stock, avg lead time).",
        "model_name": "supplier_performance",
        "expected_columns": ["supplier_id", "supplier_name", "product_count", "total_stock", "avg_lead_time_days"],
        "expected_row_count": 10,
        "unique_key": "supplier_id"
    },
    {
        "prompt": "Create a dbt model showing warehouse capacity utilization.",
        "model_name": "warehouse_utilization",
        "expected_columns": ["warehouse_id", "warehouse_name", "total_stock", "capacity", "utilization_pct"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt model showing payment method popularity with transaction counts and total amounts.",
        "model_name": "payment_method_stats",
        "expected_columns": ["payment_method", "transaction_count", "total_amount", "avg_amount"],
        "expected_row_count": None,
        "unique_key": "payment_method"
    },
    {
        "prompt": "Create a dbt model showing monthly new customer acquisition count.",
        "model_name": "monthly_new_customers",
        "expected_columns": ["signup_month", "new_customer_count"],
        "expected_row_count": None,
        "unique_key": "signup_month"
    },
    {
        "prompt": "Create a dbt model showing order status distribution with counts and percentages.",
        "model_name": "order_status_distribution",
        "expected_columns": ["status", "order_count", "pct_of_total"],
        "expected_row_count": None,
        "unique_key": "status"
    },
    {
        "prompt": "Create a dbt model showing promotion effectiveness (usage count, total discount given, avg discount).",
        "model_name": "promotion_effectiveness",
        "expected_columns": ["promo_id", "promo_code", "discount_type", "times_used", "total_discount"],
        "expected_row_count": None,
        "unique_key": "promo_id"
    },

    # === More window functions (15) ===
    {
        "prompt": "Create a dbt model that ranks products by total revenue using RANK().",
        "model_name": "product_revenue_rank",
        "expected_columns": ["product_id", "product_name", "total_revenue", "revenue_rank"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model showing each customer's order sequence number using ROW_NUMBER().",
        "model_name": "customer_order_sequence",
        "expected_columns": ["customer_id", "order_id", "order_date", "order_sequence_number"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model showing running total of payments per customer ordered by date.",
        "model_name": "customer_running_payments",
        "expected_columns": ["customer_id", "payment_id", "payment_date", "amount", "running_total"],
        "expected_row_count": None,
        "unique_key": "payment_id"
    },
    {
        "prompt": "Create a dbt model showing days between consecutive orders per customer using LAG.",
        "model_name": "days_between_orders",
        "expected_columns": ["customer_id", "order_id", "order_date", "prev_order_date", "days_since_last_order"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model showing each product's sales as a percentage of its category's total sales.",
        "model_name": "product_category_share",
        "expected_columns": ["product_id", "product_name", "category_name", "product_revenue", "category_revenue", "pct_of_category"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model showing customer rank by total spend using DENSE_RANK().",
        "model_name": "customer_spend_rank",
        "expected_columns": ["customer_id", "first_name", "total_spend", "spend_rank"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing 7-day moving average of daily revenue.",
        "model_name": "revenue_moving_avg_7d",
        "expected_columns": ["order_date", "daily_revenue", "moving_avg_7d"],
        "expected_row_count": None,
        "unique_key": "order_date"
    },
    {
        "prompt": "Create a dbt model showing cumulative page views per session using window SUM.",
        "model_name": "cumulative_page_views",
        "expected_columns": ["session_id", "page_url", "view_timestamp", "cumulative_views"],
        "expected_row_count": 300,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model showing each employee's ticket count percentile using PERCENT_RANK().",
        "model_name": "employee_ticket_percentile",
        "expected_columns": ["employee_id", "employee_name", "ticket_count", "percentile_rank"],
        "expected_row_count": None,
        "unique_key": "employee_id"
    },
    {
        "prompt": "Create a dbt model showing the next expected delivery date per customer using LEAD.",
        "model_name": "next_delivery_per_customer",
        "expected_columns": ["customer_id", "order_id", "delivered_date", "next_delivery_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model partitioning customers into quartiles by total spend using NTILE(4).",
        "model_name": "customer_spend_quartiles",
        "expected_columns": ["customer_id", "first_name", "total_spend", "spend_quartile"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing month-over-month revenue growth rate using LAG.",
        "model_name": "monthly_revenue_growth",
        "expected_columns": ["order_month", "monthly_revenue", "prev_month_revenue", "growth_rate_pct"],
        "expected_row_count": None,
        "unique_key": "order_month"
    },
    {
        "prompt": "Create a dbt model showing first and last purchase amount per customer using FIRST_VALUE and LAST_VALUE.",
        "model_name": "customer_first_last_purchase",
        "expected_columns": ["customer_id", "first_name", "first_purchase_amount", "last_purchase_amount", "order_count"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model ranking support tickets by response time within each priority level.",
        "model_name": "ticket_response_rank",
        "expected_columns": ["ticket_id", "priority", "created_at", "resolved_at", "response_hours", "rank_in_priority"],
        "expected_row_count": None,
        "unique_key": "ticket_id"
    },
    {
        "prompt": "Create a dbt model showing running count of reviews per product over time.",
        "model_name": "product_review_running_count",
        "expected_columns": ["product_id", "review_id", "created_at", "rating", "running_review_count"],
        "expected_row_count": 60,
        "unique_key": "review_id"
    },

    # === More CASE/conditional (15) ===
    {
        "prompt": "Create a dbt model that segments orders into size tiers: 'small' (<$25), 'medium' ($25-$100), 'large' (>$100).",
        "model_name": "order_size_tiers",
        "expected_columns": ["order_id", "customer_id", "order_total", "size_tier"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model that flags customers as 'churned' if no order in 90 days, 'at_risk' if 60-90 days, 'active' otherwise.",
        "model_name": "customer_churn_flags",
        "expected_columns": ["customer_id", "first_name", "last_order_date", "days_since_last_order", "churn_status"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model that categorizes reviews as 'positive' (4-5), 'neutral' (3), or 'negative' (1-2).",
        "model_name": "review_sentiment_categories",
        "expected_columns": ["review_id", "product_id", "rating", "sentiment"],
        "expected_row_count": 60,
        "unique_key": "review_id"
    },
    {
        "prompt": "Create a dbt model that labels shipping speed: 'same_day', 'next_day', 'standard' (2-5 days), 'slow' (>5 days).",
        "model_name": "shipping_speed_labels",
        "expected_columns": ["order_id", "shipped_date", "delivered_date", "delivery_days", "speed_label"],
        "expected_row_count": None,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model that categorizes products by price range: 'budget' (<$20), 'mid' ($20-$50), 'premium' (>$50).",
        "model_name": "product_price_tiers",
        "expected_columns": ["product_id", "product_name", "price", "price_tier"],
        "expected_row_count": 30,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model that flags subscriptions as 'trial', 'active', 'cancelled', or 'expired' based on dates and status.",
        "model_name": "subscription_status_flags",
        "expected_columns": ["subscription_id", "customer_id", "plan", "start_date", "end_date", "status_flag"],
        "expected_row_count": 25,
        "unique_key": "subscription_id"
    },
    {
        "prompt": "Create a dbt model that assigns customer value tiers based on total spend: 'bronze' (<$100), 'silver' ($100-$500), 'gold' (>$500).",
        "model_name": "customer_value_tiers",
        "expected_columns": ["customer_id", "first_name", "total_spend", "value_tier"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model that labels sessions by engagement: 'bounce' (1 page), 'low' (2-3), 'medium' (4-7), 'high' (8+).",
        "model_name": "session_engagement_labels",
        "expected_columns": ["session_id", "customer_id", "page_view_count", "engagement_level"],
        "expected_row_count": 200,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt model that classifies support tickets by resolution speed: 'fast' (<24h), 'normal' (24-72h), 'slow' (>72h).",
        "model_name": "ticket_resolution_speed",
        "expected_columns": ["ticket_id", "priority", "created_at", "resolved_at", "resolution_hours", "speed_class"],
        "expected_row_count": None,
        "unique_key": "ticket_id"
    },
    {
        "prompt": "Create a dbt model flagging orders with potential fraud: multiple payment methods, high value, or address mismatch.",
        "model_name": "fraud_flag_orders",
        "expected_columns": ["order_id", "customer_id", "order_total", "payment_count", "fraud_flag"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model that labels inventory status: 'out_of_stock' (0), 'low' (<reorder point), 'adequate' (>= reorder point).",
        "model_name": "inventory_status_labels",
        "expected_columns": ["product_id", "product_name", "quantity_on_hand", "reorder_point", "stock_status"],
        "expected_row_count": 50,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model categorizing campaigns by ROI: 'high_roi', 'medium_roi', 'low_roi' based on conversions vs budget.",
        "model_name": "campaign_roi_categories",
        "expected_columns": ["campaign_id", "campaign_name", "budget", "roi_category"],
        "expected_row_count": 12,
        "unique_key": "campaign_id"
    },
    {
        "prompt": "Create a dbt model that flags refunds as 'full' or 'partial' based on comparing refund amount to order total.",
        "model_name": "refund_type_flags",
        "expected_columns": ["refund_id", "order_id", "refund_amount", "order_total", "refund_type"],
        "expected_row_count": 25,
        "unique_key": "refund_id"
    },
    {
        "prompt": "Create a dbt model that labels employees by tenure: 'new' (<1yr), 'experienced' (1-3yr), 'veteran' (>3yr).",
        "model_name": "employee_tenure_labels",
        "expected_columns": ["employee_id", "employee_name", "hire_date", "tenure_years", "tenure_label"],
        "expected_row_count": 15,
        "unique_key": "employee_id"
    },
    {
        "prompt": "Create a dbt model that assigns day-of-week labels and flags weekend orders.",
        "model_name": "orders_day_of_week",
        "expected_columns": ["order_id", "order_date", "day_of_week", "is_weekend"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },

    # === More complex multi-hop (4+ tables) (15) ===
    {
        "prompt": "Create a dbt model showing full order detail: customer name, items with product names, payments, and shipping status.",
        "model_name": "full_order_detail",
        "expected_columns": ["order_id", "customer_name", "product_name", "quantity", "item_price", "payment_method", "shipping_status"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model showing customer 360 view: orders, payments, subscriptions, support tickets, and reviews summary.",
        "model_name": "customer_360_full",
        "expected_columns": ["customer_id", "first_name", "total_orders", "total_spent", "active_subscription", "open_tickets", "avg_rating_given"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing product lifecycle: inventory, sales, returns, and reviews aggregated per product.",
        "model_name": "product_lifecycle",
        "expected_columns": ["product_id", "product_name", "current_stock", "total_sold", "total_returned", "avg_review_rating"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model for marketing attribution: sessions, page views, orders, and campaign source.",
        "model_name": "marketing_attribution",
        "expected_columns": ["session_id", "customer_id", "channel", "campaign_name", "page_views", "order_id", "order_total"],
        "expected_row_count": None,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt model showing warehouse efficiency: inventory, shipments, products, and supplier lead times.",
        "model_name": "warehouse_efficiency",
        "expected_columns": ["warehouse_id", "warehouse_name", "product_count", "total_stock", "shipments_sent", "avg_supplier_lead_time"],
        "expected_row_count": None,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt model showing customer journey: first session to first order with all touchpoints.",
        "model_name": "customer_journey_first_order",
        "expected_columns": ["customer_id", "first_name", "first_session_date", "first_order_date", "days_to_convert", "sessions_before_order", "pages_viewed"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing order profitability: items, payments, shipping costs, and refunds per order.",
        "model_name": "order_profitability",
        "expected_columns": ["order_id", "gross_revenue", "shipping_cost", "refund_amount", "net_profit"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model for supply chain overview: suppliers, products, inventory, and warehouse locations.",
        "model_name": "supply_chain_overview",
        "expected_columns": ["supplier_name", "product_name", "warehouse_name", "quantity_on_hand", "reorder_point", "lead_time_days"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model showing customer satisfaction score: reviews, support tickets, returns per customer.",
        "model_name": "customer_satisfaction_score",
        "expected_columns": ["customer_id", "first_name", "avg_review_rating", "ticket_count", "return_count", "satisfaction_score"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model for cohort analysis: customer signup month, first order, retention at 30/60/90 days.",
        "model_name": "customer_cohort_retention",
        "expected_columns": ["signup_cohort", "cohort_size", "ordered_within_30d", "ordered_within_60d", "ordered_within_90d"],
        "expected_row_count": None,
        "unique_key": "signup_cohort"
    },
    {
        "prompt": "Create a dbt model showing email-to-purchase funnel: campaigns, email events, sessions, orders.",
        "model_name": "email_purchase_funnel",
        "expected_columns": ["campaign_id", "campaign_name", "emails_sent", "emails_opened", "sessions_started", "orders_placed", "conversion_rate"],
        "expected_row_count": None,
        "unique_key": "campaign_id"
    },
    {
        "prompt": "Create a dbt model showing product recommendation data: customers who bought X also bought Y.",
        "model_name": "product_co_purchase",
        "expected_columns": ["product_a_id", "product_a_name", "product_b_id", "product_b_name", "co_purchase_count"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model showing employee workload: tickets assigned, resolved, avg resolution time, by department.",
        "model_name": "employee_workload",
        "expected_columns": ["employee_id", "employee_name", "department", "tickets_assigned", "tickets_resolved", "avg_resolution_hours"],
        "expected_row_count": None,
        "unique_key": "employee_id"
    },
    {
        "prompt": "Create a dbt model for daily business dashboard: orders, revenue, new customers, sessions, tickets.",
        "model_name": "daily_business_dashboard",
        "expected_columns": ["report_date", "total_orders", "total_revenue", "new_customers", "total_sessions", "open_tickets"],
        "expected_row_count": None,
        "unique_key": "report_date"
    },
    {
        "prompt": "Create a dbt model showing geographic sales analysis: customer addresses, orders, products, categories.",
        "model_name": "geographic_sales",
        "expected_columns": ["state", "city", "total_orders", "total_revenue", "top_category", "customer_count"],
        "expected_row_count": None,
        "unique_key": None
    },

    # === More subqueries (10) ===
    {
        "prompt": "Create a dbt model showing customers whose total spend exceeds the overall average customer spend.",
        "model_name": "above_avg_spenders",
        "expected_columns": ["customer_id", "first_name", "last_name", "total_spend", "avg_customer_spend"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing products that have never been returned.",
        "model_name": "never_returned_products",
        "expected_columns": ["product_id", "product_name", "total_sold"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model showing customers who have both a subscription and at least one support ticket.",
        "model_name": "subscribers_with_tickets",
        "expected_columns": ["customer_id", "first_name", "plan", "ticket_count"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing orders where the total exceeds twice the customer's average order value.",
        "model_name": "unusual_large_orders",
        "expected_columns": ["order_id", "customer_id", "order_total", "customer_avg_order", "ratio"],
        "expected_row_count": None,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model showing products with above-average review ratings in their category.",
        "model_name": "above_avg_rated_products",
        "expected_columns": ["product_id", "product_name", "category_name", "avg_rating", "category_avg_rating"],
        "expected_row_count": None,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt model showing customers who placed an order but never left a review.",
        "model_name": "customers_no_reviews",
        "expected_columns": ["customer_id", "first_name", "last_name", "order_count"],
        "expected_row_count": None,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model showing warehouses that have products below reorder point.",
        "model_name": "warehouses_needing_restock",
        "expected_columns": ["warehouse_id", "warehouse_name", "low_stock_product_count"],
        "expected_row_count": None,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt model showing sessions that had more page views than the average session.",
        "model_name": "high_engagement_sessions",
        "expected_columns": ["session_id", "customer_id", "page_view_count", "avg_session_views"],
        "expected_row_count": None,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt model showing campaigns whose click rate exceeds the median click rate.",
        "model_name": "top_performing_campaigns",
        "expected_columns": ["campaign_id", "campaign_name", "click_rate", "median_click_rate"],
        "expected_row_count": None,
        "unique_key": "campaign_id"
    },
    {
        "prompt": "Create a dbt model showing employees who handle more tickets than the department average.",
        "model_name": "high_performing_employees",
        "expected_columns": ["employee_id", "employee_name", "department", "ticket_count", "dept_avg_tickets"],
        "expected_row_count": None,
        "unique_key": "employee_id"
    },

    # === More date/string operations (10) ===
    {
        "prompt": "Create a dbt model extracting year, quarter, and month from order dates.",
        "model_name": "order_date_parts",
        "expected_columns": ["order_id", "order_date", "order_year", "order_quarter", "order_month"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model calculating customer age in days since signup.",
        "model_name": "customer_age_days",
        "expected_columns": ["customer_id", "first_name", "created_at", "age_in_days"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model extracting email domain from customer email addresses.",
        "model_name": "customer_email_domains",
        "expected_columns": ["customer_id", "email", "email_domain"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model calculating subscription duration in months.",
        "model_name": "subscription_duration_months",
        "expected_columns": ["subscription_id", "customer_id", "start_date", "end_date", "duration_months"],
        "expected_row_count": 25,
        "unique_key": "subscription_id"
    },
    {
        "prompt": "Create a dbt model showing session duration in minutes calculated from start and end timestamps.",
        "model_name": "session_duration_calc",
        "expected_columns": ["session_id", "customer_id", "session_start", "session_end", "duration_minutes"],
        "expected_row_count": 200,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt model showing order recency: days since each order relative to the most recent order date.",
        "model_name": "order_recency",
        "expected_columns": ["order_id", "customer_id", "order_date", "days_ago"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model concatenating customer first and last name into full_name, uppercased.",
        "model_name": "customer_full_names",
        "expected_columns": ["customer_id", "full_name", "email"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model calculating average shipping transit time in days by month.",
        "model_name": "monthly_transit_time",
        "expected_columns": ["ship_month", "avg_transit_days", "shipment_count"],
        "expected_row_count": None,
        "unique_key": "ship_month"
    },
    {
        "prompt": "Create a dbt model showing support ticket resolution time in hours.",
        "model_name": "ticket_resolution_hours",
        "expected_columns": ["ticket_id", "customer_id", "created_at", "resolved_at", "resolution_hours"],
        "expected_row_count": None,
        "unique_key": "ticket_id"
    },
    {
        "prompt": "Create a dbt model truncating page view timestamps to hourly buckets and counting views per hour.",
        "model_name": "hourly_page_views",
        "expected_columns": ["view_hour", "page_view_count", "unique_sessions"],
        "expected_row_count": None,
        "unique_key": "view_hour"
    },

    # === More mart/dim/fact models (15) ===
    {
        "prompt": "Create a dbt dimension model for customers including demographics and acquisition details.",
        "model_name": "dim_customers_full",
        "expected_columns": ["customer_key", "customer_id", "first_name", "last_name", "email", "created_at", "is_active"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt dimension model for products including category and supplier info.",
        "model_name": "dim_products",
        "expected_columns": ["product_key", "product_id", "product_name", "category_name", "supplier_name", "price"],
        "expected_row_count": 30,
        "unique_key": "product_id"
    },
    {
        "prompt": "Create a dbt fact model for order items with denormalized product and order info.",
        "model_name": "fct_order_items",
        "expected_columns": ["order_item_id", "order_id", "product_id", "product_name", "quantity", "unit_price", "total_price", "order_date"],
        "expected_row_count": 200,
        "unique_key": "order_item_id"
    },
    {
        "prompt": "Create a dbt fact model for daily revenue aggregated from orders and payments.",
        "model_name": "fct_daily_revenue",
        "expected_columns": ["revenue_date", "total_orders", "total_revenue", "total_payments", "avg_order_value"],
        "expected_row_count": None,
        "unique_key": "revenue_date"
    },
    {
        "prompt": "Create a dbt fact model for support tickets with customer and employee dimensions.",
        "model_name": "fct_support_tickets",
        "expected_columns": ["ticket_id", "customer_id", "customer_name", "employee_id", "employee_name", "category", "priority", "status", "created_at"],
        "expected_row_count": 40,
        "unique_key": "ticket_id"
    },
    {
        "prompt": "Create a dbt dimension model for dates covering the full order date range.",
        "model_name": "dim_dates",
        "expected_columns": ["date_key", "full_date", "year", "quarter", "month", "day_of_week", "is_weekend"],
        "expected_row_count": None,
        "unique_key": "date_key"
    },
    {
        "prompt": "Create a dbt fact model for sessions with enriched customer and channel data.",
        "model_name": "fct_sessions",
        "expected_columns": ["session_id", "customer_id", "customer_name", "session_start", "session_end", "device", "channel", "page_view_count"],
        "expected_row_count": 200,
        "unique_key": "session_id"
    },
    {
        "prompt": "Create a dbt fact model for shipping events with carrier and warehouse context.",
        "model_name": "fct_shipments",
        "expected_columns": ["shipment_id", "order_id", "carrier", "warehouse_name", "shipped_date", "delivered_date", "shipping_cost"],
        "expected_row_count": 75,
        "unique_key": "shipment_id"
    },
    {
        "prompt": "Create a dbt dimension model for employees including department and tenure.",
        "model_name": "dim_employees",
        "expected_columns": ["employee_key", "employee_id", "employee_name", "role", "department", "hire_date"],
        "expected_row_count": 15,
        "unique_key": "employee_id"
    },
    {
        "prompt": "Create a dbt fact model for email events with campaign and customer context.",
        "model_name": "fct_email_events",
        "expected_columns": ["event_id", "campaign_name", "customer_id", "event_type", "event_timestamp"],
        "expected_row_count": 100,
        "unique_key": "event_id"
    },
    {
        "prompt": "Create a dbt fact model for inventory snapshots showing current stock levels with product details.",
        "model_name": "fct_inventory",
        "expected_columns": ["inventory_id", "product_name", "warehouse_name", "quantity_on_hand", "reorder_point"],
        "expected_row_count": 50,
        "unique_key": "inventory_id"
    },
    {
        "prompt": "Create a dbt fact model for returns with order and product context.",
        "model_name": "fct_returns",
        "expected_columns": ["return_id", "order_id", "product_name", "return_reason", "condition", "refund_amount"],
        "expected_row_count": 30,
        "unique_key": "return_id"
    },
    {
        "prompt": "Create a dbt fact model for payments with order and customer context.",
        "model_name": "fct_payments",
        "expected_columns": ["payment_id", "order_id", "customer_id", "payment_method", "amount", "payment_date"],
        "expected_row_count": None,
        "unique_key": "payment_id"
    },
    {
        "prompt": "Create a dbt dimension model for warehouses with capacity and location.",
        "model_name": "dim_warehouses",
        "expected_columns": ["warehouse_key", "warehouse_id", "warehouse_name", "city", "state", "capacity"],
        "expected_row_count": 4,
        "unique_key": "warehouse_id"
    },
    {
        "prompt": "Create a dbt mart model showing weekly business KPIs: revenue, orders, AOV, new customers, churn.",
        "model_name": "mart_weekly_kpis",
        "expected_columns": ["week_start", "total_revenue", "total_orders", "avg_order_value", "new_customers"],
        "expected_row_count": None,
        "unique_key": "week_start"
    },

    # === More UNIONs (fusion-divergent) (10) ===
    {
        "prompt": "Create a dbt model that unions raw_payments and raw_refunds into a single financial transactions table.",
        "model_name": "all_financial_transactions",
        "expected_columns": ["transaction_id", "order_id", "amount", "transaction_type", "transaction_date"],
        "expected_row_count": None,
        "unique_key": "transaction_id"
    },
    {
        "prompt": "Create a dbt model that unions customer emails from raw_customers and raw_email_events into a contact log.",
        "model_name": "all_customer_contacts",
        "expected_columns": ["customer_id", "contact_type", "contact_detail", "contact_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions raw_orders and raw_subscriptions into an all_revenue_sources table.",
        "model_name": "all_revenue_sources",
        "expected_columns": ["revenue_id", "customer_id", "revenue_type", "amount", "revenue_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions raw_support_tickets and raw_reviews into a customer_feedback table.",
        "model_name": "all_customer_feedback",
        "expected_columns": ["feedback_id", "customer_id", "feedback_type", "content", "created_at"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions raw_shipping and raw_returns into all_logistics_events.",
        "model_name": "all_logistics_events",
        "expected_columns": ["event_id", "order_id", "event_type", "event_date", "status"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions page_views and email_events into a unified engagement timeline.",
        "model_name": "all_engagement_events",
        "expected_columns": ["event_id", "customer_id", "event_type", "event_source", "event_timestamp"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions orders, subscriptions, and refunds into a complete customer transaction history.",
        "model_name": "complete_transaction_history",
        "expected_columns": ["transaction_id", "customer_id", "transaction_type", "amount", "transaction_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model unioning raw_sessions and raw_page_views into a unified web activity log.",
        "model_name": "all_web_activity",
        "expected_columns": ["activity_id", "customer_id", "activity_type", "activity_timestamp", "detail"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model that unions inventory changes and shipments into a warehouse activity log.",
        "model_name": "warehouse_activity_log",
        "expected_columns": ["activity_id", "warehouse_id", "activity_type", "product_id", "quantity", "activity_date"],
        "expected_row_count": None,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model unioning new customer signups and subscription starts into a customer_milestones table.",
        "model_name": "customer_milestones",
        "expected_columns": ["milestone_id", "customer_id", "milestone_type", "milestone_date"],
        "expected_row_count": None,
        "unique_key": None
    },

    # === NULL/defensive patterns (10) ===
    {
        "prompt": "Create a dbt model showing orders with COALESCE to handle NULL shipping dates, defaulting to 'not_shipped'.",
        "model_name": "orders_with_shipping_status",
        "expected_columns": ["order_id", "customer_id", "order_date", "shipping_status", "shipped_date"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model calculating order profit margin with safe division (avoiding divide by zero).",
        "model_name": "order_profit_margin_safe",
        "expected_columns": ["order_id", "revenue", "cost", "profit_margin_pct"],
        "expected_row_count": 100,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model using COALESCE to fill NULL review text with 'No review provided'.",
        "model_name": "reviews_with_defaults",
        "expected_columns": ["review_id", "product_id", "rating", "review_text"],
        "expected_row_count": 60,
        "unique_key": "review_id"
    },
    {
        "prompt": "Create a dbt model handling NULL end_date in subscriptions — treat as still active.",
        "model_name": "subscriptions_null_safe",
        "expected_columns": ["subscription_id", "customer_id", "plan", "start_date", "end_date", "is_active", "duration_days"],
        "expected_row_count": 25,
        "unique_key": "subscription_id"
    },
    {
        "prompt": "Create a dbt model using NULLIF to handle zero quantities in inventory to avoid division errors.",
        "model_name": "inventory_safe_division",
        "expected_columns": ["product_id", "quantity_on_hand", "reorder_point", "stock_to_reorder_ratio"],
        "expected_row_count": 50,
        "unique_key": None
    },
    {
        "prompt": "Create a dbt model with COALESCE chain for customer contact: email, then phone, then 'no_contact'.",
        "model_name": "customer_contact_fallback",
        "expected_columns": ["customer_id", "first_name", "primary_contact"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model filtering out rows where critical fields (customer_id, order_date) are NULL.",
        "model_name": "clean_orders",
        "expected_columns": ["order_id", "customer_id", "order_date", "status"],
        "expected_row_count": None,
        "unique_key": "order_id"
    },
    {
        "prompt": "Create a dbt model replacing NULL shipping costs with the average shipping cost.",
        "model_name": "shipping_cost_imputed",
        "expected_columns": ["shipment_id", "order_id", "shipping_cost", "is_imputed"],
        "expected_row_count": 75,
        "unique_key": "shipment_id"
    },
    {
        "prompt": "Create a dbt model using CASE WHEN with IS NULL checks to categorize data completeness per customer.",
        "model_name": "customer_data_completeness",
        "expected_columns": ["customer_id", "has_email", "has_address", "has_orders", "completeness_score"],
        "expected_row_count": 50,
        "unique_key": "customer_id"
    },
    {
        "prompt": "Create a dbt model handling NULL campaign budget by defaulting to 0 and calculating cost per email.",
        "model_name": "campaign_cost_per_email",
        "expected_columns": ["campaign_id", "campaign_name", "budget", "emails_sent", "cost_per_email"],
        "expected_row_count": 12,
        "unique_key": "campaign_id"
    },
]

# Merge with existing
existing_names = {p['model_name'] for p in RL_PROMPTS}
new_prompts = [p for p in BATCH3 if p['model_name'] not in existing_names]
all_prompts = RL_PROMPTS + new_prompts

# Write merged output
output_path = os.path.join(os.path.dirname(__file__), '..', 'rl_sandbox', 'prompts.py')
with open(output_path, 'w') as f:
    f.write('"""\n')
    f.write(f'Training prompts for dbt-coder RL. {len(all_prompts)} prompts across 15 categories.\n')
    f.write('Auto-generated by scripts/generate_prompts.py + generate_prompts_extra.py + generate_prompts_batch3.py\n')
    f.write('"""\n\n')
    f.write('RL_PROMPTS = [\n')
    for p in all_prompts:
        f.write('    {\n')
        f.write(f'        "prompt": {repr(p["prompt"])},\n')
        f.write(f'        "model_name": {repr(p["model_name"])},\n')
        f.write(f'        "expected_columns": {repr(p["expected_columns"])},\n')
        f.write(f'        "expected_row_count": {repr(p.get("expected_row_count"))},\n')
        f.write(f'        "unique_key": {repr(p.get("unique_key"))},\n')
        f.write('    },\n')
    f.write(']\n')

print(f"Total: {len(all_prompts)} prompts ({len(RL_PROMPTS)} existing + {len(new_prompts)} new)")
