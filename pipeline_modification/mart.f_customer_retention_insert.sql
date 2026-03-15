INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customers_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded)

WITH order_stats AS (
    SELECT
        item_id,
        customer_id,
        EXTRACT(WEEK FROM date_time) as week_number,
        COUNT(DISTINCT uniq_id) as order_count,
        SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) as refund_count,
        SUM(CASE WHEN status != 'refunded' THEN payment_amount ELSE 0 END) as total_revenue,
        SUM(CASE WHEN status = 'refunded' THEN payment_amount ELSE 0 END) as total_refund
    FROM staging.user_order_log
    WHERE EXTRACT(WEEK FROM date_time) = EXTRACT(WEEK FROM '{{ ds }}'::date)
    GROUP BY item_id, customer_id, EXTRACT(WEEK FROM date_time)
),
customer_categories AS (
    SELECT
        item_id,
        week_number,
        COUNT(DISTINCT CASE WHEN order_count = 1 AND refund_count = 0 THEN customer_id END) as new_customers,
        COUNT(DISTINCT CASE WHEN order_count > 1 AND refund_count = 0 THEN customer_id END) as returning_customers,
        COUNT(DISTINCT CASE WHEN refund_count > 0 THEN customer_id END) as refunded_customers,
        SUM(CASE WHEN order_count = 1 AND refund_count = 0 THEN total_revenue ELSE 0 END) as new_revenue,
        SUM(CASE WHEN order_count > 1 AND refund_count = 0 THEN total_revenue ELSE 0 END) as returning_revenue,
        SUM(total_refund) as total_customers_refunded
    FROM order_stats
    GROUP BY item_id, week_number
)
SELECT
    new_customers as new_customers_count,
    returning_customers as returning_customers_count,
    refunded_customers as refunded_customers_count,
    'weekly' as period_name,
    week_number as period_id,
    item_id,
    new_revenue as new_customers_revenue,
    returning_revenue as returning_customers_revenue,
    total_customers_refunded as customers_refunded
FROM customer_categories;
