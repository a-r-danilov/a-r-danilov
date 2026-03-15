insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    CASE
        WHEN uol.status = 'refunded' THEN -uol.quantity
        ELSE uol.quantity
    END,
    CASE
        WHEN uol.status = 'refunded' THEN -uol.payment_amount
        ELSE uol.payment_amount
    END,
    uol.status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
