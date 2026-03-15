create table if not exists mart.f_customer_retention (
    new_customers_count int,
    returning_customers_count int,
    refunded_customers_count int,
    period_name varchar(10),
    period_id int,
    item_id int,
    new_customers_revenue numeric(10,2),
    returning_customers_revenue numeric(10,2),
    customers_refunded int);
