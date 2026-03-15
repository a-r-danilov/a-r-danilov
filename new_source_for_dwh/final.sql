-- шаг 1. Перенос данных из внешнего источника
/* создание таблицы tmp_sources с данными из всех источников */
DROP TABLE IF EXISTS tmp_sources;
CREATE TABLE tmp_sources AS
SELECT  T1.craftsman_id,
        T1.craftsman_name,
        T1.craftsman_address,
        T1.craftsman_birthday,
        T1.craftsman_email,
        T1.product_id,
        T1.product_name,
        T1.product_description,
        T1.product_type,
        T1.product_price,
        T1.order_id,
        T1.order_created_date,
        T1.order_completion_date,
        T1.order_status,
        T2.customer_id,
        T2.customer_name,
        T2.customer_address,
        T2.customer_birthday,
        T2.customer_email
FROM external_source.craft_products_orders T1
JOIN external_source.customers T2 ON T1.customer_id = T2.customer_id;

/* обновление существующих записей и добавление новых в dwh.d_craftsmans */
MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_sources) t
ON d.craftsman_name = t.craftsman_name AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET craftsman_address = t.craftsman_address,
craftsman_birthday = t.craftsman_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_products */
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price from tmp_sources) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET product_type= t.product_type, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email from tmp_sources) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address= t.customer_address,
customer_birthday= t.customer_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);

/* создание таблицы tmp_sources_fact */
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TABLE tmp_sources_fact AS
SELECT  dp.product_id,
        dc.craftsman_id,
        dcust.customer_id,
        src.order_created_date,
        src.order_completion_date,
        src.order_status,
        current_timestamp
FROM tmp_sources src
JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name and dc.craftsman_email = src.craftsman_email
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name and dcust.customer_email = src.customer_email
JOIN dwh.d_product dp ON dp.product_name = src.product_name and dp.product_description = src.product_description and dp.product_price = src.product_price;

/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order f
USING tmp_sources_fact t
ON f.product_id = t.product_id AND f.craftsman_id = t.craftsman_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);



-- шаг 2. Создание витрины данных dwh.customer_report_datamart
-- DDL витрины данных по заказчикам
DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id BIGINT NOT NULL, -- идентификатор заказчика
    customer_name VARCHAR NOT NULL, -- Ф.И.О. заказчика
    customer_address VARCHAR NOT NULL, -- адрес заказчика
    customer_birthday DATE NOT NULL, -- дата рождения заказчика
    customer_email VARCHAR NOT NULL, -- электронная почта заказчика
    customer_money NUMERIC(15,2) NOT NULL, -- сумма, которую потратил заказчик
    platform_money NUMERIC(15,2) NOT NULL, -- сумма, которую заработала платформа (10% от customer_money)
    count_order BIGINT NOT NULL, -- количество заказов у заказчика за месяц
    avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа
    median_time_order_completed NUMERIC(10,1), -- медианное время выполнения заказа в днях
    top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров
    top_craftsman_id BIGINT NOT NULL, -- идентификатор самого популярного мастера
    count_order_created BIGINT NOT NULL, -- количество созданных заказов
    count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе
    count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке
    count_order_done BIGINT NOT NULL, -- количество завершённых заказов
    count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов
    report_period VARCHAR NOT NULL, -- отчётный период (год-месяц)
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);

-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm TIMESTAMP NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);
--шаг 3. инкрементальное обновление данных в витрине
WITH dwh_delta AS ( --извлекает измененные данные
    SELECT
        dc.customer_id,
        dc.customer_name,
        dc.customer_address,
        dc.customer_birthday,
        dc.customer_email,
        fo.order_id,
        dp.product_id,
        dp.product_price,
        dp.product_type,
        fo.order_created_date,
        fo.order_completion_date,
        fo.order_status,
        fo.craftsman_id,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        dc.load_dttm AS customer_load_dttm,
        dp.load_dttm AS product_load_dttm,
        fo.load_dttm AS order_load_dttm,
        crd.customer_id AS exist_customer_id
    FROM dwh.f_order fo
    INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id
        AND TO_CHAR(fo.order_created_date, 'yyyy-mm') = crd.report_period
    WHERE fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
          dc.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
          dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)
),
dwh_update_delta AS ( --выборка заказчиков, по которым были изменения
    SELECT DISTINCT customer_id, report_period
    FROM dwh_delta
    WHERE exist_customer_id IS NOT NULL
),
customer_stats AS ( --вычисление метрик заказчиков
    SELECT
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        report_period,
        SUM(product_price) AS customer_money,
        SUM(product_price) * 0.1 AS platform_money,
        COUNT(order_id) AS count_order,
        AVG(product_price) AS avg_price_order,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY
            CASE WHEN order_status = 'done' AND order_completion_date IS NOT NULL
            THEN (order_completion_date::date - order_created_date::date)
            ELSE NULL END) AS median_time_order_completed,
        SUM(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
        SUM(CASE WHEN order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
        SUM(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
        SUM(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
        SUM(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done
    FROM dwh_delta
    GROUP BY customer_id, customer_name, customer_address, customer_birthday, customer_email, report_period
),
product_categories AS (
    SELECT
        customer_id,
        report_period,
        product_type,
        COUNT(*) AS product_count,
        RANK() OVER(PARTITION BY customer_id, report_period ORDER BY COUNT(*) DESC) AS category_rank
    FROM dwh_delta
    GROUP BY customer_id, report_period, product_type
),
top_products AS ( --вместе с CTE product_categories вычисляет топовую категорию товаров
    SELECT
        customer_id,
        report_period,
        product_type
    FROM product_categories
    WHERE category_rank = 1
),
craftsman_orders AS (
    SELECT
        customer_id,
        report_period,
        craftsman_id,
        COUNT(*) AS order_count,
        ROW_NUMBER() OVER(PARTITION BY customer_id, report_period ORDER BY COUNT(*) DESC) AS craftsman_rank
    FROM dwh_delta
    GROUP BY customer_id, report_period, craftsman_id
),
top_craftsman AS ( --вместе с CTE craftsman_orders определяет топовых мастеров
    SELECT
        customer_id,
        report_period,
        craftsman_id
    FROM craftsman_orders
    WHERE craftsman_rank = 1
),
dwh_delta_insert_result AS ( --подготовка записей для вставки
    SELECT
        cs.*,
        tp.product_type AS top_product_category,
        tc.craftsman_id AS top_craftsman_id
    FROM customer_stats cs
    LEFT JOIN top_products tp ON cs.customer_id = tp.customer_id AND cs.report_period = tp.report_period
    LEFT JOIN top_craftsman tc ON cs.customer_id = tc.customer_id AND cs.report_period = tc.report_period
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh_update_delta ud
        WHERE ud.customer_id = cs.customer_id AND ud.report_period = cs.report_period
    )
),
dwh_delta_update_result AS ( --подготовка записей для обновления
    SELECT
        cs.*,
        tp.product_type AS top_product_category,
        tc.craftsman_id AS top_craftsman_id
    FROM customer_stats cs
    LEFT JOIN top_products tp ON cs.customer_id = tp.customer_id AND cs.report_period = tp.report_period
    LEFT JOIN top_craftsman tc ON cs.customer_id = tc.customer_id AND cs.report_period = tc.report_period
    WHERE EXISTS (
        SELECT 1 FROM dwh_update_delta ud
        WHERE ud.customer_id = cs.customer_id AND ud.report_period = cs.report_period
    )
),
insert_delta AS ( --вставка новых данных
    INSERT INTO dwh.customer_report_datamart (
        customer_id, customer_name, customer_address, customer_birthday, customer_email,
        customer_money, platform_money, count_order, avg_price_order, median_time_order_completed,
        top_product_category, top_craftsman_id,
        count_order_created, count_order_in_progress, count_order_delivery,
        count_order_done, count_order_not_done, report_period
    )
    SELECT
        customer_id, customer_name, customer_address, customer_birthday, customer_email,
        customer_money, platform_money, count_order, avg_price_order, median_time_order_completed,
        top_product_category, top_craftsman_id,
        count_order_created, count_order_in_progress, count_order_delivery,
        count_order_done, count_order_not_done, report_period
    FROM dwh_delta_insert_result
    RETURNING 1
),
update_delta AS ( --обновление измененных данных
    UPDATE dwh.customer_report_datamart target
    SET
        customer_name = source.customer_name,
        customer_address = source.customer_address,
        customer_birthday = source.customer_birthday,
        customer_email = source.customer_email,
        customer_money = source.customer_money,
        platform_money = source.platform_money,
        count_order = source.count_order,
        avg_price_order = source.avg_price_order,
        median_time_order_completed = source.median_time_order_completed,
        top_product_category = source.top_product_category,
        top_craftsman_id = source.top_craftsman_id,
        count_order_created = source.count_order_created,
        count_order_in_progress = source.count_order_in_progress,
        count_order_delivery = source.count_order_delivery,
        count_order_done = source.count_order_done,
        count_order_not_done = source.count_order_not_done
    FROM dwh_delta_update_result source
    WHERE target.customer_id = source.customer_id AND target.report_period = source.report_period
    RETURNING 1
),
insert_load_date AS ( --запись даты последнего обновления
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT GREATEST(
        COALESCE(MAX(customer_load_dttm), NOW()),
        COALESCE(MAX(product_load_dttm), NOW()),
        COALESCE(MAX(order_load_dttm), NOW()))
    FROM dwh_delta
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM insert_delta) AS inserted_rows, --возвращает количество добавленных строк
    (SELECT COUNT(*) FROM update_delta) AS updated_rows, --возвращает количество обновленных строк
    (SELECT COUNT(*) FROM insert_load_date) AS load_dates_inserted;

