--Задание 2
CREATE MATERIALIZED VIEW cafe.year_to_year_avg_check_changes AS
WITH yearly_avg_checks AS (
    SELECT
        EXTRACT(YEAR FROM s.date) AS year,
        s.restaurant_uuid,
        ROUND(AVG(s.avg_check)::numeric, 2) AS current_year_avg_check
    FROM cafe.sales s
    WHERE EXTRACT(YEAR FROM s.date) != 2023
    GROUP BY EXTRACT(YEAR FROM s.date), s.restaurant_uuid
),
restaurant_data AS (
    SELECT
        y.year,
        r.name AS restaurant_name,
        r.restaurant_type,
        y.current_year_avg_check,
        LAG(y.current_year_avg_check) OVER (
            PARTITION BY r.name
            ORDER BY y.year
        ) AS previous_year_avg_check
    FROM yearly_avg_checks y
    JOIN cafe.restaurants r ON y.restaurant_uuid = r.restaurant_uuid
)
SELECT
    year::integer AS "Год",
    restaurant_name AS "Название заведения",
    restaurant_type AS "Тип заведения",
    current_year_avg_check AS "Средний чек в текущем году",
    previous_year_avg_check AS "Средний чек в предыдущем году",
    ROUND(
        CASE
            WHEN previous_year_avg_check IS NULL OR previous_year_avg_check = 0 THEN NULL
            ELSE ((current_year_avg_check - previous_year_avg_check) / previous_year_avg_check) * 100
        END::numeric,
    2) AS "Изменение среднего чека в %"
FROM restaurant_data
ORDER BY restaurant_name, year;
