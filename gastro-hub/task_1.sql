--Задание 1
CREATE OR REPLACE VIEW cafe.top_restaurants_by_type AS
WITH restaurant_avg_checks AS (
    SELECT
        r.restaurant_uuid,
        r.name AS restaurant_name,
        r.restaurant_type,
        ROUND(AVG(s.avg_check)::numeric, 2) AS average_check
    FROM cafe.restaurants r
    JOIN cafe.sales s ON r.restaurant_uuid = s.restaurant_uuid
    GROUP BY r.restaurant_uuid, r.name, r.restaurant_type
),
ranked_restaurants AS (
    SELECT
        restaurant_name,
        restaurant_type,
        average_check,
        ROW_NUMBER() OVER (
            PARTITION BY restaurant_type
            ORDER BY average_check DESC
        ) AS rank
    FROM restaurant_avg_checks
)
SELECT
    restaurant_name AS "Название заведения",
    restaurant_type AS "Тип заведения",
    average_check AS "Средний чек"
FROM ranked_restaurants
WHERE rank <= 3
ORDER BY restaurant_type, average_check DESC;
