--Задание 4
WITH pizza_counts AS (
    SELECT
        r.name AS restaurant_name,
        COUNT(*) AS pizza_count
    FROM cafe.restaurants r,
        jsonb_each_text(r.menu->'Пицца') AS pizza_items
    WHERE r.restaurant_type = 'pizzeria'
    GROUP BY r.name
),
ranked_pizzerias AS (
    SELECT
        restaurant_name,
        pizza_count,
        DENSE_RANK() OVER (ORDER BY pizza_count DESC) AS rank
    FROM pizza_counts
)
SELECT
    restaurant_name AS "Название заведения",
    pizza_count AS "Количество пицц в меню"
FROM ranked_pizzerias
WHERE rank = 1
ORDER BY restaurant_name;
