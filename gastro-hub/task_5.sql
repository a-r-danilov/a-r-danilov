--Задание 5
WITH menu_cte AS (
    SELECT
        r.name AS restaurant_name,
        'Пицца' AS dish_type,
        pizza.key AS pizza_name,
        (pizza.value)::integer AS price
    FROM cafe.restaurants r,
        jsonb_each_text(r.menu->'Пицца') AS pizza
    WHERE r.restaurant_type = 'pizzeria'
),
menu_with_rank AS (
    SELECT
        restaurant_name,
        dish_type,
        pizza_name,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY restaurant_name
            ORDER BY price DESC
        ) AS price_rank
    FROM menu_cte
)
SELECT
    restaurant_name AS "Название заведения",
    dish_type AS "Тип блюда",
    pizza_name AS "Название пиццы",
    price AS "Цена"
FROM menu_with_rank
WHERE price_rank = 1
ORDER BY restaurant_name ASC;
