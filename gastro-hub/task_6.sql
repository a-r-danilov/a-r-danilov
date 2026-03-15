--Задание 6
--Для предотвращения изменений во время выполнения транзакции
--подходит режим блокировки FOR UPDATE
BEGIN;
WITH coffee_shops AS (
    SELECT
        restaurant_uuid,
        menu
    FROM cafe.restaurants
    WHERE restaurant_type = 'coffee_shop'
    FOR UPDATE OF restaurants -- Блокируем только строки кофеен
),
updated_menus AS (
    SELECT
        restaurant_uuid,
        jsonb_set(
            menu,
            '{Кофе,Капучино}',
            to_jsonb((menu->'Кофе'->>'Капучино')::numeric * 1.2)
        ) AS new_menu
    FROM coffee_shops
    WHERE menu->'Кофе' ? 'Капучино'
)
UPDATE cafe.restaurants r
SET menu = u.new_menu
FROM updated_menus u
WHERE r.restaurant_uuid = u.restaurant_uuid;

COMMIT;
