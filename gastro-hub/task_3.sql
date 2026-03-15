--Задание 3
WITH manager_changes AS (
    SELECT
        r.name AS restaurant_name,
        COUNT(DISTINCT rm.manager_uuid) AS manager_change_count
    FROM cafe.restaurant_manager_work_dates rm
    JOIN cafe.restaurants r ON rm.restaurant_uuid = r.restaurant_uuid
    GROUP BY r.name
)
SELECT
    restaurant_name AS "Название заведения",
    manager_change_count AS "Сколько раз менялся менеджер"
FROM manager_changes
ORDER BY manager_change_count DESC
LIMIT 3;
