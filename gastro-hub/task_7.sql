--Задание 7
--Для предотвращения изменения таблицы используем
--режим EXCLUSIVE. Он также разрешает чтение
BEGIN;
LOCK TABLE cafe.managers IN EXCLUSIVE MODE;

ALTER TABLE cafe.managers
ADD COLUMN phone_numbers varchar[];

WITH numbered_managers AS (
    SELECT
        manager_uuid,
        phone,
        ROW_NUMBER() OVER (ORDER BY name) + 99 AS manager_number
    FROM cafe.managers
)
UPDATE cafe.managers m
SET phone_numbers = ARRAY[
    CONCAT('8-800-2500-', nm.manager_number::varchar),
    m.phone
]
FROM numbered_managers nm
WHERE m.manager_uuid = nm.manager_uuid;

ALTER TABLE cafe.managers
DROP COLUMN phone;

COMMIT;
