--ПОПЫТКА 2

--создаем схему raw_data
CREATE SCHEMA IF NOT EXISTS raw_data;
--создаем таблицу sales для импорта и анализа сырых данных
CREATE TABLE IF NOT EXISTS raw_data.sales(
	id INT PRIMARY KEY,
	auto TEXT,
	gasoline_consumption REAL,
	price REAL,
	date DATE,
    person_name TEXT,  --исправил
	phone TEXT,
	discount INT,
	brand_origin VARCHAR,
);
--импортируем данные из csv-файла
--чтобы импортировать в DBeaver, нужны права суперпользователя базы
--получилось импортировать через терминал через psql
\copy raw_data.sales
FROM '/home/linadmin/Загрузки/cars.csv'
WITH CSV HEADER DELIMITER ',' NULL 'null';

-- Создаем схему car_shop
CREATE SCHEMA IF NOT EXISTS car_shop;

-- Таблица стран
CREATE TABLE IF NOT EXISTS car_shop.country (
    country_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

-- Таблица брендов
CREATE TABLE IF NOT EXISTS car_shop.brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(50) NOT NULL,
    country_id INT REFERENCES car_shop.country
);

-- Таблица цветов
CREATE TABLE IF NOT EXISTS car_shop.colors (
    color_id SERIAL PRIMARY KEY,
    color_name VARCHAR(50) NOT NULL UNIQUE
);

-- Таблица автомобилей
CREATE TABLE IF NOT EXISTS car_shop.cars (
    car_id SERIAL PRIMARY KEY,
    brand_id INT NOT NULL REFERENCES car_shop.brands(brand_id),
    model VARCHAR(50) NOT NULL,
    gasoline_consumption NUMERIC(3, 1),
    CONSTRAINT unique_car UNIQUE (brand_id, model)
);

-- Таблица цветов автомобилей
CREATE TABLE car_shop.car_colors (
    car_id INT NOT NULL REFERENCES car_shop.cars(car_id),
    color_id INT NOT NULL REFERENCES car_shop.colors(color_id),
    PRIMARY KEY (car_id, color_id)
);

-- Таблица титулов
CREATE TABLE IF NOT EXISTS car_shop.titles (
    title_id SERIAL PRIMARY KEY,
    title_name VARCHAR(10) NOT NULL UNIQUE
);

-- Таблица покупателей
CREATE TABLE IF NOT EXISTS car_shop.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    title_id INT REFERENCES car_shop.titles(title_id),
    CONSTRAINT unique_customer UNIQUE (first_name, last_name, phone)
);

-- Таблица продаж
CREATE TABLE IF NOT EXISTS car_shop.sales (
    sale_id SERIAL PRIMARY KEY,
    car_id INT NOT NULL REFERENCES car_shop.cars(car_id),
    customer_id INT NOT NULL REFERENCES car_shop.customers(customer_id),
    price NUMERIC(7, 2) NOT NULL,
    discount INT,
    sale_date DATE NOT NULL
);
--перенос стран
INSERT INTO car_shop.country (name)
SELECT DISTINCT brand_origin
FROM raw_data.sales
WHERE brand_origin IS NOT NULL AND brand_origin <> '';
--перенос брендов
INSERT INTO car_shop.brands (brand_name, country_id)
SELECT DISTINCT
    split_part(auto, ' ', 1),
    c.country_id
FROM raw_data.sales s
JOIN car_shop.country c ON s.brand_origin = c.name
WHERE split_part(auto, ' ', 1) IS NOT NULL;
--перенос цветов
INSERT INTO car_shop.colors (color_name)
SELECT DISTINCT trim(split_part(auto, ',', 2))
FROM raw_data.sales
WHERE trim(split_part(auto, ',', 2)) <> '';
--перенос машин
INSERT INTO car_shop.cars (brand_id, model, gasoline_consumption)
SELECT DISTINCT
    b.brand_id,
    CASE
        WHEN b.brand_name = 'Tesla' THEN
            trim(regexp_replace(split_part(auto, ',', 1), '^Tesla ', ''))
        ELSE
            trim(split_part(split_part(auto, ',', 1), ' ', 2))
    END AS model,
    NULLIF(gasoline_consumption, 0)
FROM raw_data.sales s
JOIN car_shop.brands b ON split_part(s.auto, ' ', 1) = b.brand_name;
--перенос связей машин и цветов
INSERT INTO car_shop.car_colors (car_id, color_id)
SELECT
    c.car_id,
    col.color_id
FROM raw_data.sales s
JOIN car_shop.brands b ON split_part(s.auto, ' ', 1) = b.brand_name
JOIN car_shop.cars c ON
    c.brand_id = b.brand_id AND
    c.model = CASE
                WHEN b.brand_name = 'Tesla' THEN
                    trim(regexp_replace(split_part(s.auto, ',', 1), 'Tesla ', ''))
                ELSE
                    trim(substring(split_part(s.auto, ',', 1) FROM position(' ' IN split_part(s.auto, ',', 1)) + 1))
              END
JOIN car_shop.colors col ON trim(split_part(s.auto, ',', 2)) = col.color_name;
--перенос приписок к именам
INSERT INTO car_shop.titles (title_name)
SELECT DISTINCT
    CASE
        WHEN person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' THEN
            regexp_replace(person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS).*$', '\1')
        WHEN person_name ~ '(Jr\.|II|DVM|DDS)$' THEN
            regexp_replace(person_name, '^.*(Jr\.|II|DVM|DDS)$', '\1')
    END
FROM raw_data.sales
WHERE
    person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' OR
    person_name ~ '(Jr\.|II|DVM|DDS)$';
--перенос покупателей
INSERT INTO car_shop.customers (first_name, last_name, phone, title_id)
WITH parsed_names AS (
    SELECT
        phone,
        -- Извлечение титула из начала
        CASE
            WHEN person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s' THEN
                regexp_replace(person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s.*$', '\1')
            WHEN person_name ~ '\s(Jr\.|II|DVM|DDS)$' THEN
                regexp_replace(person_name, '^.*\s(Jr\.|II|DVM|DDS)$', '\1')
            ELSE NULL
        END AS title,
        -- Извлечение имени
        CASE
            WHEN person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s' THEN
                trim(regexp_replace(person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s+([A-Za-z]+).*$', '\2'))
            ELSE
                trim(regexp_replace(person_name, '^([A-Za-z]+).*$', '\1'))
        END AS first_name,
        -- Извлечение фамилии
        CASE
            WHEN person_name ~ '\s(Jr\.|II|DVM|DDS)$' THEN
                trim(regexp_replace(person_name, '^(.*?)\s+([A-Za-z]+)\s+(Jr\.|II|DVM|DDS)$', '\2'))
            WHEN person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s' THEN
                trim(regexp_replace(person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s+[A-Za-z]+\s+([A-Za-z]+).*$', '\2'))
            ELSE
                trim(regexp_replace(person_name, '^[A-Za-z]+\s+([A-Za-z]+).*$', '\1'))
        END AS last_name
    FROM raw_data.sales
    WHERE phone IS NOT NULL
)
SELECT DISTINCT
    p.first_name,
    p.last_name,
    p.phone,
    t.title_id
FROM parsed_names p
LEFT JOIN car_shop.titles t ON p.title = t.title_name;
--перенос продаж
INSERT INTO car_shop.sales (car_id, customer_id, price, discount, sale_date)
SELECT
    c.car_id,
    cust.customer_id,
    s.price,
    NULLIF(s.discount, 0),
    s.date
FROM raw_data.sales s
-- Соединяем с брендами
JOIN car_shop.brands b ON split_part(s.auto, ' ', 1) = b.brand_name
-- Соединяем с автомобилями (учитываем модель)
JOIN car_shop.cars c ON
    c.brand_id = b.brand_id AND
    c.model = CASE
                WHEN b.brand_name = 'Tesla' THEN
                    trim(regexp_replace(split_part(s.auto, ',', 1), 'Tesla ', ''))
                ELSE
                    trim(substring(split_part(s.auto, ',', 1) FROM position(' ' IN split_part(s.auto, ',', 1)) + 1))
              END
-- Соединяем с цветами
JOIN car_shop.colors col ON trim(split_part(s.auto, ',', 2)) = col.color_name
JOIN car_shop.car_colors cc ON c.car_id = cc.car_id AND col.color_id = cc.color_id
-- Соединяем с покупателями
JOIN car_shop.customers cust ON
    cust.first_name = CASE
                        WHEN s.person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' THEN
                            trim(regexp_replace(s.person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s+([A-Za-z]+).*$', '\2'))
                        ELSE
                            trim(regexp_replace(s.person_name, '^([A-Za-z]+).*$', '\1'))
                      END AND
    cust.last_name = CASE
                        WHEN s.person_name ~ '(Jr\.|II|DVM|DDS)$' THEN
                            trim(regexp_replace(s.person_name, '^(.*)\s[A-Za-z]+\s(Jr\.|II|DVM|DDS)$', '\1'))
                        WHEN s.person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' THEN
                            trim(regexp_replace(s.person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s[A-Za-z]+\s([A-Za-z]+).*$', '\2'))
                        ELSE
                            trim(regexp_replace(s.person_name, '^[A-Za-z]+\s([A-Za-z]+).*$', '\1'))
                      END AND
    cust.phone = s.phone;


--Задание 1
SELECT
    (COUNT(*) FILTER (WHERE gasoline_consumption IS NULL) * 100 /
    COUNT(*)) AS nulls_percentage_gasoline_consumption
FROM car_shop.cars;

--Задание 2
SELECT
    b.brand_name,
    EXTRACT(YEAR FROM s.sale_date) AS year,
    ROUND(AVG(s.price), 2) AS price_avg
FROM
    car_shop.sales s
JOIN
    car_shop.cars c ON s.car_id = c.car_id
JOIN
    car_shop.brands b ON c.brand_id = b.brand_id
GROUP BY
    b.brand_name,
    EXTRACT(YEAR FROM s.sale_date)
ORDER BY
    b.brand_name ASC,
    year ASC;

--Задание 3
SELECT
    EXTRACT(MONTH FROM s.sale_date) AS month,
    2022 AS year,
    ROUND(AVG(s.price), 2) AS price_avg
FROM
    car_shop.sales s
WHERE
    EXTRACT(YEAR FROM s.sale_date) = 2022
GROUP BY
    EXTRACT(MONTH FROM s.sale_date)
ORDER BY
    month ASC;

--Задание 4
SELECT
    c.first_name || ' ' || c.last_name AS person,
    STRING_AGG(b.brand_name || ' ' || car.model, ', ') AS cars
FROM
    car_shop.sales s
JOIN
    car_shop.customers c ON s.customer_id = c.customer_id
JOIN
    car_shop.cars car ON s.car_id = car.car_id
JOIN
    car_shop.brands b ON car.brand_id = b.brand_id
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    c.first_name || ' ' || c.last_name ASC;

--Задание 5
SELECT
    co.name AS brand_origin,
    MAX(s.price / (1 - COALESCE(s.discount, 0) / 100.0)) AS price_max,
    MIN(s.price / (1 - COALESCE(s.discount, 0) / 100.0)) AS price_min
FROM
    car_shop.sales s
JOIN
    car_shop.cars c ON s.car_id = c.car_id
JOIN
    car_shop.brands b ON c.brand_id = b.brand_id
JOIN
	car_shop.country co ON b.country_id = co.country_id
WHERE
    co.name IS NOT NULL
GROUP BY
    co.country_id
ORDER BY
    co.name;

--Задание 6
SELECT
    COUNT(*) AS persons_from_usa_count
FROM
    car_shop.customers
WHERE
    phone LIKE '+1%';

--Попытка 3
--Измененные запросы для создания таблиц
-- Таблица цветов автомобилей
CREATE TABLE car_shop.car_colors (
    cc_id SERIAL PRIMARY KEY,
    car_id INT NOT NULL REFERENCES car_shop.cars(car_id),
    color_id INT NOT NULL REFERENCES car_shop.colors(color_id)
);
--ТАблица продаж
CREATE TABLE IF NOT EXISTS car_shop.sales (
    sale_id SERIAL PRIMARY KEY,
    car_id INT NOT NULL REFERENCES car_shop.car_colors(cc_id),
    customer_id INT NOT NULL REFERENCES car_shop.customers(customer_id),
    price NUMERIC(7, 2) NOT NULL,
    discount INT,
    sale_date DATE NOT NULL
);

--Измененный запрос для переноса данных продаж
INSERT INTO car_shop.sales (car_id, customer_id, price, discount, sale_date)
SELECT
    cc.cc_id,  -- Ссылка на конкретную комбинацию автомобиля и цвета
    cust.customer_id,
    s.price,
    NULLIF(s.discount, 0),
    s.date
FROM raw_data.sales s
JOIN car_shop.brands b ON split_part(s.auto, ' ', 1) = b.brand_name
JOIN car_shop.cars c ON
    c.brand_id = b.brand_id AND
    c.model = CASE
                WHEN b.brand_name = 'Tesla' THEN
                    trim(regexp_replace(split_part(s.auto, ',', 1), 'Tesla ', ''))
                ELSE
                    trim(substring(split_part(s.auto, ',', 1) FROM position(' ' IN split_part(s.auto, ',', 1)) + 1))
              END
JOIN car_shop.colors col ON trim(split_part(s.auto, ',', 2)) = col.color_name
-- Соединение с таблицей car_colors для получения cc_id
JOIN car_shop.car_colors cc ON c.car_id = cc.car_id AND col.color_id = cc.color_id
JOIN car_shop.customers cust ON
    cust.first_name = CASE
                        WHEN s.person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' THEN
                            trim(regexp_replace(s.person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s+([A-Za-z]+).*$', '\2'))
                        ELSE
                            trim(regexp_replace(s.person_name, '^([A-Za-z]+).*$', '\1'))
                      END AND
    cust.last_name = CASE
                        WHEN s.person_name ~ '(Jr\.|II|DVM|DDS)$' THEN
                            trim(regexp_replace(s.person_name, '^(.*?)\s+([A-Za-z]+)\s+(Jr\.|II|DVM|DDS)$', '\2'))
                        WHEN s.person_name ~ '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)' THEN
                            trim(regexp_replace(s.person_name, '^(Mrs\.|Mr\.|Miss|Dr\.|MD|DVM|DDS)\s+[A-Za-z]+\s+([A-Za-z]+).*$', '\2'))
                        ELSE
                            trim(regexp_replace(s.person_name, '^[A-Za-z]+\s+([A-Za-z]+).*$', '\1'))
                      END AND
    cust.phone = s.phone;
