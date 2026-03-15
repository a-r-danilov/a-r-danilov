--Вставляем данные из таблицы menu в таблицу
--restaurants
INSERT INTO cafe.restaurants (name, "restaurant_type", menu)
SELECT
	m.cafe_name,
	CASE
		WHEN m.menu::jsonb?'Пицца' THEN 'pizzeria'::cafe.restaurant_type
		WHEN m.menu::jsonb?'Кофе' THEN 'coffee_shop'::cafe.restaurant_type
		WHEN m.menu::jsonb?'Салат' THEN 'restaurant'::cafe.restaurant_type
	ELSE 'bar'::cafe.restaurant_type
	END,
	m.menu
FROM raw_data.menu m;
--Вставляем данные из таблицы sales в таблицу
--managers
INSERT INTO cafe.managers (name, phone)
SELECT DISTINCT
	s.manager,
	s.manager_phone
FROM raw_data.sales s;
--Вставляем данные о найме менеджеров из таблицы
--sales в таблицу restaurant_manager_work_dates.
--Принимаем день приема равный предыдущему дню
--первого заказа. День увольнения - дата последнего
--заказа
INSERT INTO cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, hired, fired)
SELECT
	r.restaurant_uuid,
	m.manager_uuid,
	MIN(s.report_date)::date - INTERVAL '1 Day',
	MAX(s.report_date)::date
FROM raw_data.sales s
JOIN cafe.restaurants r ON r.name = s.cafe_name
JOIN cafe.managers m ON m.name = s.manager
GROUP BY r.restaurant_uuid, m.manager_uuid;
--Вставляем данные о продажах
INSERT INTO cafe.sales (date, restaurant_uuid, avg_check)
SELECT
	s.report_date,
	r.restaurant_uuid,
	s.avg_check
FROM raw_data.sales s
JOIN cafe.restaurants r ON s.cafe_name = r.name;
