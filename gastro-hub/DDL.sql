CREATE SCHEMA cafe;
CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');
CREATE TABLE cafe.restaurants (
		restaurant_uuid UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
		name varchar,
		restaurant_type cafe.restaurant_type,
		menu jsonb
);
CREATE TABLE cafe.managers (
	manager_uuid UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	name varchar,
	phone varchar,
);
CREATE TABLE cafe.restaurant_manager_work_dates (
	restaurant_uuid uuid REFERENCES cafe.restaurants,
	manager_uuid uuid REFERENCES cafe.managers,
	hired date NOT NULL,
	fired date,
	PRIMARY KEY(restaurant_uuid, manager_uuid)
);
CREATE TABLE cafe.sales (
	date date NOT NULL,
	restaurant_uuid uuid REFERENCES cafe.restaurants,
	avg_check numeric(6,2),
	PRIMARY KEY (date, restaurant_uuid)
);
