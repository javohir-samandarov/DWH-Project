CREATE SCHEMA IF NOT EXISTS sa_second_dataset;

SET search_path TO sa_second_dataset;

--Removing ext table if exists
DROP TABLE IF EXISTS sa_second_dataset.src_online_sales;

--Create source table if not exists
CREATE TABLE sa_second_dataset.src_online_sales(
	transaction_id varchar(4000),
	event_dt Date,
	product_id varchar(4000),
	product_name varchar(4000),
	price numeric(10,2),
	quantity int,
	customer_id varchar(4000),
	rating int,
	customer_first_name	varchar(4000),
	customer_last_name varchar(4000),
	address	varchar(4000),
	country	varchar(4000),
	city varchar(4000)
);

COMMIT;