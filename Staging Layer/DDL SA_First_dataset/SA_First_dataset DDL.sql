CREATE SCHEMA IF NOT EXISTS sa_first_dataset;

CREATE EXTENSION IF NOT EXISTS file_fdw;
--Create SERVER
CREATE SERVER IF NOT EXISTS srv_external FOREIGN DATA WRAPPER file_fdw;

SET search_path TO sa_first_dataset;


--Create source table if not exists
DROP TABLE IF EXISTS sa_first_dataset.src_offline_sales;
CREATE TABLE sa_first_dataset.src_offline_sales(
	invoice_id varchar(4000),
	product_id varchar(4000),
	category varchar(4000),
	product_name varchar(4000),
	quantity int,
	event_dt timestamp,
	unit_price numeric(10,2),
	customer_id varchar(4000),
	country	varchar(4000),
	store_id varchar(4000),
	employee_id varchar(4000),
	customer_first_name varchar(4000),
	customer_last_name varchar(4000),
	customer_address varchar(4000),
	employee_first_name varchar(4000),
	employee_last_name varchar(4000),
	employee_address varchar(4000),
	promotion_id varchar(4000),
	promo_type varchar(4000),
	promo_start_dt Date,
	promo_end_dt Date,
	percentage numeric(5,2),
	promo_description varchar(4000),
	store_address varchar(4000)
);

COMMIT;
