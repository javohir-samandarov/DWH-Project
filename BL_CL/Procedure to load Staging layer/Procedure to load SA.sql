--First source schema: 
CREATE SCHEMA IF NOT EXISTS sa_first_dataset;
CREATE SCHEMA IF NOT EXISTS BL_CL;

CREATE EXTENSION IF NOT EXISTS file_fdw;
--Create SERVER 
CREATE SERVER IF NOT EXISTS srv_external FOREIGN DATA WRAPPER file_fdw;

SET search_path TO sa_first_dataset;


--Create source table if not exists
DROP TABLE IF EXISTS src_offline_sales;
CREATE TABLE src_offline_sales(
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

CREATE OR REPLACE PROCEDURE BL_CL.create_ext_offline_sales_and_copy_data(csv_path varchar, source_table varchar)
AS $$
BEGIN 
	--creating foreign table
	EXECUTE 'DROP FOREIGN TABLE IF EXISTS ext_offline_sales';
	EXECUTE format('CREATE FOREIGN TABLE ext_offline_sales(
		invoice_id varchar(4000),
		product_id varchar(4000),
		category varchar(4000),
		product_name varchar(4000),
		quantity int,
		event_dt timestamp,
		unit_price numeric(10,	2),
		customer_id bigint,
		country	varchar(4000),
		store_id bigint,
		employee_id bigint,
		customer_first_name varchar(4000),
		customer_last_name varchar(4000),
		customer_address varchar(4000),
		employee_first_name varchar(4000),
		employee_last_name varchar(4000),
		employee_address varchar(4000),
		promotion_id bigint,
		promo_type varchar(4000),
		promo_start_dt Date,
		promo_end_dt Date,
		percentage numeric(5,2),
		promo_description varchar(4000),
		store_address varchar(4000)
)
	SERVER srv_external
    OPTIONS (
        filename %L,
        format ''csv'',
        header ''true''
    );', csv_path);
   
   --inserting data
   EXECUTE format('INSERT INTO %I 
				   SELECT * FROM ext_offline_sales', source_table);

END;
$$ LANGUAGE plpgsql;

-- Execute the PostgreSQL procedure to copy data from ext_sales to src_sales
CALL BL_CL.create_ext_offline_sales_and_copy_data('D:\Test data\Incremental Load\5 percent data (offline_sales)\5 percent data (offline_sales).csv', 'src_offline_sales');
--D:\Test data\Incremental Load\5 percent data (offline_sales)\5 percent data (offline_sales).csv
--D:\Test data\Initial Load\95 percent data (offline_sales)\95 percent data (offline_sales).csv

--Commit DML changes
COMMIT; 



--Create second schema 
CREATE SCHEMA IF NOT EXISTS sa_second_dataset; 

SET search_path TO sa_second_dataset; 

--Removing ext table if exists
DROP TABLE IF EXISTS src_online_sales;

--Create source table if not exists
CREATE TABLE src_online_sales(
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


CREATE OR REPLACE PROCEDURE BL_CL.create_ext_online_sales_and_copy_data(file_path varchar, source_table varchar)
AS $$
BEGIN
	EXECUTE 'DROP FOREIGN TABLE IF EXISTS ext_online_sales';
    EXECUTE FORMAT('CREATE FOREIGN TABLE ext_online_sales (
                transaction_id varchar(4000),
                event_dt Date,
                product_id varchar(4000),
                product_name varchar(4000),
                price numeric(10,2),
                quantity int,
                customer_id bigint,
                rating int,
                customer_first_name	varchar(4000),
                customer_last_name varchar(4000),
                address	varchar(4000),
                country	varchar(4000),
                city varchar(4000)
	)
            SERVER srv_external
                OPTIONS (
        		filename %L,
		        format ''csv'',
		        header ''true''
		    )',	file_path);
		

    -- Inserting data from the foreign table into the destination table (src_online_sales)
	 EXECUTE format('INSERT INTO %I 
				   SELECT * FROM ext_online_sales', source_table);
END;
$$ LANGUAGE plpgsql;

-- Execute the PostgreSQL procedure to create foreign table and copy data
CALL BL_CL.create_ext_online_sales_and_copy_data('D:\Test data\Incremental Load\5 percent data (online_sales)\5 percent data (online_sales).csv', 'src_online_sales');
--D:\Test data\Incremental Load\5 percent data (online_sales)\5 percent data (online_sales).csv
--D:\Test data\Initial Load\95 percent data (online_sales)\95 percent data (online_sales).csv
--Commit insert changes
COMMIT;
