
CREATE SCHEMA IF NOT EXISTS BL_DM;

SET search_path TO BL_DM;

-- Creating sequence
CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_products START 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_promotions START 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_stores START 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_employees START 1;


--DDL scripts for BL_DM

DROP TABLE IF EXISTS DIM_Customers, DIM_Products, DIM_Employees_SCD, DIM_Promotions, DIM_Stores, DIM_Dates, FCT_Invoices_DD;

-- Create DIM_Customers table
CREATE TABLE DIM_Customers (
    customer_surr_id BIGINT PRIMARY KEY,
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    rating int,
    address_id bigint,
    customer_src_id BIGINT,
    address VARCHAR(250),
    city_id bigint,
    city VARCHAR(50),
    country_id bigint,
    country VARCHAR(50),
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE
);

-- Create DIM_Products table
CREATE TABLE DIM_Products (
    product_surr_id BIGINT PRIMARY KEY,
    category_id bigint,
    product_src_id BIGINT,
    category VARCHAR(250),
    product_name VARCHAR(250),
    unit_price NUMERIC(10,2),
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE
);

-- Create DIM_Employees_SCD table
CREATE TABLE DIM_Employees_SCD (
    employee_surr_id BIGINT,
    employee_first_name VARCHAR(50),
    employee_last_name VARCHAR(50),
    employee_src_id BIGINT,
    address_id BIGINT,
    address VARCHAR(250),
    city_id BIGINT,
    city VARCHAR(50),
    country_id BIGINT,
    country VARCHAR(50),
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    insert_dt DATE,
    start_dt DATE,
    end_date DATE,
    is_active VARCHAR(2),
    PRIMARY KEY(employee_surr_id, employee_first_name, employee_last_name)
);

-- Create DIM_Promotions table
CREATE TABLE DIM_Promotions (
    promotion_surr_id BIGINT PRIMARY KEY,
    type_id BIGINT,
    type_name VARCHAR(250),
    start_date DATE,
    end_date DATE,
    percentage NUMERIC(5,2),
    promo_description VARCHAR(250),
    promotion_src_id BIGINT,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE
);

-- Create DIM_Stores table
CREATE TABLE DIM_Stores (
    store_surr_id BIGINT PRIMARY KEY,
    address_id bigint,
    address VARCHAR(250),
    city_id bigint,
    city VARCHAR(50),
    country_id bigint,
    country VARCHAR(50),
    store_src_id BIGINT,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE
);

-- Create DIM_Dates table
CREATE TABLE DIM_Dates (
    date_id BIGINT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend VARCHAR(1),
    fiscal_year INT,
    fiscal_quarter INT
);
-- Create FCT_Invoices_DD table
CREATE TABLE FCT_Invoices_DD (
    invoice_id varchar(50),  
    store_surr_id bigint,
    product_surr_id bigint,
    customer_surr_id bigint,
    employee_surr_id bigint,
    promotion_surr_id bigint,
    date_id BIGINT,
    event_dt timestamp,
    quantity INTEGER,
    total_sales_amount FLOAT,
    source_system varchar(50),
    source_entity varchar(50),
    insert_date DATE, 
    update_date DATE )
    PARTITION BY RANGE (event_dt);


--ALTER TABLE DIM_Employees_SCD ADD CONSTRAINT uq_employee_surr_id UNIQUE (employee_surr_id);
-- Create foreign key constraints
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES DIM_Dates (date_id);
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_store_surr_id FOREIGN KEY (store_surr_id) REFERENCES DIM_Stores (store_surr_id);
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_product_surr_id FOREIGN KEY (product_surr_id) REFERENCES DIM_Products (product_surr_id);
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_customer_surr_id FOREIGN KEY (customer_surr_id) REFERENCES DIM_Customers (customer_surr_id);
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_employee_surr_id FOREIGN KEY (employee_surr_id) REFERENCES DIM_Employees_SCD (employee_surr_id);
--ALTER TABLE FCT_Invoices_DD ADD CONSTRAINT fk_promotion_surr_id FOREIGN KEY (promotion_surr_id) REFERENCES DIM_Promotions (promotion_surr_id);

