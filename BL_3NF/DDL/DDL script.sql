
CREATE SCHEMA IF NOT EXISTS BL_3NF;

--Creating Sequences
CREATE SEQUENCE IF NOT EXISTS bl_3nf.my_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.cities_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.addresses_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.customers_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.categories_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.products_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.promotiontypes_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.promotions_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.stores_sequence START 1;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.employees_sequence START 1;


SET search_path TO BL_3NF;

--DDL scripts for BL_3NF

DROP TABLE IF EXISTS CE_Invoices, CE_Stores, CE_Employees_SCD, CE_Promotions, CE_PromotionTypes, CE_Products, CE_Categories, CE_Customers, CE_Addresses, CE_Cities, CE_Countries;

-- creating sequence for ID columns

CREATE TABLE CE_Countries (
  country_id BIGINT PRIMARY KEY,
  country VARCHAR(50),
  source_entity VARCHAR(50),
  source_system VARCHAR(50),
  insert_date DATE,
  update_date DATE  
);

CREATE TABLE CE_Cities (
  city_id BIGINT PRIMARY KEY,
  country_id BIGINT,
  city VARCHAR(50),
  source_entity VARCHAR(50),
  source_system VARCHAR(50),
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (country_id) REFERENCES CE_Countries(country_id)
);

CREATE TABLE CE_Addresses (
  address_id BIGINT PRIMARY KEY,
  address VARCHAR(250),
  city_id BIGINT,
  source_entity VARCHAR(50),
  source_system VARCHAR(50),
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (city_id) REFERENCES CE_Cities(city_id)
);

CREATE TABLE CE_Customers (
  customer_id BIGINT PRIMARY KEY,
  customer_first_name VARCHAR(50),
  customer_last_name VARCHAR(50),
  rating int,
  address_id BIGINT,
  source_system VARCHAR(50),
  source_entity VARCHAR(50),
  customer_src_id varchar(50),  
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (address_id) REFERENCES CE_Addresses(address_id)
);

CREATE TABLE CE_Categories (
  category_id BIGINT PRIMARY KEY,
  category_name VARCHAR(250),
  source_entity VARCHAR(50),
  source_system VARCHAR(50),
  insert_date DATE,
  update_date DATE  
);

CREATE TABLE CE_Products (
  product_id BIGINT PRIMARY KEY,
  category_id BIGINT,
  product_name VARCHAR(250),
  unit_price NUMERIC(10,2),
  source_system VARCHAR(50),
  source_entity VARCHAR(50),
  product_src_id VARCHAR(50),
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (category_id) REFERENCES CE_Categories(category_id)
);

CREATE TABLE CE_PromotionTypes (
  type_id BIGINT PRIMARY KEY,
  type_name VARCHAR(250),
  source_entity VARCHAR(50),
  source_system VARCHAR(50),
  insert_date DATE,
  update_date DATE  
);

CREATE TABLE CE_Promotions (
  promotion_id BIGINT PRIMARY KEY,
  type_id BIGINT,
  start_date DATE,
  end_date DATE,
  percentage NUMERIC(5,2),
  promo_description VARCHAR(250),
  source_system VARCHAR(50),
  source_entity VARCHAR(50),
  promotion_src_id varchar(50),
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (type_id) REFERENCES CE_PromotionTypes(type_id)
);

CREATE TABLE CE_Employees_SCD (
  employee_id BIGINT,
  employee_first_name VARCHAR(50),
  employee_last_name VARCHAR(50),
  address_id BIGINT,
  source_system VARCHAR(50),
  source_entity VARCHAR(50),
  employee_src_id varchar(50),
  start_date DATE,
  end_date DATE,
  is_active VARCHAR(2),
  insert_date DATE,
  PRIMARY KEY (employee_id, employee_first_name, employee_last_name),
  FOREIGN KEY (address_id) REFERENCES CE_Addresses(address_id)
);

CREATE TABLE CE_Stores (
  store_id BIGINT PRIMARY KEY,
  address_id BIGINT,
  source_system VARCHAR(50),
  source_entity VARCHAR(50),
  store_src_id varchar(50),
  insert_date DATE,
  update_date DATE,
  FOREIGN KEY (address_id) REFERENCES CE_Addresses(address_id)
);

CREATE UNLOGGED TABLE CE_Invoices (
  invoice_id varchar(50),
  store_id BIGINT,
  product_id BIGINT,
  customer_id BIGINT,
  employee_id BIGINT,
  promotion_id BIGINT,
  event_dt timestamp,
  quantity INTEGER,
  source_system varchar(50),
  source_entity varchar(50),
  insert_date DATE,
  update_date DATE
  --FOREIGN KEY (store_id) REFERENCES CE_Stores(store_id),
  --FOREIGN KEY (product_id) REFERENCES CE_Products(product_id),
  --FOREIGN KEY (customer_id) REFERENCES CE_Customers(customer_id),
  --FOREIGN KEY (employee_id) REFERENCES CE_Employees_SCD(employee_id),
  --FOREIGN KEY (promotion_id) REFERENCES CE_Promotions(promotion_id)
);