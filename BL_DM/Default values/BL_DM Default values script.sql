SET search_path TO BL_DM;

--Inserting default values
INSERT INTO DIM_Customers (
	customer_surr_id,
    customer_first_name,
    customer_last_name,
    rating,
    address_id,
    customer_src_id,
    address,
    city_id,
    city,
    country_id,
    country,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT  -1,
		'n.a.',
		'n.a.',
		-1,
		-1,
		-1,
		'n.a.',
		-1,
		'n.a.',
		-1,
		'n.a.',
		'BL_3NF',
		'CE_Customers',
		'1900-01-01',
		'1900-01-01'
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Customers
    WHERE customer_surr_id = -1
);

INSERT INTO DIM_Products (
	product_surr_id,
    category_id,
    product_src_id,
    category,
    product_name,
    unit_price,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT -1,
		-1,
		-1,
		'n.a.',
		'n.a.',
		 0,
		'BL_3NF',
		'CE_Products',
		'1900-01-01',
		'1900-01-01'
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Products
    WHERE product_surr_id = -1
);
		
INSERT INTO DIM_Employees_SCD (
	employee_surr_id,
    employee_first_name,
    employee_last_name,
    employee_src_id,
    address_id,
    address,
    city_id,
    city,
    country_id,
    country,
    source_system,
    source_entity,
    insert_dt,
    start_dt,
    end_date,
    is_active
)
SELECT -1,
		'n.a.',
		'n.a.',
		-1,
		-1,
		'n.a.',
		-1,
		'n.a.',
		-1,
		'n.a.',
		'BL_3NF',
		'CE_Employees_SCD',
		'1900-01-01',
		'1900-01-01',
		'9999-12-31',
		'Y'
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Employees_SCD
    WHERE employee_surr_id = -1
);
		
INSERT INTO DIM_Promotions (
    promotion_surr_id,
    type_id,
    type_name,
    start_date,
    end_date,
    percentage,
    promo_description,
    promotion_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT  -1,
		-1,
		'n.a.',
		'1900-01-01',
		'9999-12-31',
		-1,
		'n.a.',
		-1,
		'BL_3NF',
		'CE_Promotions',
		'1900-01-01',
		'1900-01-01'
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Promotions
    WHERE promotion_surr_id = -1
);
	
INSERT INTO DIM_Stores (
    store_surr_id,
    address_id,
    address,
    city_id,
    city,
    country_id,
    country,
    store_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT  -1,
		-1,
		'n.a.',
		-1,
		'n.a.',
		-1,
		'n.a.',
		-1,
		'BL_3NF',
		'CE_Stores',
		'1900-01-01',
		'1900-01-01'
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Stores
    WHERE store_surr_id = -1
);

INSERT INTO DIM_Dates (
    date_id,
    full_date,
    year,
    quarter,
    month,
    day,
    day_of_week,
    is_weekend,
    fiscal_year,
    fiscal_quarter
)
SELECT  -1,
		'1900-01-01',
		-1,
		-1,
		-1,
		-1,
		-1,
		'!',
		-1,
		-1
		
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_Dates
    WHERE date_id = -1
);
	
COMMIT;
