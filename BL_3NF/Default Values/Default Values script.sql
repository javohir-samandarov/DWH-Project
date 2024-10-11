SET search_path TO bl_3nf;

INSERT INTO CE_Countries (
  country_id,
  country,
  source_entity,
  source_system,
  insert_date,
  update_date
)
SELECT 
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL',
    '1900-01-01',
    '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Countries
    WHERE country_id = -1
);

INSERT INTO CE_Cities (
  city_id,
  country_id,
  city,
  source_entity,
  source_system,
  insert_date,
  update_date
)
SELECT 
    -1,
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Cities
    WHERE city_id = -1
);

INSERT INTO CE_Addresses (
  address_id,
  address,
  city_id,
  source_entity,
  source_system,
  insert_date,
  update_date
)
SELECT 
    -1,
    'n.a.',
    -1,
    'MANUAL',
    'MANUAL',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Addresses
    WHERE address_id = -1
);

INSERT INTO CE_Customers (
  customer_id,
  customer_first_name,
  customer_last_name,
  rating,
  address_id,
  source_system,
  source_entity,
  customer_src_id,
  insert_date,
  update_date
)
SELECT 
    -1,
    'n.a.',
    'n.a.',
    -1,
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Customers
    WHERE customer_id = -1
);

INSERT INTO CE_Categories (
  category_id,
  category_name,
  source_entity,
  source_system,
  insert_date,
  update_date
)
SELECT 
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Categories
    WHERE category_id = -1
);

INSERT INTO CE_Products (
  product_id,
  category_id,
  product_name,
  unit_price,
  source_system,
  source_entity,
  product_src_id,
  insert_date,
  update_date
)
SELECT 
    -1,
    -1,
    'n.a.',
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Products
    WHERE product_id = -1
);

INSERT INTO CE_PromotionTypes (
  type_id,
  type_name,
  source_entity,
  source_system,
  insert_date,
  update_date
)
SELECT 
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_PromotionTypes
    WHERE type_id = -1
);

INSERT INTO CE_Promotions (
  promotion_id,
  type_id,
  start_date,
  end_date,
  percentage,
  promo_description,
  source_system,
  source_entity,
  promotion_src_id,
  insert_date,
  update_date
)
SELECT 
    -1,
    -1,
    '1900-01-01',
    '9999-12-31',
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL',
    'n.a.',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Promotions
    WHERE promotion_id = -1
);

INSERT INTO CE_Employees_SCD (
  employee_id,
  employee_first_name,
  employee_last_name,
  address_id,
  source_system,
  source_entity,
  employee_src_id,
  start_date,
  end_date,
  is_active,
  insert_date
)
SELECT 
    -1,
    'n.a.',
    'n.a.',
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    '1900-01-01',
    '9999-12-31',
    'Y',
    '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Employees_SCD
    WHERE employee_id = -1
);

INSERT INTO CE_Stores (
  store_id,
  address_id,
  source_system,
  source_entity,
  store_src_id,
  insert_date,
  update_date
)
SELECT 
    -1,
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.',
    '1900-01-01',
    '9999-12-31'
WHERE NOT EXISTS (
    SELECT 1
    FROM CE_Stores
    WHERE store_id = -1
);
