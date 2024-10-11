
-- Create or replace procedure for inserting logs
CREATE OR REPLACE PROCEDURE bl_cl.insert_logs(
    IN in_c_procedure_name VARCHAR(250),
    IN in_v_insert_start TIMESTAMP,
    IN in_v_insert_end TIMESTAMP,
    IN in_v_operation_row_count BIGINT,
    IN in_v_operation_status VARCHAR(250),
    IN in_v_operation_message VARCHAR(4000)
)
LANGUAGE plpgsql
AS $$
DECLARE
    l_v_operation_message VARCHAR(4000) := in_v_operation_message;
BEGIN
    INSERT INTO bl_cl.Logs (
        procedure_name,
        insert_start,
        insert_end,
        operation_row_count,
        operation_status,
        operation_message
    ) VALUES (
        in_c_procedure_name,
        in_v_insert_start,
        in_v_insert_end,
        in_v_operation_row_count,
        in_v_operation_status,
        l_v_operation_message
    );

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_v_operation_message = message_text;
        RAISE NOTICE 'Logging error: %', l_v_operation_message;
END;
$$;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.my_sequence START 1;

CREATE OR REPLACE PROCEDURE insert_ce_countries()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
    -- Create sequence if not exists
    BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.my_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;

    -- Insert new records into CE_Countries
    BEGIN
	    INSERT INTO bl_3nf.CE_Countries (
		country_id, 
		country, 
		source_entity, 
		source_system, 
		insert_date,
	    update_date)
	SELECT DISTINCT ON (country)
		nextval('bl_3nf.my_sequence') AS country_id, 
		COALESCE(unq.country, 'n.a.'),
		COALESCE(unq.source_entity, 'n.a.'),
		COALESCE(unq.source_system, 'n.a.'),
	    COALESCE(current_date, '1900-01-01'),
	    COALESCE(current_date, '1900-01-01')
	FROM (
		SELECT DISTINCT country, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales
		UNION 
		SELECT DISTINCT country, 'src_online_sales', 'sa_second_dataset'
		FROM sa_second_dataset.src_online_sales
		) AS unq
	WHERE NOT EXISTS (
		SELECT 1 FROM bl_3nf.CE_Countries cn 
		WHERE unq.country = cn.country);
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_countries',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;

CREATE OR REPLACE PROCEDURE insert_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.cities_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    INSERT INTO bl_3nf.CE_Cities (
		city_id, 
		country_id, 
		city, 
		source_entity, 
		source_system, 
		insert_date, 
		update_date)
	SELECT DISTINCT ON (city)
		nextval('bl_3nf.cities_sequence') AS city_id, 
		COALESCE(unq.country_id, -1),
		COALESCE(unq.city, 'n.a'),
		COALESCE(unq.source_entity, 'n.a.'),
		COALESCE(unq.source_system, 'n.a.'),
	    COALESCE(current_date, '1900-01-01'),
	    COALESCE(current_date, '1900-01-01')
	FROM (
		SELECT DISTINCT ss.city, cn.country_id, 'src_online_sales' AS source_entity, 'sa_second_dataset' AS source_system
		FROM sa_second_dataset.src_online_sales ss
		LEFT JOIN bl_3nf.ce_countries cn ON cn.country = ss.country
		) AS unq
	WHERE NOT EXISTS (
		SELECT 1 FROM bl_3nf.CE_Cities cn 
		WHERE unq.city = cn.city OR cn.city = 'n.a');
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_cities',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.addresses_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    INSERT INTO bl_3nf.CE_addresses (
		address_id, 
		address, 
		city_id, 
		source_entity, 
		source_system, 
		insert_date, 
		update_date)
	SELECT DISTINCT ON (address)
		nextval('bl_3nf.addresses_sequence') AS address_id, 
		COALESCE(unq.address, 'n.a') address,
		COALESCE(unq.city_id, -1) city_id,
		COALESCE(unq.source_entity, 'n.a.') source_entity,
		COALESCE(unq.source_system, 'n.a.') source_system,
	    COALESCE(current_date, '1900-01-01') insert_date,
	    COALESCE(current_date, '1900-01-01') update_date
	FROM (
		SELECT DISTINCT customer_address AS address, ct.city_id, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales ss
	    LEFT JOIN bl_3nf.ce_countries AS cn ON ss.country = cn.country
	    LEFT JOIN bl_3nf.CE_Cities AS ct ON ct.country_id = cn.country_id
	    UNION
		SELECT DISTINCT employee_address, -1,  'src_offline_sales', 'sa_first_dataset'
		FROM sa_first_dataset.src_offline_sales
		UNION 
		SELECT DISTINCT store_address, -1, 'src_offline_sales', 'sa_first_dataset'
		FROM sa_first_dataset.src_offline_sales
		UNION
		SELECT DISTINCT address, city_id, 'src_online_sales', 'sa_second_dataset'
		FROM sa_second_dataset.src_online_sales s2
		LEFT JOIN bl_3nf.CE_Cities AS ct ON ct.city = s2.city
		) AS unq
	WHERE NOT EXISTS (
		SELECT 1 FROM bl_3nf.CE_addresses cd 
		WHERE unq.address = cd.address OR cd.address = 'n.a');
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_addresses',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.customers_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    MERGE INTO bl_3nf.CE_Customers AS TARGET
USING (
    SELECT DISTINCT ON (customer_src_id)
        nextval('bl_3nf.customers_sequence') AS customer_id,
        COALESCE(unq.customer_first_name, 'n.a.') AS customer_first_name,
        COALESCE(unq.customer_last_name, 'n.a.') AS customer_last_name,      
        COALESCE(unq.rating, -1) AS rating,
        COALESCE(unq.address_id, -1) AS address_id,
        COALESCE(unq.source_entity, 'n.a.') AS source_entity,
        COALESCE(unq.source_system, 'n.a.') AS source_system,
        COALESCE(unq.customer_src_id, '-1') AS customer_src_id,      
        COALESCE(current_date, '1900-01-01') AS insert_date,
        COALESCE(current_date, '1900-01-01') AS update_date
    FROM (
        SELECT DISTINCT s1.customer_id AS customer_src_id, s1.customer_first_name, s1.customer_last_name, -1 AS rating, ad.address_id, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
        FROM sa_first_dataset.src_offline_sales s1
        LEFT JOIN bl_3nf.CE_addresses ad ON ad.address = s1.customer_address
        UNION
        SELECT DISTINCT s2.customer_id AS customer_src_id, s2.customer_first_name, s2.customer_last_name, max(s2.rating), ad.address_id, 'src_online_sales' AS source_entity, 'sa_second_dataset' AS source_system
        FROM sa_second_dataset.src_online_sales s2
        LEFT JOIN bl_3nf.CE_addresses ad ON s2.address = ad.address
        WHERE customer_id IS NOT NULL
        GROUP BY s2.customer_id, s2.customer_first_name, s2.customer_last_name, ad.address_id, source_entity, source_system
    ) AS unq
) AS SOURCE
ON (TARGET.customer_src_id = SOURCE.customer_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.customer_first_name = SOURCE.customer_first_name or
        TARGET.customer_last_name = SOURCE.customer_last_name or
        TARGET.rating = SOURCE.rating or
        TARGET.address_id = SOURCE.address_id
    )
THEN
    UPDATE SET
        customer_first_name = SOURCE.customer_first_name,
        customer_last_name = SOURCE.customer_last_name,
        rating = SOURCE.rating,
        address_id = SOURCE.address_id
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
        customer_id,
        customer_first_name,
        customer_last_name,
        rating,
        address_id,
        source_entity,
        source_system,
        customer_src_id,
        insert_date,
        update_date
    )
    VALUES (
        SOURCE.customer_id,
        SOURCE.customer_first_name,
        SOURCE.customer_last_name,
        SOURCE.rating,
        SOURCE.address_id,
        SOURCE.source_entity,
        SOURCE.source_system,
        SOURCE.customer_src_id,
        SOURCE.insert_date,
        SOURCE.update_date
    );
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_customers',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;

CREATE OR REPLACE PROCEDURE insert_ce_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.categories_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    INSERT INTO bl_3nf.CE_Categories (
		  category_id,
		  category_name,
		  source_entity,
		  source_system,
		  insert_date,
		  update_date)
	SELECT DISTINCT ON (category_name)
		nextval('bl_3nf.categories_sequence') AS category_id,
		COALESCE(unq.category_name, 'n.a'),
		COALESCE(unq.source_entity, 'n.a.'),
		COALESCE(unq.source_system, 'n.a.'),
	    COALESCE(current_date, '1900-01-01'),
	    COALESCE(current_date, '1900-01-01')
	FROM (
		SELECT DISTINCT s1.category AS category_name,  'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales s1
		) AS unq
	WHERE NOT EXISTS (
		SELECT 1 FROM bl_3nf.CE_Categories cat
		WHERE unq.category_name = cat.category_name OR cat.category_name = 'n.a');
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_categories',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_products()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.products_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    MERGE INTO bl_3nf.CE_Products AS TARGET
USING (
    SELECT DISTINCT ON (product_src_id)
		nextval('bl_3nf.products_sequence') AS product_id,
		COALESCE(unq.category_id, -1) AS category_id,
		COALESCE(unq.product_name, 'n.a.') AS product_name,		
		COALESCE(unq.unit_price, -1) AS unit_price,
		COALESCE(unq.source_system, 'n.a.') AS source_system,
		COALESCE(unq.source_entity, 'n.a.') AS source_entity,
		COALESCE(unq.product_src_id, '-1') AS product_src_id,		
	    COALESCE(current_date, '1900-01-01') AS insert_date,
	    COALESCE(current_date, '1900-01-01') AS update_date
FROM (
		SELECT DISTINCT s1.product_id AS product_src_id, c.category_id, max(s1.product_name) AS product_name, max(s1.unit_price) AS unit_price,'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales s1
		LEFT JOIN bl_3nf.ce_categories c ON c.category_name = s1.category
		GROUP BY product_src_id, c.category_id, source_entity, source_system
		UNION
		SELECT DISTINCT s2.product_id, -1, max(s2.product_name), max(s2.price),'src_online_sales' AS source_entity , 'sa_second_dataset' AS source_system
		FROM sa_second_dataset.src_online_sales s2
		GROUP BY s2.product_id, source_entity, source_system
		) AS unq
) AS SOURCE
ON (TARGET.product_src_id = SOURCE.product_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.product_name = SOURCE.product_name or
        TARGET.unit_price = SOURCE.unit_price or
        TARGET.category_id = SOURCE.category_id
    )
THEN
    UPDATE SET
        product_name = SOURCE.product_name,
        unit_price = SOURCE.unit_price,
        category_id = SOURCE.category_id
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
          product_id,
		  category_id,
		  product_name,
		  unit_price,
		  source_entity,
		  source_system,
		  product_src_id,
		  insert_date,
		  update_date
    )
    VALUES (
        SOURCE.product_id,
        SOURCE.category_id,
        SOURCE.product_name,
        SOURCE.unit_price,
        SOURCE.source_entity,
        SOURCE.source_system,
        SOURCE.product_src_id,
        SOURCE.insert_date,
        SOURCE.update_date
    );
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_products',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_promotiontypes()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.promotiontypes_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    INSERT INTO bl_3nf.CE_PromotionTypes (
		  type_id,
		  type_name,
		  source_entity,
		  source_system,
		  insert_date,
		  update_date)
SELECT DISTINCT ON (promo_type)
		nextval('bl_3nf.promotiontypes_sequence') AS type_id,
		COALESCE(unq.promo_type, 'n.a.') AS promo_type,
		COALESCE(unq.source_entity, 'n.a.') AS source_entity,
		COALESCE(unq.source_system, 'n.a.') AS source_system,
	    COALESCE(current_date, '1900-01-01') AS insert_date,
	    COALESCE(current_date, '1900-01-01') AS update_date
FROM ( 
		SELECT DISTINCT s1.promo_type, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales s1 WHERE s1.promo_type IS NOT NULL
		) AS unq
WHERE NOT EXISTS (
		SELECT 1 FROM bl_3nf.CE_PromotionTypes pd 
		WHERE promo_type = pd.type_name);
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_promotiontypes',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_promotions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.promotions_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    MERGE INTO bl_3nf.CE_Promotions AS TARGET
USING (
    SELECT DISTINCT ON (promotion_id)
		nextval('bl_3nf.promotions_sequence') AS promotion_id,
		COALESCE(unq.type_id, -1) AS type_id,
		COALESCE(unq.start_date, '1900-01-01') AS start_date,
		COALESCE(unq.end_date, '9999-12-31') AS end_date,
		COALESCE(unq.percentage, -1) AS percentage,
		COALESCE(unq.promo_description, 'n.a.') AS promo_description,
		COALESCE(unq.source_system, 'n.a.') AS source_system,
		COALESCE(unq.source_entity, 'n.a.') AS source_entity,
		COALESCE(unq.promotion_id, 'n.a.') AS promotion_src_id,
	    COALESCE(current_date, '1900-01-01') AS insert_date,
	    COALESCE(current_date, '1900-01-01') AS update_date
FROM ( 
		SELECT DISTINCT s1.promotion_id, pt.type_id, s1.promo_start_dt AS start_date, s1.promo_end_dt AS end_date, s1.percentage, s1.promo_description, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales s1
		LEFT JOIN bl_3nf.CE_PromotionTypes pt ON s1.promo_type = pt.type_name
		) AS unq
) AS SOURCE
ON (TARGET.promotion_src_id = SOURCE.promotion_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.type_id = SOURCE.type_id or
        TARGET.start_date = SOURCE.start_date or
        TARGET.end_date = SOURCE.end_date or 
        TARGET.percentage = SOURCE.percentage or 
        TARGET.promo_description = SOURCE.promo_description 

    )
THEN
    UPDATE SET
        type_id = SOURCE.type_id,
        start_date = SOURCE.start_date, 
        end_date = SOURCE.end_date, 
        percentage = SOURCE.percentage, 
        promo_description = SOURCE.promo_description 
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
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
    VALUES (
        SOURCE.promotion_id,
        SOURCE.type_id,
        SOURCE.start_date,
        SOURCE.end_date,
        SOURCE.percentage,
        SOURCE.promo_description,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.promotion_src_id,
        SOURCE.insert_date,
        SOURCE.update_date
    );
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_promotions',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.stores_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
	BEGIN
	    MERGE INTO bl_3nf.CE_Stores AS TARGET
USING (
    SELECT DISTINCT ON (store_src_id)
		nextval('bl_3nf.stores_sequence') AS store_id,
		COALESCE(unq.address_id, -1) AS address_id,
		COALESCE(unq.source_system, 'n.a.') AS source_system,
		COALESCE(unq.source_entity, 'n.a.') AS source_entity,
		COALESCE(unq.store_src_id, '-1') AS store_src_id,
	    COALESCE(current_date, '1900-01-01') AS insert_date,
	    COALESCE(current_date, '1900-01-01') AS update_date
FROM ( 
		SELECT DISTINCT s1.store_id AS store_src_id, ad.address_id, 'src_offline_sales' AS source_entity, 'sa_first_dataset' AS source_system
		FROM sa_first_dataset.src_offline_sales s1
		LEFT JOIN bl_3nf.CE_addresses ad ON s1.store_address = ad.address
		) AS unq
) AS SOURCE
ON (TARGET.store_src_id = SOURCE.store_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.address_id = SOURCE.address_id

    )
THEN
    UPDATE SET
        address_id = SOURCE.address_id
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
          store_id,
		  address_id,
		  source_system,
		  source_entity,
		  store_src_id,
		  insert_date,
		  update_date
    )
    VALUES (
        SOURCE.store_id,
        SOURCE.address_id,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.store_src_id,
        SOURCE.insert_date,
        SOURCE.update_date
    );
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_stores',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_ce_employees_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN
	BEGIN
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_3nf.employees_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
    BEGIN
        -- UPDATE statement
        UPDATE bl_3nf.ce_employees_scd AS dim
		SET end_date = current_date, is_active = 'N'
		WHERE dim.is_active = 'Y'
		AND EXISTS (
		    SELECT 1
		    FROM sa_first_dataset.src_offline_sales AS stg
		    WHERE stg.employee_id = dim.employee_src_id
		      AND (stg.employee_first_name <> dim.employee_first_name OR stg.employee_last_name <> dim.employee_last_name)
		  );

-- Insert statement for changed first_name or last_name
		INSERT INTO bl_3nf.ce_employees_scd (
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
		  insert_date)
		SELECT DISTINCT ON (subq.employee_id)
		  nextval('bl_3nf.employees_sequence') AS employee_id,
		  COALESCE(subq.employee_first_name, 'n.a.') AS employee_first_name,
		  COALESCE(subq.employee_last_name, 'n.a.') AS employee_last_name,
		  COALESCE(subq.address_id, -1) AS address_id,
		  COALESCE(subq.source_system, 'n.a.') AS source_system,
		  COALESCE(subq.source_entity, 'n.a.') AS source_entity,
		  COALESCE(subq.employee_id, '-1') AS employee_src_id,
		  current_date AS start_date,
		  '9999-12-31'::date AS end_date,
		  'Y' AS is_active,
		  current_date AS insert_date
		FROM (SELECT DISTINCT stg.employee_id, stg.employee_first_name, stg.employee_last_name, ad.address_id, 'sa_first_dataset' AS source_system, 'src_offline_sales' AS source_entity 
			  FROM sa_first_dataset.src_offline_sales AS stg
			  LEFT JOIN bl_3nf.ce_addresses AS ad ON stg.employee_address = ad.address
			  ) subq
		LEFT JOIN bl_3nf.ce_employees_scd dim1
		ON subq.employee_id = dim1.employee_src_id
		WHERE dim1.employee_src_id IS NULL
		OR (subq.employee_first_name <> dim1.employee_first_name OR subq.employee_last_name <> dim1.employee_last_name)
		AND NOT EXISTS (SELECT 1 FROM bl_3nf.ce_employees_scd dim2
		WHERE dim2.employee_src_id = subq.employee_id AND dim2.employee_first_name = subq.employee_first_name AND dim2.employee_last_name = subq.employee_last_name);
		
		v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
		
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_ce_employees_scd',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;

CREATE OR REPLACE PROCEDURE insert_CE_invoices()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
BEGIN

	BEGIN
	    MERGE INTO bl_3nf.CE_Invoices AS target
USING (SELECT 
		COALESCE(unq.invoice_id, '-1') AS invoice_id,
		COALESCE(unq.store_id, -1) AS store_id,
		COALESCE(unq.product_id, -1) AS product_id,
		COALESCE(unq.customer_id, -1) AS customer_id,
		COALESCE(unq.employee_id, -1) AS employee_id,
		COALESCE(unq.promotion_id, -1) AS promotion_id,
		COALESCE(unq.event_dt, '1900-01-01') AS event_dt,
		COALESCE(unq.quantity, 0) AS quantity,
		coalesce(unq.source_system) AS source_system,
		coalesce(unq.source_entity) AS source_entity,
	    COALESCE(current_date, '1900-01-01') AS insert_date,
	    COALESCE(current_date, '1900-01-01') AS update_date
FROM (
		SELECT s1.invoice_id, s.store_id, pt.product_id, cu.customer_id, emp.employee_id, pm.promotion_id, max(s1.event_dt) AS event_dt, sum(s1.quantity) AS quantity, 'sa_first_dataset' AS source_system, 'src_offline_sales' AS source_entity
		FROM sa_first_dataset.src_offline_sales s1
		LEFT JOIN bl_3nf.ce_stores s ON s1.store_id = s.store_src_id
		LEFT JOIN bl_3nf.ce_products pt ON s1.product_id = pt.product_src_id
		LEFT JOIN bl_3nf.ce_customers cu ON s1.customer_id = cu.customer_src_id
		LEFT JOIN bl_3nf.ce_employees_scd emp ON s1.employee_id = emp.employee_src_id
		LEFT JOIN bl_3nf.ce_promotions pm ON s1.promotion_id = pm.promotion_src_id
		GROUP BY s1.invoice_id, pt.product_id, cu.customer_id, s.store_id, emp.employee_id, pm.promotion_id
		UNION
		SELECT s2.transaction_id, -1, pt.product_id, cu.customer_id, -1, -1, max(s2.event_dt) AS event_dt, sum(s2.quantity) AS quantity, 'sa_second_dataset' AS source_system, 'src_online_sales' AS source_entity
		FROM sa_second_dataset.src_online_sales s2
		LEFT JOIN bl_3nf.ce_products pt ON s2.product_id = pt.product_src_id
		LEFT JOIN bl_3nf.ce_customers cu ON s2.customer_id = cu.customer_src_id	
		GROUP BY s2.transaction_id, pt.product_id, cu.customer_id
		) AS unq
	) AS source
ON (target.invoice_id = source.invoice_id AND 
   target.product_id = SOURCE.product_id AND
   target.event_dt = SOURCE.event_dt)
WHEN MATCHED AND NOT 
	(target.quantity = SOURCE.quantity or 
	 target.store_id = SOURCE.store_id or 
	 target.customer_id = SOURCE.customer_id or 
	 target.employee_id = SOURCE.employee_id or 
	 target.promotion_id = SOURCE.promotion_id)
THEN
  UPDATE SET
     quantity = source.quantity,
	 store_id = SOURCE.store_id, 
	 customer_id = SOURCE.customer_id, 
	 employee_id = SOURCE.employee_id, 
	 promotion_id = SOURCE.promotion_id
WHEN NOT MATCHED THEN
  INSERT (invoice_id, 
  		  store_id, 
  		  product_id, 
  		  customer_id, 
  		  employee_id, 
  		  promotion_id, 
  		  event_dt, 
  		  quantity, 
  		  source_system,
  		  source_entity,
  		  insert_date, 
  		  update_date)
  VALUES (source.invoice_id, 
 		  source.store_id, 
 		  source.product_id, 
 		  source.customer_id, 
 		  source.employee_id, 
 		  source.promotion_id, 
 		  source.event_dt, 
 		  source.quantity,
 		  SOURCE.source_system,
 		  SOURCE.source_entity,
 		  source.insert_date, 
 		  source.update_date);
       
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_3nf.my_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_CE_invoices',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;