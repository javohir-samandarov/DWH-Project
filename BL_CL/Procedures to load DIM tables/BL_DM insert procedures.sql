SET search_path TO bl_cl; 

CREATE OR REPLACE PROCEDURE insert_dim_customers()
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
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_sequence START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;

    BEGIN
        MERGE INTO bl_dm.dim_customers AS TARGET
USING (
    SELECT DISTINCT ON (customer_src_id)
			nextval('bl_dm.dm_sequence') AS customer_surr_id,
			COALESCE(cc.customer_first_name, 'n.a.') customer_first_name,
			COALESCE(cc.customer_last_name, 'n.a.') AS customer_last_name,
			COALESCE(cc.rating, -1) rating,
			COALESCE(ca.address_id, -1) address_id,
			COALESCE(cc.customer_id, -1) customer_src_id,
			COALESCE(ca.address, 'n.a.') address,
			COALESCE(cc2.city_id, -1) city_id,
			COALESCE(cc2.city, 'n.a.') city,
			COALESCE(cc3.country_id, -1) country_id,
			COALESCE(cc3.country, 'n.a.') country,
			'BL_3NF' AS source_system,
			'CE_Customers' AS source_entity,
			COALESCE(current_date, '1900-01-01') AS insert_dt,
			COALESCE(current_date, '1900-01-01') AS update_dt
		FROM bl_3nf.ce_customers cc 
		LEFT JOIN bl_3nf.ce_addresses ca ON cc.address_id = ca.address_id
		LEFT JOIN bl_3nf.ce_cities cc2 ON cc2.city_id = ca.city_id 
		LEFT JOIN bl_3nf.ce_countries cc3 ON cc3.country_id = cc2.country_id
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
    VALUES (
        SOURCE.customer_surr_id,
        SOURCE.customer_first_name,
        SOURCE.customer_last_name,
        SOURCE.rating,
        SOURCE.address_id,
        SOURCE.customer_src_id,
        SOURCE.address,
        SOURCE.city_id,
        SOURCE.city,
        SOURCE.country_id,
        SOURCE.country,        
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.insert_dt,
        SOURCE.update_dt
    );
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', 	v_insert_end;
        --ALTER SEQUENCE bl_dm.dm_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_dim_customers',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_dim_products()
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
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_products START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
    BEGIN
        MERGE INTO bl_dm.dim_products AS TARGET
USING (
    SELECT DISTINCT ON (product_src_id)
			nextval('bl_dm.dm_products') AS product_surr_id,
			COALESCE(cc.category_id, -1) AS category_id,
			COALESCE(cp.product_id, -1) AS product_src_id,
			COALESCE(cc.category_name, 'n.a.') AS category,
			COALESCE(cp.product_name, 'n.a.') AS product_name,
			COALESCE(cp.unit_price, -1) AS unit_price,
			'BL_3NF' AS source_system,
			'CE_Products' AS source_entity,
			COALESCE(current_date, '1900-01-01') AS insert_dt,
			COALESCE(current_date, '1900-01-01') AS update_dt
		FROM bl_3nf.ce_products cp 
		LEFT JOIN bl_3nf.ce_categories cc ON cc.category_id = cp.category_id
) AS SOURCE
ON (TARGET.product_src_id = SOURCE.product_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.product_name = SOURCE.product_name or
        TARGET.unit_price = SOURCE.unit_price or
        TARGET.category = SOURCE.category or
        TARGET.category_id = SOURCE.category_id
    )
THEN
    UPDATE SET
        product_name = SOURCE.product_name,
        unit_price = SOURCE.unit_price,
        category = SOURCE.category,
        category_id = SOURCE.category_id
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
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
    VALUES (
        SOURCE.product_surr_id,
        SOURCE.category_id,
        SOURCE.product_src_id,
        SOURCE.category,
        SOURCE.product_name,
        SOURCE.unit_price,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.insert_dt,
        SOURCE.update_dt

    );
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', 	v_insert_end;
        --ALTER SEQUENCE bl_dm.dm_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_dim_products',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_dim_promotions()
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
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_promotions START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
    BEGIN
        MERGE INTO bl_dm.dim_promotions AS TARGET
USING (
    SELECT DISTINCT ON (promotion_src_id) 
		nextval('bl_dm.dm_promotions') AS promotion_surr_id,
		COALESCE(cp.type_id, -1) AS type_id,
		COALESCE(cp.type_name, 'n.a.') AS type_name,
		COALESCE(pr.start_date, '1900-01-01') AS start_date,
		COALESCE(pr.end_date, '9999-12-31') AS end_date, 
		COALESCE(pr.percentage, 0) AS percentage,
		COALESCE(pr.promo_description, 'n.a.') AS promo_description,
		COALESCE(pr.promotion_id, '-1') AS promotion_src_id,
		'BL_3NF' AS source_system,
		'CE_Promotions' AS source_entity,
		COALESCE(current_date, '1900-01-01') AS insert_dt,
		COALESCE(current_date, '1900-01-01') AS update_dt
	FROM bl_3nf.ce_promotions pr
	LEFT JOIN bl_3nf.ce_promotiontypes cp ON cp.type_id = pr.type_id 
) AS SOURCE
ON (TARGET.promotion_src_id = SOURCE.promotion_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.promo_description = SOURCE.promo_description or
        TARGET.percentage = SOURCE.percentage or
        TARGET.start_date = SOURCE.start_date or
        TARGET.end_date = SOURCE.end_date OR 
        TARGET.type_name = SOURCE.type_name OR 
        TARGET.type_id = SOURCE.type_id 

    )
THEN
    UPDATE SET
        promo_description = SOURCE.promo_description,
        percentage = SOURCE.percentage,
        start_date = SOURCE.start_date,
        end_date = SOURCE.end_date,
        type_name = SOURCE.type_name,
        type_id = SOURCE.type_id
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
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
    VALUES (
        SOURCE.promotion_surr_id,
        SOURCE.type_id,
        SOURCE.type_name,
        SOURCE.start_date,
        SOURCE.end_date,
        SOURCE.percentage,
        SOURCE.promo_description,
        SOURCE.promotion_src_id,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.insert_dt,
        SOURCE.update_dt

    );
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', 	v_insert_end;
        --ALTER SEQUENCE bl_dm.dm_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_dim_promotions',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_dim_stores()
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
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_stores START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
    BEGIN
        MERGE INTO bl_dm.dim_stores AS TARGET
USING (
    SELECT DISTINCT ON (store_src_id)
		nextval('bl_dm.dm_stores') AS store_surr_id,
		COALESCE(ad.address_id, -1) AS address_id,
		COALESCE(ad.address, 'n.a.') AS address,
		COALESCE(ct.city_id, -1) AS city_id,
		COALESCE(ct.city, 'n.a.') AS city, 
		COALESCE(cnt.country_id, -1) AS country_id,
		COALESCE(cnt.country, 'n.a.') AS country,
		COALESCE(cs.store_id, -1) AS store_src_id,
		'BL_3NF' AS source_system,
		'CE_Stores' AS source_entity,
		COALESCE(current_date, '1900-01-01') AS insert_dt,
		COALESCE(current_date, '1900-01-01') AS update_dt
	FROM bl_3nf.ce_stores cs 
	LEFT JOIN bl_3nf.ce_addresses AS ad ON cs.address_id = ad.address_id
	LEFT JOIN bl_3nf.ce_cities AS ct ON ct.city_id = ad.city_id
	LEFT JOIN bl_3nf.ce_countries cnt ON cnt.country_id = ct.country_id 
) AS SOURCE
ON (TARGET.store_src_id = SOURCE.store_src_id)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.address = SOURCE.address or
        TARGET.address_id = SOURCE.address_id or
        TARGET.city_id = SOURCE.city_id or
        TARGET.city = SOURCE.city OR 
        TARGET.country_id = SOURCE.country_id OR 
        TARGET.country = SOURCE.country 

    )
THEN
    UPDATE SET
        address = SOURCE.address,
        address_id = SOURCE.address_id,
        city_id = SOURCE.city_id,
        city = SOURCE.city,
        country_id = SOURCE.country_id,
        country = SOURCE.country
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
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
    VALUES (
        SOURCE.store_surr_id,
        SOURCE.address_id,
        SOURCE.address,
        SOURCE.city_id,
        SOURCE.city,
        SOURCE.country_id,
        SOURCE.country,
        SOURCE.store_src_id,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.insert_dt,
        SOURCE.update_dt

    );
        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', 	v_insert_end;
        --ALTER SEQUENCE bl_dm.dm_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_dim_stores',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE insert_dim_employees_scd()
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
        EXECUTE 'CREATE SEQUENCE IF NOT EXISTS bl_dm.dm_employees START 1;';
    EXCEPTION
        WHEN duplicate_table THEN
             
            NULL;
    END;
    BEGIN
	    
	    UPDATE bl_dm.DIM_Employees_SCD AS dim
		SET end_date = current_date, is_active = 'N'
		WHERE dim.is_active = 'Y'
		AND EXISTS (
		    SELECT 1
		    FROM bl_3nf.ce_employees_scd AS stg
		    WHERE stg.employee_id = dim.employee_src_id  AND stg.is_active = 'N');

        -- Insert logic
        INSERT INTO bl_dm.DIM_Employees_SCD (
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
          is_active)
        SELECT DISTINCT ON (subq.employee_id)
              NEXTVAL('bl_dm.dm_employees') AS employee_surr_id,
              COALESCE(subq.employee_first_name, 'n.a.') AS employee_first_name,
              COALESCE(subq.employee_last_name, 'n.a.') AS employee_last_name,
              COALESCE(subq.employee_id, '-1') AS employee_src_id,
              COALESCE(subq.address_id, -1) AS address_id,
              COALESCE(subq.address, 'n.a.') AS address,
              COALESCE(subq.city_id, -1) AS city_id,
              COALESCE(subq.city, 'n.a.') AS city,
              COALESCE(subq.country_id, -1) AS country_id,
              COALESCE(subq.country, 'n.a.') AS country,
              COALESCE(subq.source_system, 'n.a.') AS source_system,
              COALESCE(subq.source_entity, 'n.a.') AS source_entity,
              current_date AS insert_dt,
              COALESCE(subq.start_date, '1900-01-01') AS start_dt,
              COALESCE(subq.end_date, '9999-12-31') AS end_date,
              COALESCE(subq.is_active, 'Y') AS is_active
        FROM (SELECT stg.employee_id, stg.employee_first_name, stg.employee_last_name, ad.address_id, ad.address, ct.city_id, ct.city, cnt.country_id, cnt.country, stg.start_date, stg.end_date, stg.is_active, 'BL_3NF' AS source_system, 'CE_Employees_SCD' AS source_entity 
              FROM bl_3nf.ce_employees_scd stg
              LEFT JOIN bl_3nf.ce_addresses ad ON stg.address_id = ad.address_id
              LEFT JOIN bl_3nf.ce_cities ct ON ad.city_id = ct.city_id
              LEFT JOIN bl_3nf.ce_countries cnt ON ct.country_id = cnt.country_id
                  ) subq
        WHERE NOT EXISTS (SELECT 1 FROM bl_dm.DIM_Employees_SCD dim
            WHERE dim.employee_src_id = subq.employee_id AND dim.employee_first_name = subq.employee_first_name AND dim.employee_last_name = subq.employee_last_name);

        v_insert_end := clock_timestamp();
        v_operation_status := 'success';
        GET DIAGNOSTICS v_operation_row_count = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows;', v_operation_row_count;
        RAISE NOTICE 'Ended at %', v_insert_end;
        --ALTER SEQUENCE bl_dm.dm_sequence RESTART WITH 1;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_operation_message = message_text;
            RAISE NOTICE 'Error occurred: %', v_operation_message;
            RAISE EXCEPTION '%', v_operation_message;
    END;

    CALL bl_cl.insert_logs(
        'insert_dim_employees_scd',
        v_insert_start,
        v_insert_end,
        v_operation_row_count,
        v_operation_status,
        v_operation_message
    );
END;
$$;


CREATE OR REPLACE PROCEDURE generate_dim_dates()
LANGUAGE plpgsql
AS $$
DECLARE
    my_current_date DATE := '2022-01-01';
    end_date DATE := '2023-12-31';
    date_key BIGINT;
BEGIN
    -- Delete any existing records for the date range
    DELETE FROM bl_dm.dim_dates WHERE full_date BETWEEN my_current_date AND end_date;

    -- Initialize the date_key
    date_key := to_char(my_current_date, 'YYYYMMDD')::BIGINT;

    -- Loop through each date in the range and insert a new record
    WHILE my_current_date <= end_date LOOP
        INSERT INTO bl_dm.dim_dates (
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
        VALUES (
            date_key,
            my_current_date,
            EXTRACT(YEAR FROM my_current_date),
            EXTRACT(QUARTER FROM my_current_date),
            EXTRACT(MONTH FROM my_current_date),
            EXTRACT(DAY FROM my_current_date),
            EXTRACT(DOW FROM my_current_date),
            CASE WHEN EXTRACT(DOW FROM my_current_date) IN (6, 0) THEN 'T' ELSE 'F' END,
            CASE
                WHEN EXTRACT(MONTH FROM my_current_date) >= 10 THEN EXTRACT(YEAR FROM my_current_date) + 1
                ELSE EXTRACT(YEAR FROM my_current_date)
            END,
            CASE
                WHEN EXTRACT(MONTH FROM my_current_date) BETWEEN 10 AND 12 THEN 1
                WHEN EXTRACT(MONTH FROM my_current_date) BETWEEN 1 AND 3 THEN 2
                WHEN EXTRACT(MONTH FROM my_current_date) BETWEEN 4 AND 6 THEN 3
                WHEN EXTRACT(MONTH FROM my_current_date) BETWEEN 7 AND 9 THEN 4
            END
        );
        -- Increment the current date and update the date_key
        my_current_date := my_current_date + INTERVAL '1 day';
        date_key := to_char(my_current_date, 'YYYYMMDD')::BIGINT;
    END LOOP;
END;
$$;


CREATE OR REPLACE PROCEDURE insert_fct_invoices_dd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_insert_start TIMESTAMP := clock_timestamp();
    v_insert_end TIMESTAMP;
    v_operation_row_count BIGINT;
    v_operation_status VARCHAR(250) := 'n.a.';
    v_operation_message VARCHAR(1000) := 'n.a.';
    v_partition_name TEXT;
BEGIN
    BEGIN
        -- Create default partition
        EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_Invoices_DD_default PARTITION OF bl_dm.FCT_Invoices_DD DEFAULT';

        EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_Invoices_DD_p1 PARTITION OF bl_dm.FCT_Invoices_DD FOR VALUES FROM (''2022-12-01'') TO (''2023-03-01'')';
        EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_Invoices_DD_p2 PARTITION OF bl_dm.FCT_Invoices_DD FOR VALUES FROM (''2023-03-01'') TO (''2023-06-01'')';
        EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_Invoices_DD_p3 PARTITION OF bl_dm.FCT_Invoices_DD FOR VALUES FROM (''2023-06-01'') TO (''2023-09-01'')';
        EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.FCT_Invoices_DD_p4 PARTITION OF bl_dm.FCT_Invoices_DD FOR VALUES FROM (''2023-09-01'') TO (''2024-01-01'')';

        MERGE INTO bl_dm.fct_invoices_dd AS TARGET
USING (
    SELECT 
		COALESCE(ci.invoice_id, 'n.a.') AS invoice_id,
		COALESCE(cs.store_surr_id, -1) AS store_surr_id,
		COALESCE(cp.product_surr_id, -1) AS product_surr_id,
		COALESCE(cu.customer_surr_id, -1) AS customer_surr_id,
		COALESCE(scd.employee_surr_id, -1) AS employee_surr_id, 
		COALESCE(pp.promotion_surr_id, -1) AS promotion_surr_id,
		COALESCE(dd.date_id, -1) AS date_id,
		COALESCE(ci.event_dt, '1900-01-01') AS event_dt,
		COALESCE(ci.quantity, 0) AS quantity,
		COALESCE(ci.quantity * cp.unit_price, -1) AS total_sales_amount,
		'BL_3NF' AS source_system,
		'CE_Invoices' AS source_entity,
		COALESCE(current_date, '1900-01-01') AS insert_date,
		COALESCE(current_date, '1900-01-01') AS update_date
FROM bl_3nf.ce_invoices ci 
		LEFT JOIN bl_dm.dim_stores cs ON cs.store_src_id = ci.store_id
		LEFT JOIN bl_dm.dim_products cp ON cp.product_src_id = ci.product_id
		LEFT JOIN bl_dm.dim_customers cu ON cu.customer_src_id = ci.customer_id
		LEFT JOIN bl_dm.dim_employees_scd scd ON scd.employee_src_id = ci.employee_id
		LEFT JOIN bl_dm.dim_promotions pp ON pp.promotion_src_id = ci.promotion_id
		LEFT JOIN bl_dm.dim_dates dd ON dd.full_date = ci.event_dt
) AS SOURCE
ON (TARGET.invoice_id = SOURCE.invoice_id AND
	target.product_surr_id = SOURCE.product_surr_id AND 
	target.event_dt = SOURCE.event_dt)
-- When records are matched, update the records if there is any change
WHEN MATCHED AND 
    NOT (
        TARGET.store_surr_id = SOURCE.store_surr_id or
        TARGET.customer_surr_id = SOURCE.customer_surr_id or
        TARGET.employee_surr_id = SOURCE.employee_surr_id or
        TARGET.promotion_surr_id = SOURCE.promotion_surr_id OR 
        TARGET.quantity = SOURCE.quantity 

    )
THEN
    UPDATE SET
        store_surr_id = SOURCE.store_surr_id,
        customer_surr_id = SOURCE.customer_surr_id,
        employee_surr_id = SOURCE.employee_surr_id,
        promotion_surr_id = SOURCE.promotion_surr_id,
        quantity = SOURCE.quantity
-- When no records are matched, insert the incoming records from the source table to the target table
WHEN NOT MATCHED
THEN
    INSERT (
        invoice_id,  
	    store_surr_id,
	    product_surr_id,
	    customer_surr_id,
	    employee_surr_id,
	    promotion_surr_id,
	    date_id,
	    event_dt,
	    quantity,
	    total_sales_amount,
	    source_system,
	    source_entity,
	    insert_date, 
	    update_date
    )
    VALUES (
        SOURCE.invoice_id,
        SOURCE.store_surr_id,
        SOURCE.product_surr_id,
        SOURCE.customer_surr_id,
        SOURCE.employee_surr_id,
        SOURCE.promotion_surr_id,
        SOURCE.date_id,
        SOURCE.event_dt,
        SOURCE.quantity,
        SOURCE.total_sales_amount,
        SOURCE.source_system,
        SOURCE.source_entity,
        SOURCE.insert_date,
        SOURCE.update_date

    );
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
    'insert_fct_invoices_dd',
    v_insert_start,
    v_insert_end,
    v_operation_row_count,
    v_operation_status,
    v_operation_message
);
END;
$$;
