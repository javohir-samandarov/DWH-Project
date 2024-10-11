SET search_path TO bl_cl;

CREATE OR REPLACE PROCEDURE bl_cl.master_insert_procedure_CE()
LANGUAGE plpgsql
AS $$
BEGIN
  CALL insert_ce_countries();
  CALL insert_ce_cities();
  CALL insert_ce_addresses();
  CALL insert_ce_categories();
  CALL insert_ce_products();
  CALL insert_ce_customers();
  CALL insert_ce_promotiontypes();
  CALL insert_ce_promotions();
  CALL insert_ce_stores();
  CALL insert_ce_employees_scd();
  CALL insert_ce_invoices();
EXCEPTION
  WHEN OTHERS THEN
    RAISE;
END;
$$;

BEGIN; CALL bl_cl.master_insert_procedure_CE(); COMMIT;