SET search_path TO bl_cl; 

CREATE OR REPLACE PROCEDURE bl_cl.master_insert_procedure_DM()
LANGUAGE plpgsql
AS $$
BEGIN
  CALL insert_dim_customers();
  CALL insert_dim_products();
  CALL insert_dim_promotions();
  CALL insert_dim_stores();
  CALL insert_dim_employees_scd();
  CALL generate_dim_dates();
  CALL insert_fct_invoices_dd();
EXCEPTION
  WHEN OTHERS THEN
    RAISE;
END;
$$;

BEGIN; CALL bl_cl.master_insert_procedure_DM(); COMMIT;