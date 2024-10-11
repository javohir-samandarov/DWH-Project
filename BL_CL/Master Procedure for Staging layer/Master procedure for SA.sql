SET search_path TO bl_cl;

CREATE OR REPLACE PROCEDURE bl_cl.master_insert_procedure_SA()
LANGUAGE plpgsql
AS $$
BEGIN
  CALL bl_cl.create_ext_offline_sales_and_copy_data('D:\Test data\Initial Load\95 percent data (offline_sales)\95 percent data (offline_sales).csv', 'src_offline_sales');
  CALL create_ext_online_sales_and_copy_data('D:\Test data\Initial Load\95 percent data (online_sales)\95 percent data (online_sales).csv', 'sa_second_dataset.src_online_sales');
EXCEPTION
  WHEN OTHERS THEN
    RAISE;
END;
$$;

BEGIN; CALL bl_cl.master_insert_procedure_SA(); COMMIT;