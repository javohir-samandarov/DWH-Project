/*1. CREATE LOGIC TO LOAD OBJECTS FROM SOURCE TO 3NF LAYER */

--Creating BL_CL schema 
CREATE SCHEMA IF NOT EXISTS BL_CL; 

--Switching into bl_cl schema 
SET search_path TO BL_CL; 

--•	Grant all required privileges to BL_CL (Cleansing Layer). 

CREATE ROLE BL_CL_USER
CREATE ROLE BL_3NF_USER

GRANT ALL PRIVILEGES ON SCHEMA BL_CL TO BL_CL_USER;
GRANT ALL PRIVILEGES ON SCHEMA BL_3NF TO BL_CL_USER;
GRANT ALL PRIVILEGES ON SCHEMA sa_first_dataset TO BL_CL_USER;
GRANT ALL PRIVILEGES ON SCHEMA sa_second_dataset TO BL_CL_USER;

GRANT ALL PRIVILEGES ON SCHEMA BL_3NF TO BL_3NF_USER;

--	Create functions and procedures on BL_CL (one procedure = one table) to load data in 3NF layer tables.

CREATE SEQUENCE IF NOT EXISTS bl_cl.logs_seq START WITH 1;
--ALTER SEQUENCE bl_cl.logs_seq reSTART WITH 1;


-- Create unlogged table if not exists for logs
CREATE UNLOGGED TABLE IF NOT EXISTS bl_cl.Logs (
    log_id BIGINT DEFAULT nextval('bl_cl.logs_seq'),
    procedure_name VARCHAR(250) DEFAULT 'n.a.',
    insert_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    insert_end TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_row_count BIGINT DEFAULT 0,
    operation_status VARCHAR(250) DEFAULT 'n.a.',
    operation_message VARCHAR(4000)
);
