CALL silver.load_silver();


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    load_start_time TIMESTAMP;
    load_end_time TIMESTAMP;

    table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
BEGIN

load_start_time := clock_timestamp();

RAISE NOTICE '===========================================';
RAISE NOTICE 'Starting Silver Layer Load';
RAISE NOTICE '===========================================';


--------------------------------------------------
-- crm_cust_info
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for crm_cust_info table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
(
	SELECT 
		cst_id,
		TRIM(cst_key),
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'N/A' 
		END AS cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A' 
		END AS cst_gndr,
		cst_create_date
	FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_
			FROM bronze.crm_cust_info
		)
	WHERE flag_ = 1
);

table_end_time := clock_timestamp();
RAISE NOTICE 'crm_cust_info loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- crm_prd_info
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for crm_prd_info table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info
(
	SELECT 
		prd_id,
		REPLACE(SUBSTRING (TRIM(prd_key),1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 ELSE 'N/A'
		END AS prd_line,
		prd_start_dt,
		LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt
	FROM bronze.crm_prd_info
);

table_end_time := clock_timestamp();
RAISE NOTICE 'crm_prd_info loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- crm_sales_details
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for crm_sales_details table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						sls_order_dt,
						sls_ship_dt,
						sls_due_dt,
						sls_sales,
						sls_quantity,
						sls_price)
(
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LENGTH(TRIM(CAST(sls_order_dt AS VARCHAR))) != 8 THEN NULL 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LENGTH(TRIM(CAST (sls_ship_dt AS VARCHAR))) != 8 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LENGTH(TRIM(CAST(sls_due_dt AS VARCHAR))) != 8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != sls_sales / NULLIF(sls_quantity,0)
					THEN sls_sales / NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details 
);	

table_end_time := clock_timestamp();
RAISE NOTICE 'crm_sales_details loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- erp_cust_az12
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for erp_cust_az12 table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
(	
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > CURRENT_TIMESTAMP THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 ELSE 'N/A'
	END AS gen
FROM bronze.erp_cust_az12 	
);

table_end_time := clock_timestamp();
RAISE NOTICE 'erp_cust_az12 loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- erp_loc_a101
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for erp_loc_a101 table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (cid,cntry)
(
	SELECT 
	REPLACE(cid,'-',''),
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) IN ('UNITED STATES', 'US', 'USA') THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) IN (NULL,'') THEN 'N/A'
		 ELSE (TRIM(cntry))
	END AS cntry
	FROM bronze.erp_loc_a101
);

table_end_time := clock_timestamp();
RAISE NOTICE 'erp_loc_a101 loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- erp_px_cat_g1v2
--------------------------------------------------

RAISE NOTICE 'Loading data from bronze to silver layer for erp_px_cat_g1v2 table';
table_start_time := clock_timestamp();

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
(
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
); 

table_end_time := clock_timestamp();
RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds',
EXTRACT(EPOCH FROM table_end_time - table_start_time);


--------------------------------------------------
-- TOTAL TIME
--------------------------------------------------

load_end_time := clock_timestamp();

RAISE NOTICE '===========================================';
RAISE NOTICE 'Silver Layer Load Completed Successfully';
RAISE NOTICE 'Total Execution Time: % seconds',
EXTRACT(EPOCH FROM load_end_time - load_start_time);
RAISE NOTICE '===========================================';

END;
$$;