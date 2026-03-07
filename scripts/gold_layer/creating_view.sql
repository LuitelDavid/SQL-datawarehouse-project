--creating the dimension customer view.
CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS fname,
	ci.cst_lastname AS lname,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS DOB,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid
;


--creating the dimension product view.
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt,pi.prd_id) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pi.prd_cost AS product_cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON 	pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL;

--creating the fact sales view.
CREATE VIEW gold.fact_sales AS
SELECT 
	S.sls_ord_num AS order_number,
	P.product_key,
	C.customer_key,
	S.sls_order_dt AS order_date,
	S.sls_ship_dt AS ship_date,
	S.sls_due_dt AS due_date,
	S.sls_sales AS sales_amount,
	S.sls_quantity AS quantity,
	S.sls_price AS price
FROM silver.crm_sales_details AS S
LEFT JOIN gold.dim_products AS P
	ON S.sls_prd_key = P.product_number
LEFT JOIN gold.dim_customers AS C
	ON S.sls_cust_id = C.customer_id;