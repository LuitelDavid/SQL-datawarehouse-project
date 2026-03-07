# SQL Data Warehouse Project
## Overview
This project implements a **SQL-based data warehouse pipeline** that transforms raw operational data into structured datasets suitable for analytics and reporting.

The warehouse follows a **layered architecture (Bronze → Silver → Gold)** where each layer improves data quality and prepares the data for analytical use.

The goal of this project is to demonstrate core **data engineering concepts**, including data ingestion, transformation, cleaning, and analytical modeling using SQL.

---

## Architecture
1. Source Data
2. Bronze Layer (Raw Data)
3. Silver Layer (Cleaned & Standardized Data)
4. Gold Layer (Analytics Ready Data)


---

## Technologies Used
- SQL
- PostgreSQL
- Data Warehouse Modeling
- ETL / ELT Concepts

---

## Key Concepts Demonstrated
- Layered data warehouse architecture
- SQL-based data transformation
- Data cleaning and normalization
- Analytical data preparation

---

## Example Transformation

```sql
INSERT INTO silver.crm_cust_info
SELECT DISTINCT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname
FROM bronze.crm_cust_info;
```
### Author : David Luitel
