/*
=============================================================
Create Database and Schemas in postgresql 
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    The script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
	The code may not run in other databases.
*/

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

--use the database you just created and then run the scripts below

-- Create Schemas
CREATE SCHEMA bronze;


CREATE SCHEMA silver;


CREATE SCHEMA gold;

