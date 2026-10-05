/*
=========================================================
Create Database and Schemas
=========================================================
Script purposes:
	This script creates a new databse named 'DWH_AdvenureWorks2025' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the scripts sets up three schemas
	with the database: 'broze', 'silver' and 'gold'

WARNING:
	Running this scripts will drop the entire 'DataWarehouse' database if exists.
	All data in the database will be permanently deleted. Proced with caution and
	ensure yuu have proper backups before running this scripts.
*/
-- Create Database

USE MASTER;
GO

--Drop and recreate the DWH_AdvenureWorks2025 database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH_AdvenureWorks2025')
BEGIN
	ALTER DATABASE DWH_AdvenureWorks2025 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DWH_AdvenureWorks2025;
END
GO

-- Create the DWH_AdvenureWorks2025 Database
CREATE DATABASE DWH_AdvenureWorks2025;

--Use DWH_AdvenureWorks2025
USE DWH_AdvenureWorks2025;

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
