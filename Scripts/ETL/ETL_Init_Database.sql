/*
=========================================================
ETL DDL – Database and Schemas
=========================================================
Purpose:
    Creates the database DWH_AdventureWorks2025 (dropping it
    first if it exists) and the schemas used by the
    warehouse: bronze, silver and gold for the three layers
    of the Medallion Architecture, and etl for the framework
    tables and procedures. The dwh schema (sequences) is
    created by Silver_DDL.sql.

SSIS Usage:
    Not executed by SSIS. Run once at deployment: it is the
    first script to run.

Warning:
    Running this script DROPS the entire database if it
    exists. All data will be permanently deleted. Make sure
    you have a valid backup.
*/

-- Create Database

USE MASTER;
GO

--Drop and recreate the DWH_AdventureWorks2025 database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH_AdventureWorks2025')
BEGIN
	ALTER DATABASE DWH_AdventureWorks2025 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DWH_AdventureWorks2025;
END
GO

-- Create the DWH_AdventureWorks2025 Database
CREATE DATABASE DWH_AdventureWorks2025;

--Use DWH_AdventureWorks2025
USE DWH_AdventureWorks2025;

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

CREATE SCHEMA etl;
GO
