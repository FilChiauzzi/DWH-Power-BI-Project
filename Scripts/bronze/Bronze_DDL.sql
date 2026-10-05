/*
=========================================================
Bronze Layer – Table Definitions
=========================================================
Purpose:
    This script creates raw table structures for the Bronze
    layer of the data platform.

Warning:
    Running this script will DROP existing tables and recreate
    them. All data in these tables will be permanently lost.
*/

USE [DWH_AdventureWorks2025];
GO

----------------------------------------------------------
/*
	Bronze Category
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_category', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_category;

CREATE TABLE bronze.erp_category(
    ProductCategoryID INT NULL,
    [Name] NVARCHAR(50) NULL,
    rowguid UNIQUEIDENTIFIER NULL,
    ModifiedDate Datetime NULL

);

----------------------------------------------
/*
	Bronze SubCategory
*/
-------------------------------------------------

IF OBJECT_ID ('bronze.erp_subcategory', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_subcategory;

CREATE TABLE bronze.erp_subcategory(
    ProductSubcategoryID INT NULL,
    ProductCategoryID INT NULL,
    [Name] NVARCHAR(50) NULL,
    rowguid UNIQUEIDENTIFIER NULL,
    ModifiedDate Datetime NULL

);

-----------------------------------------------
/*
	Bronze Product
*/
-----------------------------------------------

IF OBJECT_ID ('bronze.erp_product', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_product;

CREATE TABLE bronze.erp_product(
	ProductID INT NULL,
	[Name] NVARCHAR(50) NULL,
	ProductNumber NVARCHAR(25) NULL,
	MakeFlag BIT NULL,
	FinishedGoodsFlag BIT NULL,
	Color NVARCHAR(15) NULL,
	SafetyStockLevel SMALLINT NULL,
	ReorderPoint SMALLINT NULL,
	StandardCost DECIMAL(19, 4) NULL,
	ListPrice DECIMAL(19, 4) NULL,
	[Size] NVARCHAR(5) NULL,
	SizeUnitMeasureCode NCHAR(3) NULL,
	WeightUnitMeasureCode NCHAR(3) NULL,
	[Weight] DECIMAL(8, 2) NULL,
	DaysToManufacture INT NULL,
	ProductLine NCHAR(2) NULL,
	Class NCHAR(2) NULL,
	Style NCHAR(2) NULL,
	[ProductSubcategoryID] INT NULL,
	ProductModelID INT NULL,
	SellStartDate DATETIME NULL,
	SellEndDate DATETIME NULL,
	DiscontinuedDate datetime NULL,
	rowguid UNIQUEIDENTIFIER NULL,
	ModifiedDate DATETIME NULL
);
