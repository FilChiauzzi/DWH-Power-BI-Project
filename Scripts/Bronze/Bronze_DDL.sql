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

----------------------------------------------------------
/*
	Bronze Customer
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_customer', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_customer;

CREATE TABLE bronze.erp_customer(
	[CustomerID] [int] NULL,
	[PersonID] [int] NULL,
	[StoreID] [int] NULL,
	[TerritoryID] [int] NULL,
	[AccountNumber]  NVARCHAR(20) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime] NULL,
);

----------------------------------------------------------
/*
	Bronze Store
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_store', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_store;

CREATE TABLE bronze.erp_store(
	[BusinessEntityID] [int] NULL,
	[Name] NVARCHAR(50)  NULL,
	[SalesPersonID] [int] NULL,
	[Demographics]  [xml] NULL,
	[rowguid] [uniqueidentifier]  NULL,
	[ModifiedDate] [datetime] NULL
);

----------------------------------------------------------
/*
	Bronze Sales Territory
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_sales_territory', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_sales_territory;

CREATE TABLE bronze.erp_sales_territory(
	[TerritoryID] [int] NULL,
	[Name] NVARCHAR(20) NULL,
	[CountryRegionCode] [nvarchar](3) NULL,
	[Group] [nvarchar](50) NULL,
	[SalesYTD] [money] NULL,
	[SalesLastYear] [money] NULL,
	[CostYTD] [money] NULL,
	[CostLastYear] [money] NULL,
	[rowguid] [uniqueidentifier] NOT NULL,
	[ModifiedDate] [datetime] NULL
);

----------------------------------------------------------
/*
	Bronze Person
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_person', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_person;

CREATE TABLE bronze.erp_person(
	[BusinessEntityID] [int] NULL,
	[PersonType] [nchar](2) NULL,
	[NameStyle] [nvarchar](50) NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [nvarchar](20) NULL,
	[MiddleName] [nvarchar](20) NULL,
	[LastName] [nvarchar](20) NULL,
	[Suffix] [nvarchar](10) NULL,
	[EmailPromotion] [int] NOT NULL,
	[AdditionalContactInfo] [xml] NULL,
	[Demographics] [xml] NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime] NULL
);

----------------------------------------------------------
/*
	Bronze Country Region
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_country_region', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_country_region;

CREATE TABLE bronze.erp_country_region(
	[CountryRegionCode] [nvarchar](3)  NULL,
	[Name] [nvarchar](20) NULL,
	[ModifiedDate] [datetime] NULL
);