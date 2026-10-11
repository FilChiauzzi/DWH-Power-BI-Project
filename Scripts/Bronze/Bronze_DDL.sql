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

-----------------------------------------------------------
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
	[FirstName] [nvarchar](50) NULL,
	[MiddleName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NULL,
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
	[Name] [nvarchar](50) NULL,
	[ModifiedDate] [datetime] NULL
);

----------------------------------------------------------
/*
	Bronze Employee
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_employee', 'U') IS NOT NULL
DROP TABLE bronze.erp_employee;

CREATE TABLE bronze.erp_employee(
	[BusinessEntityID] [int] NULL,
	[NationalIDNumber] [nvarchar] NULL,
	[LoginID] [nvarchar](256)  NULL,
	[OrganizationNode] [hierarchyid] NULL,
	[OrganizationLevel] [smallint] NULL,
	[JobTitle] [nvarchar](50) NULL,
	[BirthDate] [date]  NULL,
	[MaritalStatus] [nchar](1)  NULL,
	[Gender] [nchar](1)  NULL,
	[HireDate] [date]  NULL,
	[SalariedFlag] [bit] NULL,
	[VacationHours] [smallint]  NULL,
	[SickLeaveHours] [smallint]  NULL,
	[CurrentFlag] [bit]  NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime] NULL
	)

----------------------------------------------------------
/*
	Bronze Sales Person
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_sales_person', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_sales_person;

CREATE TABLE bronze.erp_sales_person(
	[BusinessEntityID] [int] NULL,
	[TerritoryID] [int] NULL,
	[SalesQuota] [money] NULL,
	[Bonus] [money] NOT NULL,
	[CommissionPct] [smallmoney] NULL,
	[SalesYTD] [money] NULL,
	[SalesLastYear] [money]  NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime] NULL
	)

----------------------------------------------------------
/*
	Bronze Department
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_department', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_department;

CREATE TABLE bronze.erp_department(
	[DepartmentID] [smallint]  NULL,
	[Name] [nvarchar](50) NULL,
	[GroupName] [nvarchar](50) NULL,
	[ModifiedDate] [datetime] NULL
	)

----------------------------------------------------------
/*
	Bronze Department History
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_empployee_department_history', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_empployee_department_history;

CREATE TABLE bronze.erp_empployee_department_history(
	[BusinessEntityID] [int]  NULL,
	[DepartmentID] [smallint]  NULL,
	[ShiftID] [tinyint]  NULL,
	[StartDate] [date]  NULL,
	[EndDate] [date] NULL,
	[ModifiedDate] [datetime]  NULL
	)

----------------------------------------------------------
/*
	Bronze Sales Order Header
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_sales_order_header', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_sales_order_header;

CREATE TABLE bronze.erp_sales_order_header (
	[SalesOrderID] [int] NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderDate] [datetime]  NULL,
	[DueDate] [datetime]  NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint]  NULL,
	[OnlineOrderFlag] [bit]  NULL,
	[SalesOrderNumber]  NVARCHAR(25) NULL,
	[PurchaseOrderNumber] NVARCHAR(25) NULL,
	[AccountNumber] NVARCHAR(15) NULL,
	[CustomerID] [int] NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int]  NULL,
	[ShipToAddressID] [int]  NULL,
	[ShipMethodID] [int] NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[TotalDue] [money] NULL,
	[Comment] [nvarchar](128) NULL,
	[rowguid] [uniqueidentifier] NULL,
	[ModifiedDate] [datetime] NULL
	)

----------------------------------------------------------
/*
	Bronze Sales Order Detail
*/
---------------------------------------------------------
IF OBJECT_ID ('bronze.erp_sales_order_detail', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_sales_order_detail;

CREATE TABLE bronze.erp_sales_order_detail (
	[SalesOrderID] [int]  NULL,
	[SalesOrderDetailID] [int] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[OrderQty] [smallint]  NULL,
	[ProductID] [int]  NULL,
	[SpecialOfferID] [int]  NULL,
	[UnitPrice] [money]  NULL,
	[UnitPriceDiscount] [money]  NULL,
	[LineTotal]  [numeric] (38,6) NULL,
	[rowguid] [uniqueidentifier]  NULL,
	[ModifiedDate] [datetime] NULL
	)