/*
=========================================================
Silver Layer Tables – Category, Subcategory, Product
=========================================================
Purpose:
    These tables represent the refined and standardized
    entities of the Silver layer in the Medallion Architecture.
    Data loaded here has passed Bronze-level quality checks
    and is ready for business rule transformations.

Notes:
    - All column names follow snake_case naming convention.
    - Surrogate keys (SK) are managed by SSIS during the load.
    - dwh_create_date / dwh_start_date / dwh_end_date track
      lineage and SCD Type 2 history.
*/

---------------------------------------------------------
-- Silver Category
---------------------------------------------------------

IF OBJECT_ID('silver.erp_category', 'U') IS NOT NULL
    DROP TABLE silver.erp_category;

CREATE TABLE silver.erp_category (
    category_sk INT NOT NULL,                 
    category_id INT NULL,                     
    category_name NVARCHAR(50) NULL,          
    dwh_create_date DATETIME DEFAULT GETDATE() 
);

---------------------------------------------------------
-- Silver Subcategory
---------------------------------------------------------

IF OBJECT_ID('silver.erp_subcategory', 'U') IS NOT NULL
    DROP TABLE silver.erp_subcategory;

CREATE TABLE silver.erp_subcategory (
    subcategory_sk INT NOT NULL,              
    subcategory_id INT NULL,                  
    subcategory_name NVARCHAR(50) NULL,       
    category_sk INT NULL,                     
    dwh_create_date DATETIME DEFAULT GETDATE() 
);

---------------------------------------------------------
-- Silver Product
---------------------------------------------------------

IF OBJECT_ID('silver.erp_product', 'U') IS NOT NULL
    DROP TABLE silver.erp_product;

CREATE TABLE silver.erp_product (
    product_sk INT NOT NULL,                  
    product_id INT NULL,                      
    product_name NVARCHAR(50) NULL,           
    product_number NVARCHAR(25) NULL,         
    make_flag BIT NULL,                       
    finished_goods_flag BIT NULL,             
    color NVARCHAR(15) NULL,                  
    safety_stock_level SMALLINT NULL,         
    reorder_point SMALLINT NULL,              
    standard_cost DECIMAL(19,4) NULL,         
    list_price DECIMAL(19,4) NULL,            
    size NCHAR(3) NULL,                       
    size_unit_measure_code NCHAR(3) NULL,     
    weight_unit_measure_code NCHAR(3) NULL,   
    weight DECIMAL(10,2) NULL,                
    days_to_manufacture INT NULL,             
    product_line NCHAR(3) NULL,               
    class NCHAR(3) NULL,                      
    style NCHAR(3) NULL,                      
    sell_start_date DATE NULL,                
    sell_end_date DATE NULL,                  
    row_hash VARBINARY(8000) NULL,            
    dwh_start_date DATETIME NOT NULL,         
    dwh_end_date DATETIME NOT NULL,           
    is_active BIT NOT NULL,                   
    product_subcategory_sk INT NULL           
);

----------------------------------------------------------
/*
	Silver Customer
*/
---------------------------------------------------------
IF OBJECT_ID ('silver.erp_customer', 'U') IS NOT NULL 
	DROP TABLE silver.erp_customer;

CREATE TABLE silver.erp_customer(
	[customer_sk] [int] NOT NULL,
	[customer_id] [int] NULL,
	[person_id] [int] NULL,
	[store_id] [int] NULL,
	[territory_id] [int] NULL,
    [customer_type] NVARCHAR(50) NULL,
    [person_sk] [int] NULL,
	[store_sk] [int] NULL,
	[territory_sk] [int] NULL,
    dwh_start_date DATETIME NULL,         
    dwh_end_date DATETIME NULL,           
    is_active BIT NOT NULL,
    row_hash VARBINARY(8000) NULL
);

----------------------------------------------------------
/*
	Silver Store
*/
---------------------------------------------------------
IF OBJECT_ID ('silver.erp_store', 'U') IS NOT NULL 
	DROP TABLE silver.erp_store;

CREATE TABLE silver.erp_store(
    [store_sk] [int] NOT NULL,
	[store_id] [int] NULL,
	[store_name] NVARCHAR(50)  NULL,
    dwh_create_date DATETIME DEFAULT GETDATE() 
);

----------------------------------------------------------
/*
	Silver Sales Territory
*/
---------------------------------------------------------
IF OBJECT_ID ('silver.erp_sales_territory', 'U') IS NOT NULL 
	DROP TABLE silver.erp_sales_territory;

CREATE TABLE silver.erp_sales_territory(
    [territory_sk] [int] NOT NULL,
	[territory_id] [int] NULL,
	[territory_name] NVARCHAR(20) NULL,
	[country_code] [nvarchar](3) NULL,
	[continent_name] [nvarchar](50) NULL,
    [country_sk] [int] NULL,
    dwh_create_date DATETIME DEFAULT GETDATE() 
);

----------------------------------------------------------
/*
	Silver Person
*/
---------------------------------------------------------
IF OBJECT_ID ('silver.erp_person', 'U') IS NOT NULL 
	DROP TABLE silver.erp_person;

CREATE TABLE silver.erp_person(
    [person_sk] [int] NOT NULL,
	[person_id] [int] NULL,
	[person_name] [nvarchar](80) NULL,
    dwh_create_date DATETIME DEFAULT GETDATE() 
);

----------------------------------------------------------
/*
	Silver Country Region
*/
----------------------------------------------------------
IF OBJECT_ID ('silver.erp_country_region', 'U') IS NOT NULL 
	DROP TABLE silver.erp_country_region;

CREATE TABLE silver.erp_country_region(
    [country_sk]   [int] NOT NULL,
	[country_code] [nvarchar](3)  NULL,
	[country_name] [nvarchar](50) NULL,
	dwh_create_date DATETIME DEFAULT GETDATE() 
);

----------------------------------------------------------
/*
	Silver Sales Person
*/
----------------------------------------------------------
IF OBJECT_ID ('silver.erp_sales_person', 'U') IS NOT NULL 
	DROP TABLE silver.erp_sales_person;

CREATE TABLE silver.erp_sales_person(
    person_sk [int] NOT NULL,
    person_id [int] NULL,
    territory_id [int] NULL,
    sales_quota [money] NULL,
    bonus_quota [money] NULL,
    commission_pct [smallmoney] NULL,
    sales_ytd [money] NULL,
    sales_last_year [money] NULL,
    territory_sk [int] NULL,
    row_hash VARBINARY(8000) NULL,
    dwh_start_date DATETIME DEFAULT GETDATE(),
    dwh_end_date DATETIME NULL,
    is_active [bit] NULL
);

----------------------------------------------------------
/*
	Silver Sales Order Header
*/
----------------------------------------------------------
IF OBJECT_ID ('silver.erp_sales_order_header', 'U') IS NOT NULL 
	DROP TABLE silver.erp_sales_order_header;

CREATE TABLE silver.erp_sales_order_header(
    sales_order_id INT NULL,
    order_date DATETIME NULL,
    due_date DATETIME NULL,
    ship_date DATETIME NULL,
    status TINYINT NULL,
    online_order_flag BIT NULL,
    sales_order_number NVARCHAR(25) NULL,
    purchase_order_number NVARCHAR(25) NULL,
    account_number NVARCHAR(15) NULL,
    customer_id INT NULL,
    sales_person_id INT NULL,
    territory_id INT NULL,
    currency_rate_id INT NULL,
    sub_total MONEY NULL,
    tax_amount MONEY NULL,
    freight MONEY NULL,
    total_due MONEY NULL,
    modified_date DATE NULL,
    customer_sk INT NULL,
    person_sk INT NULL,
    territory_sk INT NULL
   )