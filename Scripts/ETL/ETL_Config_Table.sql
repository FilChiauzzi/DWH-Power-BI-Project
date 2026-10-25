/*
=========================================================
ETL DDL – Configuration Table (etl.config)
=========================================================
Purpose:
    Creates and fills etl.config, the metadata table that
    drives the pipeline: one row per object with the source
    system (object_type), load order, load type, change
    detection method, SCD type (0 static lookup, 1 or 2),
    the source, Bronze and Silver table names, the active
    flag, the bookmarks and the Gold object it feeds.

    Two questions, two columns:
    - load_type answers "how much do I load": full or
      incremental. It is read by Bronze AND Silver.
    - cdc_method answers "how do I find the changes" for
      incremental objects. It is read by Bronze only:
        cdc       SQL Server Change Data Capture
                  (bookmark: last_lsn)
        watermark audit column such as ModifiedDate
                  (bookmark: last_watermark_value)
      Full objects have cdc_method NULL.

    Bookmarks:
    - last_watermark_value / last_lsn: Source -> Bronze,
      filled by the Bronze load.
    - last_silver_batch_id: Bronze -> Silver, the last batch
      whose Bronze rows were processed by Silver with
      success. Filled by SSIS after each Load Silver.

    The last row, fact_sales_order, describes the Gold fact.
    It is inactive because the fact is loaded by its own
    task at the end of the pipeline, not by the Bronze and
    Silver loops.

SSIS Usage:
    SSIS reads the active objects, ordered by load_order,
    into the variable object_list. A Foreach Loop iterates
    over the list and passes each object_name to the Bronze
    and Silver load procedures. The load order follows the
    dependencies: parents before children.

Warning:
    Running this script DROPS and recreates etl.config.
    All bookmarks are lost:
    - last_watermark_value / last_lsn NULL: the next Bronze
      load of every object is an initial FULL load, which
      fills them again;
    - last_silver_batch_id NULL: Silver processes every row
      currently in Bronze.
    CDC must already be enabled on the source tables with
    cdc_method = 'cdc' (sys.sp_cdc_enable_table).
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Drop & Create Table
---------------------------------------------------------
IF OBJECT_ID('etl.config', 'U') IS NOT NULL
    DROP TABLE etl.config;
GO

CREATE TABLE etl.config (
    config_key           INT IDENTITY(1,1) PRIMARY KEY,
    object_name          VARCHAR(50)   NOT NULL,   -- entity, lowercase snake_case (no erp_ prefix)
    object_type          VARCHAR(10)   NOT NULL,   -- source system: erp
    load_order           INT           NOT NULL,   -- parents before children
    load_type            VARCHAR(20)   NOT NULL,   -- full / incremental (Bronze and Silver)
    cdc_method           VARCHAR(20)   NULL,       -- incremental only: cdc / watermark (Bronze)
    scd_type             VARCHAR(10)   NOT NULL,   -- 0 / 1 / 2
    source_table         VARCHAR(200)  NOT NULL,   -- table in the source database
    bronze_table         VARCHAR(200)  NOT NULL,
    silver_table         VARCHAR(200)  NOT NULL,
    is_active            BIT           NOT NULL,
    watermark            VARCHAR(20)   NULL,       -- audit column used by cdc_method = watermark
    last_watermark_value DATETIME      NULL,       -- watermark bookmark, filled by the Bronze load
    last_lsn             BINARY(10)    NULL,       -- CDC bookmark, filled by the Bronze load
    last_silver_batch_id BIGINT        NULL,       -- Bronze -> Silver bookmark, filled by SSIS
    target_table         VARCHAR(200)  NULL        -- Gold object fed by the entity
);
GO

---------------------------------------------------------
-- Configuration values
-- The 5 small objects are full; the other 10 have CDC
-- enabled on the source. load_type stays 'full' until the
-- CDC branch of usp_load_bronze is ready.
---------------------------------------------------------
INSERT INTO etl.config (
    object_name, object_type, load_order, load_type, cdc_method, scd_type,
    source_table, bronze_table, silver_table,
    is_active, watermark, last_watermark_value, target_table
)
VALUES
('category', 'erp', 10, 'full', NULL, '1',
 'AdventureWorks2025.Production.ProductCategory',
 'bronze.erp_category',
 'silver.erp_category',
 1, 'ModifiedDate', NULL, 'dim_product'),

('subcategory', 'erp', 20, 'full', NULL, '1',
 'AdventureWorks2025.Production.ProductSubcategory',
 'bronze.erp_subcategory',
 'silver.erp_subcategory',
 1, 'ModifiedDate', NULL, 'dim_product'),

('product', 'erp', 30, 'full', 'cdc', '2',
 'AdventureWorks2025.Production.Product',
 'bronze.erp_product',
 'silver.erp_product',
 1, 'ModifiedDate', NULL, 'dim_product'),

('store', 'erp', 40, 'full', 'cdc', '1',
 'AdventureWorks2025.Sales.Store',
 'bronze.erp_store',
 'silver.erp_store',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('country_region', 'erp', 50, 'full', 'cdc', '0',
 'AdventureWorks2025.Person.CountryRegion',
 'bronze.erp_country_region',
 'silver.erp_country_region',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('person', 'erp', 60, 'full', 'cdc', '1',
 'AdventureWorks2025.Person.Person',
 'bronze.erp_person',
 'silver.erp_person',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('sales_territory', 'erp', 70, 'full', NULL, '1',
 'AdventureWorks2025.Sales.SalesTerritory',
 'bronze.erp_sales_territory',
 'silver.erp_sales_territory',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('customer', 'erp', 80, 'full', 'cdc', '2',
 'AdventureWorks2025.Sales.Customer',
 'bronze.erp_customer',
 'silver.erp_customer',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('employee', 'erp', 90, 'full', 'cdc', '1',
 'AdventureWorks2025.HumanResources.Employee',
 'bronze.erp_employee',
 'silver.erp_employee',
 1, 'ModifiedDate', NULL, 'dim_sales_person'),

('sales_person', 'erp', 100, 'full', NULL, '2',
 'AdventureWorks2025.Sales.SalesPerson',
 'bronze.erp_sales_person',
 'silver.erp_sales_person',
 1, 'ModifiedDate', NULL, 'dim_sales_person'),

('ship_method', 'erp', 110, 'full', NULL, '1',
 'AdventureWorks2025.Purchasing.ShipMethod',
 'bronze.erp_ship_method',
 'silver.erp_ship_method',
 1, 'ModifiedDate', NULL, 'dim_ship_method'),

('state_province', 'erp', 120, 'full', 'cdc', '1',
 'AdventureWorks2025.Person.StateProvince',
 'bronze.erp_state_province',
 'silver.erp_state_province',
 1, 'ModifiedDate', NULL, 'dim_geography'),

('person_address', 'erp', 130, 'full', 'cdc', '1',
 'AdventureWorks2025.Person.Address',
 'bronze.erp_person_address',
 'silver.erp_person_address',
 1, 'ModifiedDate', NULL, 'dim_geography'),

('sales_order_header', 'erp', 140, 'full', 'cdc', '1',
 'AdventureWorks2025.Sales.SalesOrderHeader',
 'bronze.erp_sales_order_header',
 'silver.erp_sales_order_header',
 1, 'ModifiedDate', NULL, 'fact_sales_order'),

('sales_order_detail', 'erp', 150, 'full', 'cdc', '1',
 'AdventureWorks2025.Sales.SalesOrderDetail',
 'bronze.erp_sales_order_detail',
 'silver.erp_sales_order_detail',
 1, 'ModifiedDate', NULL, 'fact_sales_order');
GO
