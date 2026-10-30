/*
=========================================================
ETL DDL – Configuration Table (etl.config)
=========================================================
Purpose:
    Creates and fills etl.config, the metadata table that
    drives the pipeline: one row per object with the source
    system (object_type), load order, load type, SCD type
    (0 static lookup, 1 or 2), the source, Bronze and Silver
    table names, the active flag, the bookmarks and the Gold
    object it feeds.

    load_type (read by Bronze AND Silver):
    - full        Bronze copies the whole source every run;
    - incremental Bronze reads only the changes, with SQL
                  Server Change Data Capture (CDC must be
                  enabled on the source table).
    Rule: an SCD2 object (scd_type = '2') must be incremental:
    a full load truncates Silver and loses its history.

    is_cdc_enabled (read by ETL/CDC/Enable_CDC_Source.sql):
    1 = SQL Server CDC is enabled on the source table. An
    incremental object needs is_cdc_enabled = 1: the Bronze
    load stops with an error otherwise.

    Bookmarks:
    - last_lsn: Source -> Bronze, how far the CDC log was
      read. Filled by etl.usp_load_bronze_cdc.
    - last_silver_batch_id: Bronze -> Silver, the last batch
      whose Bronze rows were processed by Silver with
      success. Filled by SSIS after each Load Silver.


SSIS Usage:
    SSIS reads the active objects, ordered by load_order,
    into the variable object_list. A Foreach Loop iterates
    over the list and passes each object_name to the Bronze
    and Silver load procedures. The load order follows the
    dependencies: parents before children.

Warning:
    Running this script DROPS and recreates etl.config.
    All bookmarks are lost:
    - last_lsn NULL: the next Bronze load of every
      incremental object is an initial FULL load, which
      fills it again;
    - last_silver_batch_id NULL: Silver processes every row
      currently in Bronze.
    Then run ETL/CDC/Enable_CDC_Source.sql: it enables CDC on
    the source tables with is_cdc_enabled = 1.
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
    load_type            VARCHAR(20)   NOT NULL,   -- full / incremental (CDC)
    is_cdc_enabled       BIT           NOT NULL,   -- 1 = CDC enabled on the source table
    scd_type             VARCHAR(10)   NOT NULL,   -- 0 / 1 / 2
    source_table         VARCHAR(200)  NOT NULL,   -- table in the source database
    bronze_table         VARCHAR(200)  NOT NULL,
    silver_table         VARCHAR(200)  NOT NULL,
    is_active            BIT           NOT NULL,
    last_lsn             BINARY(10)    NULL,       -- CDC bookmark, filled by the Bronze load
    last_silver_batch_id BIGINT        NULL,       -- Bronze -> Silver bookmark, filled by SSIS
    target_table         VARCHAR(200)  NULL        -- Gold object fed by the entity
);
GO

---------------------------------------------------------
-- Configuration values
-- category, subcategory, sales_territory and ship_method are
-- small SCD1 objects: full, no CDC. The other 11 are
-- incremental with CDC (sales_person included: it is SCD2).
---------------------------------------------------------
INSERT INTO etl.config (
    object_name, object_type, load_order, load_type, is_cdc_enabled, scd_type,
    source_table, bronze_table, silver_table,
    is_active, target_table
)
VALUES
('category', 'erp', 10, 'full', 0, '1',
 'AdventureWorks2025.Production.ProductCategory',
 'bronze.erp_category',
 'silver.erp_category',
 1, 'dim_product'),

('subcategory', 'erp', 20, 'full', 0, '1',
 'AdventureWorks2025.Production.ProductSubcategory',
 'bronze.erp_subcategory',
 'silver.erp_subcategory',
 1, 'dim_product'),

('product', 'erp', 30, 'incremental', 1, '2',
 'AdventureWorks2025.Production.Product',
 'bronze.erp_product',
 'silver.erp_product',
 1, 'dim_product'),

('store', 'erp', 40, 'incremental', 1, '1',
 'AdventureWorks2025.Sales.Store',
 'bronze.erp_store',
 'silver.erp_store',
 1, 'dim_customer'),

('country_region', 'erp', 50, 'incremental', 1, '0',
 'AdventureWorks2025.Person.CountryRegion',
 'bronze.erp_country_region',
 'silver.erp_country_region',
 1, 'dim_customer'),

('person', 'erp', 60, 'incremental', 1, '1',
 'AdventureWorks2025.Person.Person',
 'bronze.erp_person',
 'silver.erp_person',
 1, 'dim_customer'),

('sales_territory', 'erp', 70, 'full', 0, '1',
 'AdventureWorks2025.Sales.SalesTerritory',
 'bronze.erp_sales_territory',
 'silver.erp_sales_territory',
 1, 'dim_customer'),

('customer', 'erp', 80, 'incremental', 1, '2',
 'AdventureWorks2025.Sales.Customer',
 'bronze.erp_customer',
 'silver.erp_customer',
 1, 'dim_customer'),

('employee', 'erp', 90, 'incremental', 1, '1',
 'AdventureWorks2025.HumanResources.Employee',
 'bronze.erp_employee',
 'silver.erp_employee',
 1, 'dim_sales_person'),

('sales_person', 'erp', 100, 'incremental', 1, '2',
 'AdventureWorks2025.Sales.SalesPerson',
 'bronze.erp_sales_person',
 'silver.erp_sales_person',
 1, 'dim_sales_person'),

('ship_method', 'erp', 110, 'full', 0, '1',
 'AdventureWorks2025.Purchasing.ShipMethod',
 'bronze.erp_ship_method',
 'silver.erp_ship_method',
 1, 'dim_ship_method'),

('state_province', 'erp', 120, 'incremental', 1, '1',
 'AdventureWorks2025.Person.StateProvince',
 'bronze.erp_state_province',
 'silver.erp_state_province',
 1, 'dim_geography'),

('person_address', 'erp', 130, 'incremental', 1, '1',
 'AdventureWorks2025.Person.Address',
 'bronze.erp_person_address',
 'silver.erp_person_address',
 1, 'dim_geography'),

('sales_order_header', 'erp', 140, 'incremental', 1, '1',
 'AdventureWorks2025.Sales.SalesOrderHeader',
 'bronze.erp_sales_order_header',
 'silver.erp_sales_order_header',
 1, 'fact_sales_order'),

('sales_order_detail', 'erp', 150, 'incremental', 1, '1',
 'AdventureWorks2025.Sales.SalesOrderDetail',
 'bronze.erp_sales_order_detail',
 'silver.erp_sales_order_detail',
 1, 'fact_sales_order');
GO
