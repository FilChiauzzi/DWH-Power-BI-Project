/*
=========================================================
ETL DDL – Configuration Table (etl.config)
=========================================================
Purpose:
    Creates and fills etl.config, the metadata table that
    drives the pipeline: one row per object with the source
    system (object_type), load order, load type (full or
    incremental), SCD type (0 static lookup, 1 or 2), the
    source, Bronze and Silver table names, the active flag,
    the Bronze watermark, last_silver_batch_id, last batch 
    whose Bronze rows were processed by Silver with success 
    and the Gold object it feeds.

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
    Watermarks are lost: the first Bronze load of every
    object must be a FULL load, which fills them again.
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
    load_type            VARCHAR(20)   NOT NULL,   -- full / incremental
    scd_type             VARCHAR(10)   NOT NULL,   -- 0 / 1 / 2
    source_table         VARCHAR(200)  NOT NULL,   -- table in the source database
    bronze_table         VARCHAR(200)  NOT NULL,
    silver_table         VARCHAR(200)  NOT NULL,
    is_active            BIT           NOT NULL,
    watermark            VARCHAR(20)   NULL,       -- column used as delta marker
    last_watermark_value DATETIME      NULL,       -- filled by the Bronze load
    last_silver_batch_id  BIGINT  NULL,            -- last batch processed by Silver with success (Bronze -> Silver bookmark)
    target_table         VARCHAR(200)  NULL        -- Gold object fed by the entity
);
GO

---------------------------------------------------------
-- Configuration values
---------------------------------------------------------
INSERT INTO etl.config (
    object_name, object_type, load_order, load_type, scd_type,
    source_table, bronze_table, silver_table,
    is_active, watermark, last_watermark_value, target_table
)
VALUES
('category', 'erp', 10, 'full', '1',
 'AdventureWorks2025.Production.ProductCategory',
 'bronze.erp_category',
 'silver.erp_category',
 1, 'ModifiedDate', NULL, 'dim_product'),

('subcategory', 'erp', 20, 'full', '1',
 'AdventureWorks2025.Production.ProductSubcategory',
 'bronze.erp_subcategory',
 'silver.erp_subcategory',
 1, 'ModifiedDate', NULL, 'dim_product'),

('product', 'erp', 30, 'full', '2',
 'AdventureWorks2025.Production.Product',
 'bronze.erp_product',
 'silver.erp_product',
 1, 'ModifiedDate', NULL, 'dim_product'),

('store', 'erp', 40, 'full', '1',
 'AdventureWorks2025.Sales.Store',
 'bronze.erp_store',
 'silver.erp_store',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('country_region', 'erp', 50, 'full', '0',
 'AdventureWorks2025.Person.CountryRegion',
 'bronze.erp_country_region',
 'silver.erp_country_region',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('person', 'erp', 60, 'full', '1',
 'AdventureWorks2025.Person.Person',
 'bronze.erp_person',
 'silver.erp_person',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('sales_territory', 'erp', 70, 'full', '1',
 'AdventureWorks2025.Sales.SalesTerritory',
 'bronze.erp_sales_territory',
 'silver.erp_sales_territory',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('customer', 'erp', 80, 'full', '2',
 'AdventureWorks2025.Sales.Customer',
 'bronze.erp_customer',
 'silver.erp_customer',
 1, 'ModifiedDate', NULL, 'dim_customer'),

('employee', 'erp', 90, 'full', '1',
 'AdventureWorks2025.HumanResources.Employee',
 'bronze.erp_employee',
 'silver.erp_employee',
 1, 'ModifiedDate', NULL, 'dim_sales_person'),

('sales_person', 'erp', 100, 'full', '2',
 'AdventureWorks2025.Sales.SalesPerson',
 'bronze.erp_sales_person',
 'silver.erp_sales_person',
 1, 'ModifiedDate', NULL, 'dim_sales_person'),

('ship_method', 'erp', 110, 'full', '1',
 'AdventureWorks2025.Purchasing.ShipMethod',
 'bronze.erp_ship_method',
 'silver.erp_ship_method',
 1, 'ModifiedDate', NULL, 'dim_ship_method'),

('state_province', 'erp', 120, 'full', '1',
 'AdventureWorks2025.Person.StateProvince',
 'bronze.erp_state_province',
 'silver.erp_state_province',
 1, 'ModifiedDate', NULL, 'dim_geography'),

('person_address', 'erp', 130, 'full', '1',
 'AdventureWorks2025.Person.Address',
 'bronze.erp_person_address',
 'silver.erp_person_address',
 1, 'ModifiedDate', NULL, 'dim_geography'),

('sales_order_header', 'erp', 140, 'full', '1',
 'AdventureWorks2025.Sales.SalesOrderHeader',
 'bronze.erp_sales_order_header',
 'silver.erp_sales_order_header',
 1, 'ModifiedDate', NULL, 'fact_sales_order'),

('sales_order_detail', 'erp', 150, 'full', '1',
 'AdventureWorks2025.Sales.SalesOrderDetail',
 'bronze.erp_sales_order_detail',
 'silver.erp_sales_order_detail',
 1, 'ModifiedDate', NULL, 'fact_sales_order'),

-- Gold fact (loaded by its own task, not by the loops)
('fact_sales_order', 'erp', 160, 'full', '1',
 'gold.fact_sales_order',
 'gold.fact_sales_order',
 'gold.fact_sales_order',
 0, 'dwh_load_date', NULL, 'fact_sales_order');
GO
