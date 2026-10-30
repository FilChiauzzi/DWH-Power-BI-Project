/*
=========================================================
Error Event Schema – screen catalog (Kimball subsystem 5)
=========================================================
Purpose:
    Fills the Screen Dimension (etl.dim_screen), the tables
    (etl.dim_table), the fields (etl.dim_field) and the fields
    tested by each screen (etl.bridge_screen_field).

    One row per quality screen of the 15 quality checks
    (Scripts/Silver/Coding/Check_Bronze_Quality) and of the
    Bronze loads (volume checks, reconciliation):
      screen_type       column / structure / business rule
      exception_action  set aside (critical), pass (error,
                        warning), halt (aggregate thresholds)

    Generated from the code of the procedures: when a screen
    is added or changed, add or change its row here.

SSIS Usage:
    Not executed by SSIS. Run after ETL_Error_Event_Schema.sql.

Warning:
    Re-runnable: rows already present (same object and screen
    name) are updated, new ones are added. The keys of existing
    screens never change.
*/

USE [DWH_AdventureWorks2025];
GO

SET NOCOUNT ON;

---------------------------------------------------------
-- 1. Tables
---------------------------------------------------------
DECLARE @t TABLE (object_name VARCHAR(50), layer VARCHAR(10), table_name NVARCHAR(200));
INSERT INTO @t VALUES
    ('category', 'bronze', N'bronze.erp_category'),
    ('country_region', 'bronze', N'bronze.erp_country_region'),
    ('employee', 'bronze', N'bronze.erp_employee'),
    ('person', 'bronze', N'bronze.erp_person'),
    ('person_address', 'bronze', N'bronze.erp_person_address'),
    ('product', 'bronze', N'bronze.erp_product'),
    ('customer', 'bronze', N'bronze.erp_customer'),
    ('sales_order_detail', 'bronze', N'bronze.erp_sales_order_detail'),
    ('sales_order_header', 'bronze', N'bronze.erp_sales_order_header'),
    ('sales_person', 'bronze', N'bronze.erp_sales_person'),
    ('sales_territory', 'bronze', N'bronze.erp_sales_territory'),
    ('ship_method', 'bronze', N'bronze.erp_ship_method'),
    ('state_province', 'bronze', N'bronze.erp_state_province'),
    ('store', 'bronze', N'bronze.erp_store'),
    ('subcategory', 'bronze', N'bronze.erp_subcategory');

INSERT INTO etl.dim_table (table_key, object_name, layer, table_name)
SELECT ISNULL((SELECT MAX(table_key) FROM etl.dim_table), 0) + ROW_NUMBER() OVER (ORDER BY t.object_name),
       t.object_name, t.layer, t.table_name
FROM @t AS t
WHERE NOT EXISTS (SELECT 1 FROM etl.dim_table AS d WHERE d.object_name = t.object_name AND d.layer = t.layer);
GO

---------------------------------------------------------
-- 2. Screens
---------------------------------------------------------
DECLARE @s TABLE (object_name VARCHAR(50), screen_name NVARCHAR(200), screen_type VARCHAR(20), etl_module NVARCHAR(256),
                  screen_processing_definition NVARCHAR(500), severity VARCHAR(20), exception_action VARCHAR(20));
INSERT INTO @s VALUES
    ('category', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_category', N'source empty', 'warning', 'pass'),
    ('category', N'ProductCategoryID NULL', 'column', N'usp_check_bronze_quality_load_category', N'business key NULL', 'critical', 'set aside'),
    ('category', N'Duplicate ProductCategoryID', 'structure', N'usp_check_bronze_quality_load_category', N'duplicated business key', 'critical', 'set aside'),
    ('category', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_category', N'name NULL or empty', 'warning', 'pass'),
    ('category', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_category', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('country_region', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_country_region', N'source empty', 'warning', 'pass'),
    ('country_region', N'CountryRegionCode NULL', 'column', N'usp_check_bronze_quality_load_country_region', N'CountryRegionCode NULL', 'critical', 'set aside'),
    ('country_region', N'Duplicate CountryRegionCode', 'structure', N'usp_check_bronze_quality_load_country_region', N'Duplicate CountryRegionCode', 'critical', 'set aside'),
    ('country_region', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_country_region', N'Name NULL or empty', 'warning', 'pass'),
    ('country_region', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_country_region', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('employee', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_employee', N'source empty', 'warning', 'pass'),
    ('employee', N'BusinessEntityID NULL', 'column', N'usp_check_bronze_quality_load_employee', N'BusinessEntityID NULL', 'critical', 'set aside'),
    ('employee', N'Duplicate BusinessEntityID', 'structure', N'usp_check_bronze_quality_load_employee', N'Duplicate BusinessEntityID', 'critical', 'set aside'),
    ('employee', N'JobTitle NULL', 'column', N'usp_check_bronze_quality_load_employee', N'JobTitle NULL', 'warning', 'pass'),
    ('employee', N'Gender NULL', 'column', N'usp_check_bronze_quality_load_employee', N'Gender NULL', 'warning', 'pass'),
    ('employee', N'HireDate NULL', 'business rule', N'usp_check_bronze_quality_load_employee', N'HireDate NULL', 'warning', 'pass'),
    ('employee', N'CurrentFlag NULL', 'column', N'usp_check_bronze_quality_load_employee', N'CurrentFlag NULL', 'warning', 'pass'),
    ('employee', N'Person not found', 'structure', N'usp_check_bronze_quality_load_employee', N'Person not found in Silver', 'error', 'pass'),
    ('employee', N'Hire age below 16', 'business rule', N'usp_check_bronze_quality_load_employee', N'Hired before 16', 'warning', 'pass'),
    ('employee', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_employee', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('person', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_person', N'source empty', 'warning', 'pass'),
    ('person', N'BusinessEntityID NULL', 'column', N'usp_check_bronze_quality_load_person', N'BusinessEntityID NULL', 'critical', 'set aside'),
    ('person', N'Duplicate BusinessEntityID', 'structure', N'usp_check_bronze_quality_load_person', N'Duplicate BusinessEntityID', 'critical', 'set aside'),
    ('person', N'FirstName NULL or empty', 'column', N'usp_check_bronze_quality_load_person', N'FirstName NULL or empty', 'warning', 'pass'),
    ('person', N'LastName NULL or empty', 'column', N'usp_check_bronze_quality_load_person', N'LastName NULL or empty', 'warning', 'pass'),
    ('person', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_person', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('person_address', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_person_address', N'source empty', 'warning', 'pass'),
    ('person_address', N'AddressID NULL', 'column', N'usp_check_bronze_quality_load_person_address', N'AddressID NULL', 'critical', 'set aside'),
    ('person_address', N'Duplicate AddressID', 'structure', N'usp_check_bronze_quality_load_person_address', N'Duplicate AddressID', 'critical', 'set aside'),
    ('person_address', N'City NULL', 'column', N'usp_check_bronze_quality_load_person_address', N'City NULL', 'warning', 'pass'),
    ('person_address', N'StateProvinceID NULL', 'column', N'usp_check_bronze_quality_load_person_address', N'StateProvinceID NULL', 'error', 'pass'),
    ('person_address', N'PostalCode NULL', 'column', N'usp_check_bronze_quality_load_person_address', N'PostalCode NULL', 'warning', 'pass'),
    ('person_address', N'State/province not found', 'structure', N'usp_check_bronze_quality_load_person_address', N'State/province not found in Silver', 'error', 'pass'),
    ('person_address', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_person_address', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('product', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_product', N'source empty', 'warning', 'pass'),
    ('product', N'ProductID NULL', 'column', N'usp_check_bronze_quality_load_product', N'ProductID NULL', 'critical', 'set aside'),
    ('product', N'Duplicate ProductID', 'structure', N'usp_check_bronze_quality_load_product', N'Duplicate ProductID', 'critical', 'set aside'),
    ('product', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_product', N'Name NULL or empty', 'error', 'pass'),
    ('product', N'ProductNumber NULL or empty', 'column', N'usp_check_bronze_quality_load_product', N'ProductNumber NULL or empty', 'error', 'pass'),
    ('product', N'SellStartDate NULL', 'column', N'usp_check_bronze_quality_load_product', N'SellStartDate NULL', 'error', 'pass'),
    ('product', N'Negative or NULL numeric value', 'column', N'usp_check_bronze_quality_load_product', N'Numeric fields NULL or negative', 'error', 'pass'),
    ('product', N'Weight without unit', 'column', N'usp_check_bronze_quality_load_product', N'Weight without unit of measure', 'error', 'pass'),
    ('product', N'Descriptive field empty', 'column', N'usp_check_bronze_quality_load_product', N'Descriptive fields empty', 'warning', 'pass'),
    ('product', N'Subcategory not found', 'structure', N'usp_check_bronze_quality_load_product', N'Subcategory not found in Silver', 'error', 'pass'),
    ('product', N'Sell dates inverted', 'business rule', N'usp_check_bronze_quality_load_product', N'SellEndDate before SellStartDate', 'error', 'pass'),
    ('product', N'Price below cost', 'business rule', N'usp_check_bronze_quality_load_product', N'ListPrice below StandardCost', 'warning', 'pass'),
    ('product', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_product', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('customer', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_customer', N'source empty', 'warning', 'pass'),
    ('customer', N'CustomerID NULL', 'column', N'usp_check_bronze_quality_load_customer', N'CustomerID NULL', 'critical', 'set aside'),
    ('customer', N'Duplicate CustomerID', 'structure', N'usp_check_bronze_quality_load_customer', N'Duplicate CustomerID', 'critical', 'set aside'),
    ('customer', N'TerritoryID NULL', 'column', N'usp_check_bronze_quality_load_customer', N'TerritoryID NULL', 'error', 'pass'),
    ('customer', N'Territory not found', 'structure', N'usp_check_bronze_quality_load_customer', N'Territory not found in Silver', 'error', 'pass'),
    ('customer', N'Person not found', 'structure', N'usp_check_bronze_quality_load_customer', N'Person not found in Silver', 'error', 'pass'),
    ('customer', N'Store not found', 'structure', N'usp_check_bronze_quality_load_customer', N'Store not found in Silver', 'error', 'pass'),
    ('customer', N'Customer without person or store', 'business rule', N'usp_check_bronze_quality_load_customer', N'Neither person nor store', 'error', 'pass'),
    ('customer', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_customer', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('sales_order_detail', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_sales_order_detail', N'source empty', 'warning', 'pass'),
    ('sales_order_detail', N'SalesOrderID NULL', 'column', N'usp_check_bronze_quality_load_sales_order_detail', N'SalesOrderID NULL', 'critical', 'set aside'),
    ('sales_order_detail', N'SalesOrderDetailID NULL', 'column', N'usp_check_bronze_quality_load_sales_order_detail', N'SalesOrderDetailID NULL', 'critical', 'set aside'),
    ('sales_order_detail', N'Duplicate line', 'structure', N'usp_check_bronze_quality_load_sales_order_detail', N'Duplicate (SalesOrderID, SalesOrderDetailID)', 'critical', 'set aside'),
    ('sales_order_detail', N'OrderQty invalid', 'column', N'usp_check_bronze_quality_load_sales_order_detail', N'OrderQty <= 0', 'error', 'pass'),
    ('sales_order_detail', N'Negative price', 'column', N'usp_check_bronze_quality_load_sales_order_detail', N'UnitPrice < 0 OR UnitPriceDiscount < 0', 'error', 'pass'),
    ('sales_order_detail', N'LineTotal mismatch', 'business rule', N'usp_check_bronze_quality_load_sales_order_detail', N'LineTotal mismatch', 'error', 'pass'),
    ('sales_order_detail', N'Order header not found', 'structure', N'usp_check_bronze_quality_load_sales_order_detail', N'Order header not found in Silver', 'error', 'pass'),
    ('sales_order_detail', N'Product not found', 'structure', N'usp_check_bronze_quality_load_sales_order_detail', N'Product not found in Silver', 'error', 'pass'),
    ('sales_order_detail', N'Discount above 100%', 'business rule', N'usp_check_bronze_quality_load_sales_order_detail', N'UnitPriceDiscount above 100%', 'error', 'pass'),
    ('sales_order_detail', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_sales_order_detail', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('sales_order_header', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'source empty', 'warning', 'pass'),
    ('sales_order_header', N'SalesOrderID NULL', 'column', N'usp_check_bronze_quality_load_sales_order_header', N'SalesOrderID NULL', 'critical', 'set aside'),
    ('sales_order_header', N'Duplicate SalesOrderID', 'structure', N'usp_check_bronze_quality_load_sales_order_header', N'Duplicate SalesOrderID', 'critical', 'set aside'),
    ('sales_order_header', N'OrderDate NULL', 'column', N'usp_check_bronze_quality_load_sales_order_header', N'OrderDate NULL', 'error', 'pass'),
    ('sales_order_header', N'DueDate < OrderDate', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'DueDate < OrderDate', 'error', 'pass'),
    ('sales_order_header', N'ShipDate < OrderDate', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'ShipDate < OrderDate', 'error', 'pass'),
    ('sales_order_header', N'ModifiedDate < OrderDate', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'ModifiedDate < OrderDate', 'error', 'pass'),
    ('sales_order_header', N'Negative numeric value', 'column', N'usp_check_bronze_quality_load_sales_order_header', N'Numeric fields < 0', 'error', 'pass'),
    ('sales_order_header', N'TotalDue mismatch', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'TotalDue mismatch', 'error', 'pass'),
    ('sales_order_header', N'CustomerID NULL', 'column', N'usp_check_bronze_quality_load_sales_order_header', N'CustomerID NULL', 'error', 'pass'),
    ('sales_order_header', N'ShipMethodID NULL', 'column', N'usp_check_bronze_quality_load_sales_order_header', N'ShipMethodID NULL', 'error', 'pass'),
    ('sales_order_header', N'Customer not found', 'structure', N'usp_check_bronze_quality_load_sales_order_header', N'Customer not found in Silver', 'error', 'pass'),
    ('sales_order_header', N'Sales person not found', 'structure', N'usp_check_bronze_quality_load_sales_order_header', N'Sales person not found in Silver', 'error', 'pass'),
    ('sales_order_header', N'Territory not found', 'structure', N'usp_check_bronze_quality_load_sales_order_header', N'Territory not found in Silver', 'error', 'pass'),
    ('sales_order_header', N'Ship method not found', 'structure', N'usp_check_bronze_quality_load_sales_order_header', N'Ship method not found in Silver', 'error', 'pass'),
    ('sales_order_header', N'Status invalid', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'Status outside 1-6', 'error', 'pass'),
    ('sales_order_header', N'OrderDate in the future', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'OrderDate in the future', 'error', 'pass'),
    ('sales_order_header', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_sales_order_header', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('sales_person', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_sales_person', N'source empty', 'warning', 'pass'),
    ('sales_person', N'BusinessEntityID NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'BusinessEntityID NULL', 'critical', 'set aside'),
    ('sales_person', N'Duplicate BusinessEntityID', 'structure', N'usp_check_bronze_quality_load_sales_person', N'Duplicate BusinessEntityID', 'critical', 'set aside'),
    ('sales_person', N'TerritoryID NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'TerritoryID NULL', 'warning', 'pass'),
    ('sales_person', N'CommissionPct NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'CommissionPct NULL', 'warning', 'pass'),
    ('sales_person', N'SalesQuota NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'SalesQuota NULL', 'warning', 'pass'),
    ('sales_person', N'Bonus NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'Bonus NULL', 'warning', 'pass'),
    ('sales_person', N'SalesYTD NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'SalesYTD NULL', 'warning', 'pass'),
    ('sales_person', N'SalesLastYear NULL', 'column', N'usp_check_bronze_quality_load_sales_person', N'SalesLastYear NULL', 'warning', 'pass'),
    ('sales_person', N'Employee not found', 'structure', N'usp_check_bronze_quality_load_sales_person', N'Employee not found in Silver', 'error', 'pass'),
    ('sales_person', N'Territory not found', 'structure', N'usp_check_bronze_quality_load_sales_person', N'Territory not found in Silver', 'error', 'pass'),
    ('sales_person', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_sales_person', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('sales_territory', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_sales_territory', N'source empty', 'warning', 'pass'),
    ('sales_territory', N'TerritoryID NULL', 'column', N'usp_check_bronze_quality_load_sales_territory', N'TerritoryID NULL', 'critical', 'set aside'),
    ('sales_territory', N'Duplicate TerritoryID', 'structure', N'usp_check_bronze_quality_load_sales_territory', N'Duplicate TerritoryID', 'critical', 'set aside'),
    ('sales_territory', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_sales_territory', N'Name NULL or empty', 'warning', 'pass'),
    ('sales_territory', N'Group NULL or empty', 'column', N'usp_check_bronze_quality_load_sales_territory', N'Group NULL or empty', 'error', 'pass'),
    ('sales_territory', N'CountryRegionCode NULL or empty', 'column', N'usp_check_bronze_quality_load_sales_territory', N'CountryRegionCode NULL or empty', 'error', 'pass'),
    ('sales_territory', N'Country not found', 'structure', N'usp_check_bronze_quality_load_sales_territory', N'Country not found in Silver', 'error', 'pass'),
    ('sales_territory', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_sales_territory', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('ship_method', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_ship_method', N'source empty', 'warning', 'pass'),
    ('ship_method', N'ShipMethodID NULL', 'column', N'usp_check_bronze_quality_load_ship_method', N'ShipMethodID NULL', 'critical', 'set aside'),
    ('ship_method', N'Duplicate ShipMethodID', 'structure', N'usp_check_bronze_quality_load_ship_method', N'Duplicate ShipMethodID', 'critical', 'set aside'),
    ('ship_method', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_ship_method', N'Name NULL or empty', 'warning', 'pass'),
    ('ship_method', N'ShipBase NULL', 'column', N'usp_check_bronze_quality_load_ship_method', N'ShipBase NULL', 'warning', 'pass'),
    ('ship_method', N'ShipRate NULL', 'column', N'usp_check_bronze_quality_load_ship_method', N'ShipRate NULL', 'warning', 'pass'),
    ('ship_method', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_ship_method', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('state_province', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_state_province', N'source empty', 'warning', 'pass'),
    ('state_province', N'StateProvinceID NULL', 'column', N'usp_check_bronze_quality_load_state_province', N'StateProvinceID NULL', 'critical', 'set aside'),
    ('state_province', N'Duplicate StateProvinceID', 'structure', N'usp_check_bronze_quality_load_state_province', N'Duplicate StateProvinceID', 'critical', 'set aside'),
    ('state_province', N'CountryRegionCode NULL', 'column', N'usp_check_bronze_quality_load_state_province', N'CountryRegionCode NULL', 'error', 'pass'),
    ('state_province', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_state_province', N'Name NULL or empty', 'warning', 'pass'),
    ('state_province', N'TerritoryID NULL', 'column', N'usp_check_bronze_quality_load_state_province', N'TerritoryID NULL', 'warning', 'pass'),
    ('state_province', N'Country not found', 'structure', N'usp_check_bronze_quality_load_state_province', N'Country not found in Silver', 'error', 'pass'),
    ('state_province', N'Territory not found', 'structure', N'usp_check_bronze_quality_load_state_province', N'Territory not found in Silver', 'error', 'pass'),
    ('state_province', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_state_province', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('store', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_store', N'source empty', 'warning', 'pass'),
    ('store', N'BusinessEntityID NULL', 'column', N'usp_check_bronze_quality_load_store', N'BusinessEntityID NULL', 'critical', 'set aside'),
    ('store', N'Duplicate BusinessEntityID', 'structure', N'usp_check_bronze_quality_load_store', N'Duplicate BusinessEntityID', 'critical', 'set aside'),
    ('store', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_store', N'Name NULL or empty', 'warning', 'pass'),
    ('store', N'ModifiedDate NULL', 'column', N'usp_check_bronze_quality_load_store', N'ModifiedDate NULL', 'error', 'pass'),
    ('store', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_store', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('subcategory', N'Source empty', 'business rule', N'usp_check_bronze_quality_load_subcategory', N'source empty', 'warning', 'pass'),
    ('subcategory', N'SubCategoryID NULL', 'column', N'usp_check_bronze_quality_load_subcategory', N'ProductSubcategoryID NULL', 'critical', 'set aside'),
    ('subcategory', N'CategoryID NULL', 'column', N'usp_check_bronze_quality_load_subcategory', N'ProductCategoryID NULL', 'error', 'pass'),
    ('subcategory', N'Duplicate SubCategoryID', 'structure', N'usp_check_bronze_quality_load_subcategory', N'Duplicate ProductSubcategoryID', 'critical', 'set aside'),
    ('subcategory', N'Name NULL or empty', 'column', N'usp_check_bronze_quality_load_subcategory', N'Name NULL or empty', 'warning', 'pass'),
    ('subcategory', N'Category not found', 'structure', N'usp_check_bronze_quality_load_subcategory', N'Category not found in Silver', 'error', 'pass'),
    ('subcategory', N'Rejection rate above threshold', 'business rule', N'usp_check_bronze_quality_load_subcategory', N'More rows set aside than @max_reject_rate (default 20%): THROW 51010, pipeline stopped', 'critical', 'halt'),
    ('category', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('category', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('category', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('category', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('country_region', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('country_region', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('country_region', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('country_region', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('employee', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('employee', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('employee', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('employee', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('person', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('person', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('person', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('person', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('person_address', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('person_address', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('person_address', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('person_address', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('product', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('product', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('product', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('product', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('customer', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('customer', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('customer', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('customer', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('sales_order_detail', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('sales_order_detail', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('sales_order_detail', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('sales_order_detail', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('sales_order_header', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('sales_order_header', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('sales_order_header', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('sales_order_header', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('sales_person', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('sales_person', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('sales_person', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('sales_person', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('sales_territory', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('sales_territory', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('sales_territory', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('sales_territory', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('ship_method', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('ship_method', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('ship_method', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('ship_method', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('state_province', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('state_province', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('state_province', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('state_province', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('store', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('store', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('store', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('store', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt'),
    ('subcategory', N'Row count reconciliation', 'business rule', N'usp_load_bronze', N'Full load: rows counted in the source before the copy differ from the rows copied', 'warning', 'pass'),
    ('subcategory', N'State reconciliation', 'business rule', N'usp_load_bronze_cdc', N'CDC: current state of Bronze (latest version of each key, deletes excluded) differs from the rows of the source', 'warning', 'pass'),
    ('subcategory', N'Source empty or dropped', 'business rule', N'usp_load_bronze / usp_load_bronze_cdc', N'Volume check before the load: empty source or more than 50% fewer rows than the last load (51004, 51005, 52005, 52006)', 'critical', 'halt'),
    ('subcategory', N'Mass delete', 'business rule', N'usp_load_bronze_cdc', N'More than 50% of the current rows deleted in one night (52007)', 'critical', 'halt');

UPDATE d
SET d.screen_type = s.screen_type, d.etl_module = s.etl_module,
    d.screen_processing_definition = s.screen_processing_definition,
    d.severity = s.severity, d.exception_action = s.exception_action
FROM etl.dim_screen AS d
JOIN @s AS s ON s.object_name = d.object_name AND s.screen_name = d.screen_name;

INSERT INTO etl.dim_screen (screen_key, screen_name, object_name, screen_type, etl_module,
                            screen_processing_definition, severity, exception_action)
SELECT ISNULL((SELECT MAX(screen_key) FROM etl.dim_screen), 0) + ROW_NUMBER() OVER (ORDER BY s.object_name, s.screen_name),
       s.screen_name, s.object_name, s.screen_type, s.etl_module,
       s.screen_processing_definition, s.severity, s.exception_action
FROM @s AS s
WHERE NOT EXISTS (SELECT 1 FROM etl.dim_screen AS d WHERE d.object_name = s.object_name AND d.screen_name = s.screen_name);
GO

---------------------------------------------------------
-- 3. Fields and the fields tested by each screen
---------------------------------------------------------
DECLARE @f TABLE (object_name VARCHAR(50), screen_name NVARCHAR(200), field_name SYSNAME);
INSERT INTO @f VALUES
    ('category', N'ProductCategoryID NULL', N'ProductCategoryID'),
    ('category', N'Duplicate ProductCategoryID', N'ProductCategoryID'),
    ('category', N'Name NULL or empty', N'Name'),
    ('country_region', N'CountryRegionCode NULL', N'CountryRegionCode'),
    ('country_region', N'Duplicate CountryRegionCode', N'CountryRegionCode'),
    ('country_region', N'Name NULL or empty', N'Name'),
    ('employee', N'Duplicate BusinessEntityID', N'BusinessEntityID'),
    ('employee', N'JobTitle NULL', N'JobTitle'),
    ('employee', N'Gender NULL', N'Gender'),
    ('employee', N'HireDate NULL', N'HireDate'),
    ('employee', N'CurrentFlag NULL', N'CurrentFlag'),
    ('employee', N'Person not found', N'BusinessEntityID'),
    ('employee', N'Hire age below 16', N'BirthDate'),
    ('employee', N'Hire age below 16', N'HireDate'),
    ('person', N'BusinessEntityID NULL', N'BusinessEntityID'),
    ('person', N'Duplicate BusinessEntityID', N'BusinessEntityID'),
    ('person', N'FirstName NULL or empty', N'FirstName'),
    ('person', N'LastName NULL or empty', N'LastName'),
    ('person_address', N'State/province not found', N'StateProvinceID'),
    ('product', N'ProductID NULL', N'ProductID'),
    ('product', N'Duplicate ProductID', N'ProductID'),
    ('product', N'Name NULL or empty', N'Name'),
    ('product', N'ProductNumber NULL or empty', N'ProductNumber'),
    ('product', N'SellStartDate NULL', N'SellStartDate'),
    ('product', N'Negative or NULL numeric value', N'StandardCost'),
    ('product', N'Negative or NULL numeric value', N'ListPrice'),
    ('product', N'Negative or NULL numeric value', N'SafetyStockLevel'),
    ('product', N'Negative or NULL numeric value', N'ReorderPoint'),
    ('product', N'Negative or NULL numeric value', N'Weight'),
    ('product', N'Negative or NULL numeric value', N'DaysToManufacture'),
    ('product', N'Weight without unit', N'Weight'),
    ('product', N'Weight without unit', N'WeightUnitMeasureCode'),
    ('product', N'Descriptive field empty', N'Color'),
    ('product', N'Descriptive field empty', N'Size'),
    ('product', N'Descriptive field empty', N'ProductLine'),
    ('product', N'Descriptive field empty', N'Class'),
    ('product', N'Descriptive field empty', N'Style'),
    ('product', N'Subcategory not found', N'ProductSubcategoryID'),
    ('product', N'Sell dates inverted', N'SellEndDate'),
    ('product', N'Sell dates inverted', N'SellStartDate'),
    ('product', N'Price below cost', N'ListPrice'),
    ('product', N'Price below cost', N'StandardCost'),
    ('customer', N'CustomerID NULL', N'CustomerID'),
    ('customer', N'Duplicate CustomerID', N'CustomerID'),
    ('customer', N'TerritoryID NULL', N'TerritoryID'),
    ('customer', N'Territory not found', N'TerritoryID'),
    ('customer', N'Person not found', N'PersonID'),
    ('customer', N'Store not found', N'StoreID'),
    ('customer', N'Customer without person or store', N'PersonID'),
    ('customer', N'Customer without person or store', N'StoreID'),
    ('sales_order_detail', N'SalesOrderID NULL', N'SalesOrderID'),
    ('sales_order_detail', N'SalesOrderDetailID NULL', N'SalesOrderDetailID'),
    ('sales_order_detail', N'Duplicate line', N'SalesOrderID'),
    ('sales_order_detail', N'Duplicate line', N'SalesOrderDetailID'),
    ('sales_order_detail', N'OrderQty invalid', N'OrderQty'),
    ('sales_order_detail', N'Negative price', N'UnitPrice'),
    ('sales_order_detail', N'Negative price', N'UnitPriceDiscount'),
    ('sales_order_detail', N'LineTotal mismatch', N'LineTotal'),
    ('sales_order_detail', N'LineTotal mismatch', N'UnitPrice'),
    ('sales_order_detail', N'LineTotal mismatch', N'UnitPriceDiscount'),
    ('sales_order_detail', N'LineTotal mismatch', N'OrderQty'),
    ('sales_order_detail', N'Order header not found', N'SalesOrderID'),
    ('sales_order_detail', N'Product not found', N'ProductID'),
    ('sales_order_detail', N'Discount above 100%', N'UnitPriceDiscount'),
    ('sales_order_header', N'SalesOrderID NULL', N'SalesOrderID'),
    ('sales_order_header', N'Duplicate SalesOrderID', N'SalesOrderID'),
    ('sales_order_header', N'OrderDate NULL', N'OrderDate'),
    ('sales_order_header', N'DueDate < OrderDate', N'DueDate'),
    ('sales_order_header', N'DueDate < OrderDate', N'OrderDate'),
    ('sales_order_header', N'ShipDate < OrderDate', N'ShipDate'),
    ('sales_order_header', N'ShipDate < OrderDate', N'OrderDate'),
    ('sales_order_header', N'ModifiedDate < OrderDate', N'ModifiedDate'),
    ('sales_order_header', N'ModifiedDate < OrderDate', N'OrderDate'),
    ('sales_order_header', N'Negative numeric value', N'SubTotal'),
    ('sales_order_header', N'Negative numeric value', N'TaxAmt'),
    ('sales_order_header', N'Negative numeric value', N'Freight'),
    ('sales_order_header', N'TotalDue mismatch', N'TotalDue'),
    ('sales_order_header', N'TotalDue mismatch', N'SubTotal'),
    ('sales_order_header', N'TotalDue mismatch', N'TaxAmt'),
    ('sales_order_header', N'TotalDue mismatch', N'Freight'),
    ('sales_order_header', N'CustomerID NULL', N'CustomerID'),
    ('sales_order_header', N'ShipMethodID NULL', N'ShipMethodID'),
    ('sales_order_header', N'Customer not found', N'CustomerID'),
    ('sales_order_header', N'Sales person not found', N'SalesPersonID'),
    ('sales_order_header', N'Territory not found', N'TerritoryID'),
    ('sales_order_header', N'Ship method not found', N'ShipMethodID'),
    ('sales_order_header', N'Status invalid', N'Status'),
    ('sales_order_header', N'OrderDate in the future', N'OrderDate'),
    ('sales_person', N'Duplicate BusinessEntityID', N'BusinessEntityID'),
    ('sales_person', N'TerritoryID NULL', N'TerritoryID'),
    ('sales_person', N'CommissionPct NULL', N'CommissionPct'),
    ('sales_person', N'SalesQuota NULL', N'SalesQuota'),
    ('sales_person', N'Bonus NULL', N'Bonus'),
    ('sales_person', N'SalesYTD NULL', N'SalesYTD'),
    ('sales_person', N'SalesLastYear NULL', N'SalesLastYear'),
    ('sales_person', N'Employee not found', N'BusinessEntityID'),
    ('sales_person', N'Territory not found', N'TerritoryID'),
    ('sales_territory', N'TerritoryID NULL', N'TerritoryID'),
    ('sales_territory', N'Duplicate TerritoryID', N'TerritoryID'),
    ('sales_territory', N'Name NULL or empty', N'Name'),
    ('sales_territory', N'Group NULL or empty', N'Group'),
    ('sales_territory', N'CountryRegionCode NULL or empty', N'CountryRegionCode'),
    ('sales_territory', N'Country not found', N'CountryRegionCode'),
    ('state_province', N'Country not found', N'CountryRegionCode'),
    ('state_province', N'Territory not found', N'TerritoryID'),
    ('store', N'BusinessEntityID NULL', N'BusinessEntityID'),
    ('store', N'Duplicate BusinessEntityID', N'BusinessEntityID'),
    ('store', N'Name NULL or empty', N'Name'),
    ('store', N'ModifiedDate NULL', N'ModifiedDate'),
    ('subcategory', N'SubCategoryID NULL', N'ProductSubcategoryID'),
    ('subcategory', N'CategoryID NULL', N'ProductCategoryID'),
    ('subcategory', N'Duplicate SubCategoryID', N'ProductSubcategoryID'),
    ('subcategory', N'Name NULL or empty', N'Name'),
    ('subcategory', N'Category not found', N'ProductCategoryID');

INSERT INTO etl.dim_field (field_key, table_key, field_name)
SELECT ISNULL((SELECT MAX(field_key) FROM etl.dim_field), 0) + ROW_NUMBER() OVER (ORDER BY n.table_key, n.field_name),
       n.table_key, n.field_name
FROM (SELECT DISTINCT t.table_key, f.field_name
      FROM @f AS f
      JOIN etl.dim_table AS t ON t.object_name = f.object_name AND t.layer = 'bronze') AS n
WHERE NOT EXISTS (SELECT 1 FROM etl.dim_field AS d WHERE d.table_key = n.table_key AND d.field_name = n.field_name);

INSERT INTO etl.bridge_screen_field (screen_key, field_key)
SELECT DISTINCT s.screen_key, d.field_key
FROM @f AS f
JOIN etl.dim_screen AS s ON s.object_name = f.object_name AND s.screen_name = f.screen_name
JOIN etl.dim_table  AS t ON t.object_name = f.object_name AND t.layer = 'bronze'
JOIN etl.dim_field  AS d ON d.table_key = t.table_key AND d.field_name = f.field_name
WHERE NOT EXISTS (SELECT 1 FROM etl.bridge_screen_field AS b
                  WHERE b.screen_key = s.screen_key AND b.field_key = d.field_key);
GO

-- Check
SELECT screen_type, exception_action, COUNT(*) AS screens
FROM etl.dim_screen
WHERE screen_key > 0
GROUP BY screen_type, exception_action
ORDER BY screen_type, exception_action;
GO
