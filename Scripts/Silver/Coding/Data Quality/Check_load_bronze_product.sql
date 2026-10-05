/*
=========================================================
Bronze Data Quality Check – Product
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Product domain.
    It checks for null keys, duplicate keys, missing foreign
    keys, invalid descriptive fields, and negative numeric
    values.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure for
    each imported source table. Each procedure logs issues
    into etl.data_quality_log and returns the number of
    detected errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Product object in the Bronze layer.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_product]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_start_time DATETIME = GETDATE();
    DECLARE @batch_end_time DATETIME;

    PRINT '====================================================';
    PRINT 'Bronze Data Quality Check: Product';
    PRINT '====================================================';

    ------------------------------------------------------------
    -- 1. Delete previous logs
    ------------------------------------------------------------
    DELETE FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'product';

    ------------------------------------------------------------
    -- 2. ProductID NULL
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'ProductID NULL',
        CAST(ProductID AS NVARCHAR(200)),
        'ProductID is NULL',
        'critical'
    FROM bronze.erp_product
    WHERE ProductID IS NULL;

    ------------------------------------------------------------
    -- 3. Duplicate ProductID
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'Duplicate ProductID',
        CAST(ProductID AS NVARCHAR(200)),
        'Duplicate ProductID detected',
        'critical'
    FROM bronze.erp_product
    GROUP BY ProductID
    HAVING COUNT(*) > 1;

    ------------------------------------------------------------
    -- 4. Name NULL or empty
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'Name NULL or empty',
        CAST(ProductID AS NVARCHAR(200)),
        'Name is NULL or empty',
        'error'
    FROM bronze.erp_product
    WHERE Name IS NULL
       OR LTRIM(RTRIM(Name)) = '';

    ------------------------------------------------------------
    -- 5. ProductNumber NULL or empty
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'ProductNumber NULL or empty',
        CAST(ProductID AS NVARCHAR(200)),
        'ProductNumber is NULL or empty',
        'error'
    FROM bronze.erp_product
    WHERE ProductNumber IS NULL
       OR LTRIM(RTRIM(ProductNumber)) = '';

    ------------------------------------------------------------
    -- 6. SellStartDate NULL
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'SellStartDate NULL',
        CAST(ProductID AS NVARCHAR(200)),
        'SellStartDate is NULL',
        'error'
    FROM bronze.erp_product
    WHERE SellStartDate IS NULL;

    ------------------------------------------------------------
    -- 7. ProductSubcategoryID NULL (FK missing)
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'SubCategoryID NULL',
        CAST(ProductID AS NVARCHAR(200)),
        'ProductSubcategoryID is NULL',
        'error'
    FROM bronze.erp_product
    WHERE ProductSubcategoryID IS NULL;

    ------------------------------------------------------------
    -- 8. Negative or NULL numeric values
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'Negative numeric value',
        CAST(ProductID AS NVARCHAR(200)),
        'One or more numeric fields are negative or NULL',
        'error'
    FROM bronze.erp_product
    WHERE StandardCost < 0
       OR ListPrice < 0
       OR SafetyStockLevel < 0
       OR ReorderPoint < 0
       OR Weight < 0
       OR DaysToManufacture < 0
       OR StandardCost IS NULL
       OR ListPrice IS NULL
       OR SafetyStockLevel IS NULL
       OR ReorderPoint IS NULL
       OR Weight IS NULL
       OR DaysToManufacture IS NULL;

    ------------------------------------------------------------
    -- 9. Descriptive fields empty
    ------------------------------------------------------------
    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'product',
        'Descriptive field empty',
        CAST(ProductID AS NVARCHAR(200)),
        'One or more descriptive fields are empty',
        'warning'
    FROM bronze.erp_product
    WHERE LTRIM(RTRIM(Color)) = ''
       OR LTRIM(RTRIM(Size)) = ''
       OR LTRIM(RTRIM(ProductLine)) = ''
       OR LTRIM(RTRIM(Class)) = ''
       OR LTRIM(RTRIM(Style)) = '';

    ------------------------------------------------------------
    -- Count errors
    ------------------------------------------------------------
    DECLARE @errors INT;

    SELECT @errors = COUNT(*)
    FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'product'
      AND severity IN ('error', 'critical');

    PRINT '====================================================';
    PRINT 'Number of Errors: ' + CAST(@errors AS NVARCHAR);
    PRINT '====================================================';

    SET @batch_end_time = GETDATE();

    PRINT '====================================================';
    PRINT 'Bronze Data Quality Product Completed in '
          + CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
          + ' ms';
    PRINT '====================================================';

    RETURN @errors;
END;
GO
