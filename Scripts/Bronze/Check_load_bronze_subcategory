/*
=========================================================
Bronze Data Quality Check – Subcategory
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Subcategory domain.
    It checks for null keys, missing foreign keys, duplicate
    keys, and invalid names.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure for
    each imported source table. Each procedure logs issues
    into etl.data_quality_log and returns the number of
    detected errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Subcategory object in the Bronze layer.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_subcategory]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();

    PRINT '====================================================';
    PRINT 'Delete previous logs from DataQualityLog';
    PRINT '====================================================';

    DELETE FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'subcategory';

    ---------------------------------------------------------
    -- Check 1: ProductSubcategoryID NULL
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check ProductSubcategoryID NULL';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'subcategory',
        'SubCategoryID NULL',
        CAST(ProductSubcategoryID AS NVARCHAR(200)),
        'ProductSubcategoryID is NULL',
        'error'
    FROM bronze.erp_subcategory
    WHERE ProductSubcategoryID IS NULL;

    ---------------------------------------------------------
    -- Check 2: ProductCategoryID NULL (FK missing)
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check ProductCategoryID NULL';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'subcategory',
        'CategoryID NULL',
        CAST(ProductSubcategoryID AS NVARCHAR(200)),
        'ProductCategoryID is NULL',
        'error'
    FROM bronze.erp_subcategory
    WHERE ProductCategoryID IS NULL;

    ---------------------------------------------------------
    -- Check 3: Duplicate ProductSubcategoryID
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check ProductSubcategoryID duplicated';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'subcategory',
        'Duplicate SubCategoryID',
        CAST(ProductSubcategoryID AS NVARCHAR(200)),
        'Duplicate ProductSubcategoryID detected',
        'error'
    FROM bronze.erp_subcategory
    GROUP BY ProductSubcategoryID
    HAVING COUNT(*) > 1;

    ---------------------------------------------------------
    -- Check 4: Subcategory Name NULL or empty
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check Subcategory Name NULL or empty';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'subcategory',
        'Name NULL or empty',
        CAST(ProductSubcategoryID AS NVARCHAR(200)),
        'Name is NULL or empty',
        'error'
    FROM bronze.erp_subcategory
    WHERE Name IS NULL
       OR LTRIM(RTRIM(Name)) = '';

    ---------------------------------------------------------
    -- Count errors
    ---------------------------------------------------------
    DECLARE @errors INT;

    SELECT @errors = COUNT(*)
    FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'subcategory'
      AND severity IN ('error', 'critical');

    PRINT '====================================================';
    PRINT 'Number of Errors: ' + CAST(@errors AS NVARCHAR);
    PRINT '====================================================';

    SET @batch_end_time = GETDATE();

    PRINT '========================================================================';
    PRINT 'Data Quality Subcategory Bronze Layer Completed in ' +
          CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
          ' milliseconds';
    PRINT '========================================================================';

    RETURN @errors;
END;
GO
