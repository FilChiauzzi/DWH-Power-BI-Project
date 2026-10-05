/*
=========================================================
Bronze Data Quality Check – Category
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Category domain.
    It checks for null keys, duplicate keys, and invalid names.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    pipeline. SSIS loops through all active objects and calls
    the corresponding Bronze Data Quality procedure for each
    imported source table. Each procedure logs issues into
    etl.data_quality_log and returns the number of detected
    errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Category object in the Bronze layer.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_category]
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
      AND object_name = 'category';

    ---------------------------------------------------------
    -- Check 1: ProductCategoryID NULL
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check ProductCategoryID NULL';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'category',
        'ProductCategoryID NULL',
        CAST(ProductCategoryID AS NVARCHAR(200)),
        'ProductCategoryID is NULL',
        'error'
    FROM bronze.erp_category
    WHERE ProductCategoryID IS NULL;

    ---------------------------------------------------------
    -- Check 2: Duplicate ProductCategoryID
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check ProductCategoryID duplicated';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'category',
        'Duplicate CategoryID',
        CAST(ProductCategoryID AS NVARCHAR(200)),
        'Duplicate ProductCategoryID detected',
        'error'
    FROM bronze.erp_category
    GROUP BY ProductCategoryID
    HAVING COUNT(*) > 1;

    ---------------------------------------------------------
    -- Check 3: Category Name NULL or empty
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check Category Name NULL or empty';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'category',
        'Name NULL or empty',
        CAST(ProductCategoryID AS NVARCHAR(200)),
        'Name is NULL or empty',
        'error'
    FROM bronze.erp_category
    WHERE Name IS NULL
       OR LTRIM(RTRIM(Name)) = '';

    ---------------------------------------------------------
    -- Count errors
    ---------------------------------------------------------
    DECLARE @errors INT;

    SELECT @errors = COUNT(*)
    FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'category'
      AND severity IN ('error', 'critical');

    PRINT '====================================================';
    PRINT 'Number of Errors: ' + CAST(@errors AS NVARCHAR);
    PRINT '====================================================';

    SET @batch_end_time = GETDATE();

    PRINT '========================================================================';
    PRINT 'Data Quality Category Bronze Layer Completed in ' +
          CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
          ' milliseconds';
    PRINT '========================================================================';

    RETURN @errors;
END;
GO
