/*
=========================================================
Bronze Data Quality Check – Person
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Person domain.
    It checks for null keys, duplicate keys, and invalid
    first/last names.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    pipeline. SSIS loops through all active objects and calls
    the corresponding Bronze Data Quality procedure for each
    imported source table. Each procedure logs issues into
    etl.data_quality_log and returns the number of detected
    errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Person object in the Bronze layer.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_person]
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
      AND object_name = 'person';

    ---------------------------------------------------------
    -- Check 1: BusinessEntityID NULL
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check BusinessEntityID NULL';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'person',
        'BusinessEntityID NULL',
        CAST(BusinessEntityID AS NVARCHAR(200)),
        'BusinessEntityID is NULL',
        'error'
    FROM bronze.erp_person
    WHERE BusinessEntityID IS NULL;

    ---------------------------------------------------------
    -- Check 2: Duplicate BusinessEntityID
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check BusinessEntityID duplicated';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'person',
        'Duplicate BusinessEntityID',
        CAST(BusinessEntityID AS NVARCHAR(200)),
        'Duplicate BusinessEntityID detected',
        'error'
    FROM bronze.erp_person
    GROUP BY BusinessEntityID
    HAVING COUNT(*) > 1;

    ---------------------------------------------------------
    -- Check 3: FirstName NULL or empty
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check FirstName NULL or empty';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'person',
        'FirstName NULL or empty',
        CAST(BusinessEntityID AS NVARCHAR(200)),
        'FirstName is NULL or empty',
        'error'
    FROM bronze.erp_person
    WHERE FirstName IS NULL
       OR LTRIM(RTRIM(FirstName)) = '';

    ---------------------------------------------------------
    -- Check 4: LastName NULL or empty
    ---------------------------------------------------------
    PRINT '====================================================';
    PRINT 'Check LastName NULL or empty';
    PRINT '====================================================';

    INSERT INTO etl.data_quality_log (layer, object_name, check_name, record_id, description, severity)
    SELECT
        'bronze',
        'person',
        'LastName NULL or empty',
        CAST(BusinessEntityID AS NVARCHAR(200)),
        'LastName is NULL or empty',
        'error'
    FROM bronze.erp_person
    WHERE LastName IS NULL
       OR LTRIM(RTRIM(LastName)) = '';

    ---------------------------------------------------------
    -- Count errors
    ---------------------------------------------------------
    DECLARE @errors INT;

    SELECT @errors = COUNT(*)
    FROM etl.data_quality_log
    WHERE layer = 'bronze'
      AND object_name = 'person'
      AND severity IN ('error', 'critical');

    PRINT '====================================================';
    PRINT 'Number of Errors: ' + CAST(@errors AS NVARCHAR);
    PRINT '====================================================';

    SET @batch_end_time = GETDATE();

    PRINT '========================================================================';
    PRINT 'Data Quality Person Bronze Layer Completed in ' +
          CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
          ' milliseconds';
    PRINT '========================================================================';

    RETURN @errors;
END;
GO
