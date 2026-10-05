/*
=========================================================
Silver Load – Category
=========================================================
Purpose:
    This stored procedure loads the Silver-layer table
    for the Category domain. It applies basic cleaning
    rules, removes duplicates using ModifiedDate ordering,
    resets the surrogate key sequence, and inserts the
    refined data into silver.erp_category.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Silver Load pipeline. The Bronze layer must be validated
    before this step. The procedure truncates the Silver
    table and reloads it entirely.

Notes:
    - Surrogate keys (category_sk) are generated using
      dwh.seq_silver_category.
    - Only the most recent record per ProductCategoryID
      is selected (ROW_NUMBER logic).
    - SCD type 1
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_category]
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Silver Load: Category';
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Truncate Silver table
        ---------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_category';
        TRUNCATE TABLE silver.erp_category;

        ---------------------------------------------------------
        -- Reset surrogate key sequence
        ---------------------------------------------------------
        PRINT '>> Resetting sequence: dwh.seq_silver_category';
        ALTER SEQUENCE dwh.seq_silver_category
            RESTART WITH 1;

        ---------------------------------------------------------
        -- Insert cleaned and deduplicated data
        ---------------------------------------------------------
        PRINT '>> Inserting data into: silver.erp_category';

        WITH clean_category AS (
            SELECT
                ProductCategoryID AS category_id,
                CASE 
                    WHEN Name IS NULL THEN 'Unknown'
                    WHEN LTRIM(RTRIM(Name)) = '' THEN 'Unknown'
                    ELSE LTRIM(RTRIM(Name))
                END AS category_name,
                ROW_NUMBER() OVER (
                    PARTITION BY ProductCategoryID
                    ORDER BY ModifiedDate DESC
                ) AS flag_unique
            FROM bronze.erp_category
        )
        INSERT INTO silver.erp_category (
            category_sk,
            category_id,
            category_name
        )
        SELECT
            NEXT VALUE FOR dwh.seq_silver_category OVER (ORDER BY category_id),
            category_id,
            category_name
        FROM clean_category
        WHERE flag_unique = 1
          AND category_id IS NOT NULL;

        ---------------------------------------------------------
        -- Logging duration
        ---------------------------------------------------------
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) +
              ' milliseconds';
        PRINT '>> ----------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '====================================================';
        PRINT 'Silver Load Category Completed in ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
              ' milliseconds';
        PRINT '====================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING SILVER CATEGORY LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
GO
