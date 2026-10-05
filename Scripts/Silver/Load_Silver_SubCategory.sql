/*
=========================================================
Silver Load – Subcategory
=========================================================
Purpose:
    This stored procedure loads the Silver-layer table
    for the Subcategory domain. It applies basic cleaning
    rules, removes duplicates using ModifiedDate ordering,
    resets the surrogate key sequence, and inserts the
    refined data into silver.erp_subcategory.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Silver Load pipeline. The Bronze layer must be validated
    before this step. The procedure truncates the Silver
    table and reloads it entirely.

Notes:
    - Surrogate keys (subcategory_sk) are generated using
      dwh.seq_silver_subcategory.
    - Only the most recent record per ProductSubcategoryID
      is selected (ROW_NUMBER logic).
    - category_sk is resolved by joining Silver Category.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_subcategory]
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Silver Load: Subcategory';
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Truncate Silver table
        ---------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_subcategory';
        TRUNCATE TABLE silver.erp_subcategory;

        ---------------------------------------------------------
        -- Reset surrogate key sequence
        ---------------------------------------------------------
        PRINT '>> Resetting sequence: dwh.seq_silver_subcategory';
        ALTER SEQUENCE dwh.seq_silver_subcategory
            RESTART WITH 1;

        ---------------------------------------------------------
        -- Insert cleaned and deduplicated data
        ---------------------------------------------------------
        PRINT '>> Inserting data into: silver.erp_subcategory';

        WITH clean_subcategory AS (
            SELECT
                ProductSubcategoryID AS subcategory_id,
                CASE 
                    WHEN Name IS NULL THEN 'Unknown'
                    WHEN LTRIM(RTRIM(Name)) = '' THEN 'Unknown'
                    ELSE LTRIM(RTRIM(Name))
                END AS subcategory_name,
                ROW_NUMBER() OVER (
                    PARTITION BY ProductSubcategoryID
                    ORDER BY ModifiedDate DESC
                ) AS flag_unique,
                ProductCategoryID
            FROM bronze.erp_subcategory
        )
        INSERT INTO silver.erp_subcategory (
            subcategory_sk,
            subcategory_id,
            subcategory_name,
            category_sk
        )
        SELECT
            NEXT VALUE FOR dwh.seq_silver_subcategory OVER (ORDER BY subcategory_id),
            sc.subcategory_id,
            sc.subcategory_name,
            tc.category_sk
        FROM clean_subcategory sc
        LEFT JOIN silver.erp_category tc
            ON sc.ProductCategoryID = tc.category_id
        WHERE sc.flag_unique = 1
          AND sc.subcategory_id IS NOT NULL;

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
        PRINT 'Silver Load Subcategory Completed in ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
              ' milliseconds';
        PRINT '====================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING SILVER SUBCATEGORY LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
GO
