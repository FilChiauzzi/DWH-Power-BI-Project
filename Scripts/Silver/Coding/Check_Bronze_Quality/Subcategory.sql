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
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start_time DATETIME2(3) = SYSDATETIME(),
            @rows_checked     INT           = 0,
            @rows_rejected    INT           = 0,
            @errors           INT           = 0,
            @reject_rate      DECIMAL(9,4)  = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - subcategory | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_subcategory;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'subcategory', 'Source empty', NULL,
                     'bronze.erp_subcategory contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: ProductSubcategoryID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: ProductSubcategoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'subcategory',
                   'SubCategoryID NULL',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'ProductSubcategoryID is NULL',
                   'error'
            FROM bronze.erp_subcategory AS b
            WHERE b.ProductSubcategoryID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'subcategory',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'NULL_BK',
                   'Business key ProductSubcategoryID is NULL',
                   'error',
                   JSON_OBJECT('ProductSubcategoryID': b.ProductSubcategoryID,
                               'ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_subcategory AS b
            WHERE b.ProductSubcategoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: ProductCategoryID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 2: ProductCategoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'subcategory',
                   'CategoryID NULL',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'ProductCategoryID is NULL',
                   'error'
            FROM bronze.erp_subcategory AS b
            WHERE b.ProductCategoryID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'subcategory',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'NULL_FK',
                   'Foreign key ProductCategoryID is NULL',
                   'error',
                   JSON_OBJECT('ProductSubcategoryID': b.ProductSubcategoryID,
                               'ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_subcategory AS b
            WHERE b.ProductCategoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 3: Duplicate ProductSubcategoryID -> error
            ---------------------------------------------------------
            PRINT '-- Rule 3: Duplicate ProductSubcategoryID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'subcategory',
                   'Duplicate SubCategoryID',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'Duplicate ProductSubcategoryID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_subcategory AS b
            GROUP BY b.ProductSubcategoryID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'subcategory',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate ProductSubcategoryID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT('ProductSubcategoryID': b.ProductSubcategoryID,
                               'ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_subcategory AS b
            INNER JOIN (
                SELECT ProductSubcategoryID, COUNT(*) AS cnt
                FROM bronze.erp_subcategory
                GROUP BY ProductSubcategoryID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.ProductSubcategoryID = b.ProductSubcategoryID;

            ---------------------------------------------------------
            -- RULE 4: Name NULL or empty -> warning (NOT blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 4: Name NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'subcategory',
                   'Name NULL or empty',
                   CAST(b.ProductSubcategoryID AS NVARCHAR(200)),
                   'Name is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_subcategory AS b
            WHERE b.[Name] IS NULL
               OR LTRIM(RTRIM(b.[Name])) = '';

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'subcategory'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'subcategory';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, SYSDATETIME()) AS NVARCHAR(20)) + ' ms';

        ---------------------------------------------------------
        -- Threshold guard
        ---------------------------------------------------------
        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - subcategory completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: subcategory';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
