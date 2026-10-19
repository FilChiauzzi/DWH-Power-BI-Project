/*
=========================================================
Bronze Data Quality Check – Category
=========================================================
Purpose:
    Validates bronze.erp_category before the Silver load.
    ProductCategoryID NULL or duplicated is a blocking
    error; Name NULL or empty is a warning (Silver uses
    'Unknown').

    Blocking errors quarantine the row; non-blocking
    warnings are only logged.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline,
    before the Silver load of the same object. SSIS passes
    @batch_id and @max_reject_rate (default 20). Every
    finding is logged in etl.data_quality_log; blocking
    errors are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_category]
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
        PRINT 'Bronze Data Quality - category | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_category;

            ---------------------------------------------------------
            -- RULE 0: source empty  ->  warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'category', 'Source empty', NULL,
                     'bronze.erp_category contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: business key NULL  ->  error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: ProductCategoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'ProductCategoryID NULL',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Business key ProductCategoryID is NULL',
                   'error'
            FROM bronze.erp_category AS b
            WHERE b.ProductCategoryID IS NULL;

            -- Quarantine
            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'category',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'NULL_BK',
                   'Business key ProductCategoryID is NULL',
                   'error',
                   JSON_OBJECT('ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_category AS b
            WHERE b.ProductCategoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: duplicated business key  ->  error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 2: duplicated ProductCategoryID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'Duplicate ProductCategoryID',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Business key duplicated: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_category AS b
            GROUP BY b.ProductCategoryID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'category',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Business key duplicated: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT('ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_category AS b
            INNER JOIN (
                SELECT ProductCategoryID, COUNT(*) AS cnt
                FROM bronze.erp_category
                GROUP BY ProductCategoryID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.ProductCategoryID = b.ProductCategoryID;

            ---------------------------------------------------------
            -- RULE 3: name NULL or empty  ->  warning
            ---------------------------------------------------------
            PRINT '-- Rule 3: name NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'Name NULL or empty',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Name is NULL or empty - the load substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_category AS b
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
          AND object_name = 'category'
          AND severity IN ('error', 'critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'category';

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
            THROW 51010, 'Rejection rate above the allowed threshold: the source looks broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - category completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: category';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
