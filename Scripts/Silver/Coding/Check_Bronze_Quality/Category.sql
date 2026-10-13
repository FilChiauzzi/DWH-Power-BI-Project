/*
=========================================================
Bronze Data Quality Check - Category
=========================================================
Purpose:
    Validates the Bronze data that feeds the Silver category
    dimension and records the outcome in the quality framework.

The three steps, two tables:
    1. DETECT      evaluate the rules on bronze.erp_category
    2. LOG         every finding -> etl.data_quality_log
                   (append-only: it is the quality trend;
                   warnings included, they block nothing)
    3. QUARANTINE  the blocking findings -> etl.rejected
                   (row level, with the rule and the original
                   row serialized in JSON; this is what the load
                   procedure reads to exclude the row)

    Why two tables:
       etl.data_quality_log = "how good is the data, over time?"
       etl.rejected         = "which rows are waiting for a fix?"

Changes vs the original version:
    1. The DELETE of the previous logs is gone. Quality history
       is append-only and batch_id keeps the runs apart, so the
       trend is preserved instead of erased at every run.
    2. @batch_id BIGINT (required input): created ONCE by SSIS at
       the start of the run and passed to every procedure, so the
       findings and the loaded rows all share the same run id.
       The procedure never creates a run by itself: if the number
       is wrong or missing, it must fail, not guess.
    3. Blocking findings also go to etl.rejected with raw_row in
       JSON. In the original the check logged and stopped there:
       the load went ahead regardless of what was found.
    4. Severity now matches what the load really does:
         ProductCategoryID NULL       -> error   (blocking)
         Duplicate ProductCategoryID  -> error   (blocking)
         Name NULL or empty           -> warning (NOT blocking)
       The load substitutes 'Unknown', so the row is fine.
       A rule the load can repair is a warning, never an error.
    5. DATETIME2(3) / SYSDATETIME() instead of DATETIME / GETDATE().
    6. Threshold guard: if the rejection rate exceeds
       @max_reject_rate %, the procedure fails on purpose.
       Two bad rows are data; most of the table bad means a broken
       source. The findings are COMMITTED before the guard, so the
       evidence survives the alarm.
    7. No silent CATCH: rollback when a transaction is open, then
       re-throw, so SSIS sees the failure.
    8. No DDL and no self-registration: this script only defines
       the procedure (the tables live in 00_etl_ddl.sql).

SSIS Usage:
    EXEC etl.usp_check_bronze_quality_load_category ?, ?
        param 0: batch_id         (Input, from the SSIS variable)
        param 1: max_reject_rate  (Input, e.g. 20)
    Wire the precedence constraint "on success" to the load task.

Prerequisite: run 00_etl_ddl.sql first (etl.batch, etl.rejected,
etl.data_quality_log).
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
GO
