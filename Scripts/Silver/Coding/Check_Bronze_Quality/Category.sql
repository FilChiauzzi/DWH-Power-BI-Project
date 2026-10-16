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

ALTER PROCEDURE [etl].[usp_load_silver_category]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @start_time DATETIME2(3) = SYSDATETIME(),
        @end_time   DATETIME2(3),
        @load_ts    DATETIME2(3) = SYSDATETIME(),
        @rows_src   INT = 0,
        @rows_ins   INT = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Silver Load – CATEGORY (FULL ONLY) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Source set (after quarantine)
        ---------------------------------------------------------
        SELECT
            b.ProductCategoryID AS category_id,

            CASE
                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(b.[Name]))
            END AS category_name,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(b.ProductCategoryID AS VARCHAR(20)),
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END
                )
            ) AS row_hash
        INTO #src
        FROM bronze.erp_category b
        WHERE NOT EXISTS (
            SELECT 1
            FROM etl.rejected r
            WHERE r.batch_id    = @batch_id
              AND r.layer        = 'bronze'
              AND r.object_name  = 'category'
              AND r.business_key = CAST(b.ProductCategoryID AS NVARCHAR(200))
        );

        SELECT @rows_src = COUNT(*) FROM #src;

        PRINT '>> Source rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD ONLY
        ---------------------------------------------------------
        PRINT '>> FULL load: truncate + reset sequence + reload';

        BEGIN TRANSACTION;

            TRUNCATE TABLE silver.erp_category;

            ALTER SEQUENCE dwh.seq_silver_category RESTART WITH 1;

            INSERT INTO silver.erp_category
                (category_sk, category_id, category_name,
                 row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_category OVER (ORDER BY src.category_id),
                src.category_id,
                src.category_name,
                src.row_hash,
                @load_ts,
                @load_ts,
                @batch_id
            FROM #src AS src;

            SET @rows_ins = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE #src;

        SET @end_time = SYSDATETIME();

        PRINT '>> FULL load completed';
        PRINT '>> Rows inserted: ' + CAST(@rows_ins AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: category';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
