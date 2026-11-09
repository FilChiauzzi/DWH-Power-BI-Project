/*
=========================================================
Silver Load – Category (SCD1)
=========================================================
Purpose:
    Loads silver.erp_category from bronze.erp_category with
    SCD Type 1 logic: one row per business key, changes
    overwrite the previous values. It keeps the latest
    version of each key, replaces an empty Name with
    'Unknown' and uses a row_hash to detect changes. Rows
    quarantined in etl.rejected for the current batch are
    excluded, and a default member (surrogate key -1,
    'Unknown') is always kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction. INCREMENTAL updates the
    changed rows and inserts the new keys.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object. SSIS passes
    the current @batch_id. The load type (full or
    incremental) is read from etl.config.

Warning:
    A FULL load truncates silver.erp_category and restarts
    its surrogate key sequence. Tables that store its
    surrogate keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_category]
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

    -- Audit (etl.load_audit): declared before TRY, the CATCH uses them
    DECLARE
        @audit_id   BIGINT,
        @proc_name  NVARCHAR(256) =
            QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));

    BEGIN TRY
        ---------------------------------------------------------
        -- Audit: open the step (outside any transaction)
        ---------------------------------------------------------
        EXEC etl.usp_audit_start
            @batch_id       = @batch_id,
            @layer          = 'silver',
            @object_name    = 'category',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

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
                ELSE TRIM(b.[Name])
            END AS category_name,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(b.ProductCategoryID AS VARCHAR(20)),
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                            THEN 'Unknown'
                        ELSE TRIM(b.[Name])
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

            -------------------------------------------------------
            -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
            -------------------------------------------------------
            IF NOT EXISTS (SELECT 1 FROM silver.erp_category WHERE category_sk = -1)
                INSERT INTO silver.erp_category (
                    category_sk, category_id, category_name,
                    row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                )
                VALUES (
                    -1, -1, 'Unknown',
                    NULL,@load_ts, @load_ts, -1
                );

             -------------------------------------------------------
             -- INSERT ALL RECORDS + SURROGATE KEY
             -------------------------------------------------------

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

        -- Audit: load completed
        EXEC etl.usp_audit_end
            @audit_id      = @audit_id,
            @status        = 'succeeded',
            @load_type     = 'full',
            @rows_read     = @rows_src,
            @rows_inserted = @rows_ins;

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

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id  = @audit_id,
                @status    = 'failed',
                @load_type = 'full';

        THROW;
    END CATCH
END;
