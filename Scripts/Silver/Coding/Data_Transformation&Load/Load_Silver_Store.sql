/*
=========================================================
Silver Load – Store (SCD1)
=========================================================
Purpose:
    Loads silver.erp_store from bronze.erp_store with SCD
    Type 1 logic: one row per business key, changes
    overwrite the previous values. It keeps the latest
    version of each store, replaces an empty Name with
    'Unknown' and uses a row_hash to detect changes. Rows
    quarantined in etl.rejected for the current batch are
    excluded, and a default member (surrogate key -1,
    'Unknown') is always kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction. INCREMENTAL reads only the
    Bronze rows loaded after the last Silver load
    (MAX(dwh_load_date) of the Silver table), then updates
    rows whose row_hash changed and inserts new business
    keys.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
    Incremental rows (CDC): Silver reads the Bronze rows not
    yet processed (dwh_batch_id > last_silver_batch_id in
    etl.config), only the latest version of each key (a key
    that arrived in several pending batches is loaded once,
    from its most recent batch) and never the deleted rows
    (dwh_operation = 'D'), which are handled separately.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object. SSIS passes
    the current @batch_id. The load type (full or
    incremental) is read from etl.config.

Warning:
    A FULL load truncates silver.erp_store and restarts its
    surrogate key sequence. Tables that store its surrogate
    keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_store]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time        DATETIME2(3),
        @end_time          DATETIME2(3),
        @load_ts           DATETIME2(3) = SYSDATETIME(),
        @load_type         VARCHAR(20),
        @last_silver_load  DATETIME2(3),
        @rows_src          INT = 0,
        @rows_updated      INT = 0,
        @rows_inserted     INT = 0;

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
            @object_name    = 'store',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type
        FROM etl.config
        WHERE object_name = 'store'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for store in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – STORE (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_store;

                ALTER SEQUENCE dwh.seq_silver_store RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
                -------------------------------------------------------
               IF NOT EXISTS (SELECT 1 FROM silver.erp_store WHERE store_sk = -1)
                INSERT INTO silver.erp_store ( 
                    store_sk, store_id, store_name, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                VALUES ( -1, -1, 'Unknown', NULL, @load_ts,@load_ts, -1);


                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS store_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE TRIM(b.[Name])
                    END AS store_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE TRIM(b.[Name])
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_store AS b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'store'
                      AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_store
                    (store_sk, store_id, store_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_store OVER (ORDER BY src.store_id),
                    src.store_id,
                    src.store_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full AS src;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_full;

            SET @end_time = SYSDATETIME();

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';

            -- Audit: FULL load completed
            EXEC etl.usp_audit_end
                @audit_id      = @audit_id,
                @status        = 'succeeded',
                @load_type     = 'full',
                @rows_read     = @rows_src,
                @rows_inserted = @rows_inserted;
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -- (empty Silver -> 1900, so everything is read)
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_store;

            -- Bronze -> Silver register: last batch processed with success
            DECLARE @last_silver_batch_id BIGINT =
                (SELECT ISNULL(last_silver_batch_id, 0)
                 FROM etl.config
                 WHERE object_name = 'store');

            PRINT '>> Silver register (last batch processed): ' + CAST(@last_silver_batch_id AS NVARCHAR(20));

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS store_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE TRIM(b.[Name])
                    END AS store_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE TRIM(b.[Name])
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_store AS b
                WHERE b.dwh_batch_id > @last_silver_batch_id          -- not yet processed by Silver
                  AND b.dwh_operation <> 'D'                         -- deletions handled separately
                  AND NOT EXISTS (                                      -- latest version of the key only
                        SELECT 1
                        FROM bronze.erp_store AS newer
                        WHERE newer.BusinessEntityID = b.BusinessEntityID
                          AND newer.dwh_batch_id > b.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected AS r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'store'
                          AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                    );

                SELECT @rows_src = COUNT(*) FROM #src_delta;

                PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                IF @rows_src = 0
                BEGIN
                    PRINT '>> No delta found. Nothing to do.';
                    DROP TABLE #src_delta;
                    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

                    SET @end_time = SYSDATETIME();
                    PRINT '>> INCREMENTAL load completed (no changes)';
                    PRINT '>> Duration: ' +
                          CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
                    PRINT '====================================================';
                    -- Audit: no delta, nothing to load
                    EXEC etl.usp_audit_end
                        @audit_id       = @audit_id,
                        @status         = 'succeeded',
                        @load_type      = @load_type,
                        @rows_read      = @rows_src,
                        @rows_inserted  = @rows_inserted,
                        @rows_updated   = @rows_updated,
                        @watermark_from = @last_silver_load;
                    RETURN;
                END

               BEGIN TRANSACTION;
                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.store_name     = src.store_name,
                    tgt.row_hash       = src.row_hash,
                    tgt.dwh_load_date  = @load_ts,
                    tgt.dwh_batch_id   = @batch_id
                FROM silver.erp_store AS tgt
                INNER JOIN #src_delta AS src
                    ON src.store_id = tgt.store_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_store
                    (store_sk, store_id, store_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_store OVER (ORDER BY src.store_id),
                    src.store_id,
                    src.store_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta AS src
                LEFT JOIN silver.erp_store AS tgt
                    ON tgt.store_id = src.store_id
                WHERE tgt.store_id IS NULL;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
        END

        ---------------------------------------------------------
        -- Unsupported load type
        ---------------------------------------------------------
        ELSE
        BEGIN
            THROW 51020, 'Unsupported load type for STORE.', 1;
        END

        -- Audit: INCREMENTAL load completed
        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @load_type,
            @rows_read      = @rows_src,
            @rows_inserted  = @rows_inserted,
            @rows_updated   = @rows_updated,
            @watermark_from = @last_silver_load;

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full') IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: store';
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
                @load_type = @load_type;

        THROW;
    END CATCH
END;
