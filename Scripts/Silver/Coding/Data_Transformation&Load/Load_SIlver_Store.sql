/*
=========================================================
Silver Load – Store (SCD Type 1)
=========================================================
Purpose:
    Loads the Store dimension into Silver with SCD1 logic:
    one row per business key, attributes overwritten in place.

Load types:
    FULL:
        - TRUNCATE Silver
        - RESET surrogate key sequence
        - INSERT the entire deduplicated + quarantined source
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        - Read delta rows using ModifiedDate > last_watermark_value
        - UPDATE changed rows (SK preserved)
        - INSERT new business keys
        - Update last_watermark_value in etl.config
      Idempotent: a second run makes no further changes.

Source rules (same hardening as Bronze DQ):
    - Dedup: latest version per business key
    - Standardize Name (NULL/empty → 'Unknown')
    - Exclude rows rejected for THIS batch
    - Compute row_hash (SHA2_256) for change detection

Flow:
    1. Read load_type and last_watermark_value from etl.config
    2. FULL:
         - Build #src_full (dedup + standardize + quarantine gate)
         - TRUNCATE + RESET SEQUENCE + INSERT all rows
    3. INCREMENTAL:
         - Build #src_delta (delta + quarantine gate)
         - UPDATE changed rows (row_hash comparison)
         - INSERT new business keys
         - Update watermark
    4. Report counters

Deliberately not done:
    - No business logic already owned by the DQ check
    - No delete of missing keys (SCD1 never deletes)
    - Unknown member (SK = -1) is always preserved

Why SCD1:
    Store is a stable master entity. Changes overwrite previous
    values instead of generating historical versions. Only the
    latest version of each store is kept.

SSIS Usage:
    EXEC etl.usp_load_silver_store ?
        param 0: batch_id (same variable used by Bronze DQ)
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_load_silver_store]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @load_ts          DATETIME2(3) = SYSDATETIME(),
        @load_type        VARCHAR(20),
        @last_wm          DATETIME,
        @new_wm           DATETIME,
        @rows_src         INT = 0,
        @rows_updated     INT = 0,
        @rows_inserted    INT = 0;

    BEGIN TRY
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @last_wm   = last_watermark_value
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

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS store_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS store_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.[Name]))
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

            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            PRINT '>> INCREMENTAL load (SCD1) started';

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS store_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS store_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.[Name]))
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_store AS b
                WHERE b.ModifiedDate > @last_wm
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
                    ROLLBACK TRANSACTION;

                    SET @end_time = SYSDATETIME();
                    PRINT '>> INCREMENTAL load completed (no changes)';
                    PRINT '>> Duration: ' +
                          CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
                    PRINT '====================================================';
                    RETURN;
                END

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
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_store AS tgt
                    WHERE tgt.store_id = src.store_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'store'
                  AND is_active = 1;

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

        THROW;
    END CATCH
END;
GO
