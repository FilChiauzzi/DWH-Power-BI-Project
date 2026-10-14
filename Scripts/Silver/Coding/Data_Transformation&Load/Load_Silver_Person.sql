/*
=========================================================
Silver Load – Person (SCD Type 1)
=========================================================
Purpose:
    Loads the Person dimension into Silver with SCD1 logic:
    one row per business key, attributes overwritten in place.

Load types:
    FULL:
        - TRUNCATE Silver
        - RESET surrogate key sequence
        - INSERT the entire cleaned + quarantined source
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        - UPDATE changed rows (row_hash comparison)
        - INSERT new business keys
      Idempotent: a second run makes no further changes.

Source rules (same hardening as Bronze DQ):
    - Bronze already guarantees:
         * BusinessEntityID not NULL
         * No duplicates
         * Standardized names (NULL/empty → warning)
         * Quarantine of invalid rows
    - Silver applies:
         * TRIM normalization
         * row_hash (SHA2-256) for change detection
         * Exclusion of quarantined rows

Flow:
    1. Read load_type and last_watermark_value from etl.config
    2. FULL:
         - Build #src_full (clean + quarantine gate)
         - TRUNCATE + RESET SEQUENCE + INSERT all rows
    3. INCREMENTAL:
         - Build #src_delta (ModifiedDate > watermark)
         - UPDATE changed rows (row_hash comparison)
         - INSERT new business keys
         - Update watermark
    4. Report counters

Why SCD1:
    Person is a stable master entity. Changes overwrite previous
    values instead of generating historical versions. Only the
    latest version of each person is kept.

Deliberately not done:
    - No delete of missing keys (SCD1 never deletes)
    - No business logic already owned by the DQ check
    - No use of ROW_NUMBER() or dwh_source_row_num
      (Bronze DQ already ensures one valid row per business key)

SSIS Usage:
    EXEC etl.usp_load_silver_person ?
        param 0: batch_id (same variable used by Bronze DQ)

Prerequisite:
    Run 00_etl_ddl.sql first (etl.batch, etl.rejected,
    etl.data_quality_log, sequences, Silver tables).
*/


USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_load_silver_person]
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
        WHERE object_name = 'person'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for PERSON.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – PERSON (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_person;

                ALTER SEQUENCE dwh.seq_silver_person RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS person_id,
                    CASE
                        WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                            THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                        ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                    END AS person_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                                    THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                                ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_person b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'person'
                      AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_person
                    (person_sk, person_id, person_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.person_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full src;

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
        -- INCREMENTAL LOAD (SCD1)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            PRINT '>> INCREMENTAL load (SCD1) started';

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS person_id,
                    CASE
                        WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                            THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                        ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                    END AS person_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                                    THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                                ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_person b
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'person'
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
                SET tgt.person_name    = src.person_name,
                    tgt.row_hash       = src.row_hash,
                    tgt.dwh_load_date  = @load_ts,
                    tgt.dwh_batch_id   = @batch_id
                FROM silver.erp_person tgt
                INNER JOIN #src_delta src
                    ON src.person_id = tgt.person_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_person
                    (person_sk, person_id, person_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.person_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_person tgt
                    WHERE tgt.person_id = src.person_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'person'
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
            THROW 51020, 'Unsupported load type for PERSON.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: person';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO

