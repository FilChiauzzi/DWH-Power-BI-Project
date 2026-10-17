/*
=========================================================
Silver Load – country_region (SCD Type 1)
=========================================================
Purpose:
    Loads the COUNTRY REGION lookup table into the Silver layer
    using SCD Type 1 logic. Only one version per business key
    (CountryRegionCode) is kept. Changes overwrite previous values.

Load types:
    FULL:
        - TRUNCATE Silver table
        - RESET surrogate key sequence
        - INSERT all valid rows from Bronze (excluding quarantined)
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        - SELECT only rows with ModifiedDate > last_watermark_value
        - UPDATE existing rows when row_hash changes
        - INSERT new business keys
        - UPDATE watermark
      Idempotent: re-running the same batch produces no changes.

Source rules (from Bronze DQ):
    Bronze already guarantees:
        * CountryRegionCode not NULL
        * CountryRegionCode unique
        * Name may be NULL/empty → warning only
        * ModifiedDate may be NULL → allowed
        * Quarantine of invalid rows into etl.rejected

Silver transformations:
    * TRIM normalization of Name
    * Name NULL/empty → substituted with 'Unknown'
    * row_hash (SHA2-256) used for change detection
    * Exclusion of quarantined rows
    * Surrogate key generated via dwh.seq_silver_country_region

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
    4. Print counters and duration

Why SCD1:
    CountryRegion is a stable lookup table (ISO codes).
    Historical tracking is not required. Only the latest version
    of each country/region is kept.

Deliberately not done:
    - No delete of missing keys (SCD1 never deletes)
    - No ROW_NUMBER() or dwh_source_row_num
      (Bronze DQ already ensures one valid row per business key)
    - No business logic already owned by Bronze DQ

SSIS Usage:
    EXEC etl.usp_load_silver_country_region ?
        param 0: batch_id (same variable used by Bronze DQ)

Prerequisite:
    Run 00_etl_ddl.sql first (etl.batch, etl.rejected,
    etl.data_quality_log, sequences, Silver tables).
*/

CREATE OR ALTER   PROCEDURE [etl].[usp_load_silver_country_region]
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

    BEGIN TRY
        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type
        FROM etl.config
        WHERE object_name = 'country_region'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for COUNTRY_REGION.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – COUNTRY REGION (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.CountryRegionCode AS country_code,
                    CASE
                        WHEN b.Name IS NULL OR TRIM(b.Name) = ''
                            THEN 'Unknown'
                        ELSE TRIM(b.Name)
                    END AS country_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.CountryRegionCode,
                            CASE
                                WHEN b.Name IS NULL OR TRIM(b.Name) = ''
                                    THEN 'Unknown'
                                ELSE TRIM(b.Name)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_country_region b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'country_region'
                      AND r.business_key = CAST(b.CountryRegionCode AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_country_region;

                ALTER SEQUENCE dwh.seq_silver_country_region RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_country_region WHERE country_sk = -1)
                    INSERT INTO silver.erp_country_region (
                        country_sk, country_code, country_name,
                        row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, '-1', 'Unknown',
                        NULL, @load_ts, @load_ts, -1
                    );

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_country_region
                    (country_sk, country_code, country_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_country_region OVER (ORDER BY src.country_code),
                    src.country_code,
                    src.country_name,
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
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -- (empty Silver -> 1900, so everything is read)
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_country_region;

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.CountryRegionCode AS country_code,
                    CASE
                        WHEN b.Name IS NULL OR TRIM(b.Name) = ''
                            THEN 'Unknown'
                        ELSE TRIM(b.Name)
                    END AS country_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.CountryRegionCode,
                            CASE
                                WHEN b.Name IS NULL OR TRIM(b.Name) = ''
                                    THEN 'Unknown'
                                ELSE TRIM(b.Name)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_country_region b
                WHERE b.dwh_load_date > @last_silver_load
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'country_region'
                          AND r.business_key = CAST(b.CountryRegionCode AS NVARCHAR(200))
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

                BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.country_name   = src.country_name,
                    tgt.row_hash       = src.row_hash,
                    tgt.dwh_load_date  = @load_ts,
                    tgt.dwh_batch_id   = @batch_id
                FROM silver.erp_country_region tgt
                INNER JOIN #src_delta src
                    ON src.country_code = tgt.country_code
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_country_region
                    (country_sk, country_code, country_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_country_region OVER (ORDER BY src.country_code),
                    src.country_code,
                    src.country_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_country_region tgt
                    ON tgt.country_code = src.country_code
                WHERE tgt.country_code IS NULL;

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
            THROW 51020, 'Unsupported load type for COUNTRY_REGION.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: country_region';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
