/*
=========================================================
Silver Load – State Province (SCD1)
=========================================================
Purpose:
    Loads silver.erp_state_province from
    bronze.erp_state_province with SCD Type 1 logic: one row
    per StateProvinceID, changes overwrite the previous
    values. It replaces empty text with 'Unknown', resolves
    country_region_sk and territory_sk, and uses a row_hash
    to detect changes. Rows quarantined in etl.rejected for
    the current batch are excluded, and a default member
    (surrogate key -1, 'Unknown') is always kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction. INCREMENTAL reads only the
    Bronze rows loaded after the last Silver load
    (MAX(dwh_load_date) of the Silver table), then updates
    rows whose row_hash changed and inserts new business
    keys.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    silver.erp_country_region and
    silver.erp_sales_territory. SSIS passes the current
    @batch_id. The load type (full or incremental) is read
    from etl.config.

Warning:
    A FULL load truncates silver.erp_state_province and
    restarts its surrogate key sequence. Tables that store
    its surrogate keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_state_province]
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
            @object_name    = 'state_province',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        ---------------------------------------------------------
        -- Read configuration (load_type only)
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'state_province'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for STATE_PROVINCE.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – STATE_PROVINCE (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            -----------------------------------------------------
            -- Build FULL source (#src_full)
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.StateProvinceID AS state_province_id,
                    b.StateProvinceCode AS state_province_code,
                    b.CountryRegionCode AS country_region_code,
                    b.TerritoryID AS territory_id,
                    CASE
                        WHEN b.[Name] IS NULL OR TRIM(b.[Name]) = ''
                            THEN 'Unknown'
                        ELSE TRIM(b.[Name])
                    END AS name
                FROM bronze.erp_state_province b
                WHERE b.StateProvinceID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'state_province'
                          AND r.business_key = CAST(b.StateProvinceID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.state_province_id,
                cte.state_province_code,
                cte.country_region_code,
                cte.territory_id,
                cte.name,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.state_province_id AS VARCHAR(20)),
                        cte.state_province_code,
                        cte.country_region_code,
                        CAST(cte.territory_id AS VARCHAR(20)),
                        cte.name
                    )
                ) AS row_hash
            INTO #src_full
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
                THROW 51030, 'Bronze state_province is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_state_province;

                ALTER SEQUENCE dwh.seq_silver_state_province RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOWN MEMBER
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_state_province WHERE state_province_sk = -1)
                    INSERT INTO silver.erp_state_province (
                        state_province_sk, state_province_id, state_province_code,
                        country_region_code, country_region_sk,
                        name, territory_id, territory_sk,
                        row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown',
                        'Unknown', -1,
                        'Unknown', -1, -1,
                        NULL, @load_ts, @load_ts, -1
                    );

                -------------------------------------------------------
                -- INSERT ALL RECORDS + SURROGATE KEY
                -------------------------------------------------------
                INSERT INTO silver.erp_state_province
                    (state_province_sk, state_province_id, state_province_code,
                     country_region_code, country_region_sk,
                     name, territory_id, territory_sk,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_state_province OVER (ORDER BY src.state_province_id),
                    src.state_province_id,
                    src.state_province_code,
                    src.country_region_code,
                    cr.country_sk,
                    src.name,
                    src.territory_id,
                    tr.territory_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full src
                LEFT JOIN silver.erp_country_region cr
                    ON src.country_region_code = cr.country_code
                LEFT JOIN silver.erp_sales_territory tr
                    ON src.territory_id = tr.territory_id;

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
        -- INCREMENTAL LOAD (SCD1)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Last Silver load timestamp
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_state_province;

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

            -----------------------------------------------------
            -- Build DELTA (#src_delta)
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.StateProvinceID AS state_province_id,
                    b.StateProvinceCode AS state_province_code,
                    b.CountryRegionCode AS country_region_code,
                    b.TerritoryID AS territory_id,
                    CASE
                        WHEN b.[Name] IS NULL OR TRIM(b.[Name]) = ''
                            THEN 'Unknown'
                        ELSE TRIM(b.[Name])
                    END AS name
                FROM bronze.erp_state_province b
                WHERE b.dwh_load_date > @last_silver_load
                  AND b.StateProvinceID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'state_province'
                          AND r.business_key = CAST(b.StateProvinceID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.state_province_id,
                cte.state_province_code,
                cte.country_region_code,
                cte.territory_id,
                cte.name,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.state_province_id AS VARCHAR(20)),
                        cte.state_province_code,
                        cte.country_region_code,
                        CAST(cte.territory_id AS VARCHAR(20)),
                        cte.name
                    )
                ) AS row_hash
            INTO #src_delta
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_delta;

            PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
            BEGIN
                PRINT '>> No delta found. Nothing to do.';
                DROP TABLE #src_delta;
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

            -----------------------------------------------------
            -- UPDATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                -----------------------------------------------------
                -- UPDATE changed rows
                -----------------------------------------------------
                UPDATE tgt
                SET tgt.state_province_code = src.state_province_code,
                    tgt.country_region_code = src.country_region_code,
                    tgt.country_region_sk   = cr.country_sk,
                    tgt.name                = src.name,
                    tgt.territory_id        = src.territory_id,
                    tgt.territory_sk        = tr.territory_sk,
                    tgt.row_hash            = src.row_hash,
                    tgt.dwh_load_date       = @load_ts,
                    tgt.dwh_batch_id        = @batch_id
                FROM silver.erp_state_province tgt
                INNER JOIN #src_delta src
                    ON src.state_province_id = tgt.state_province_id
                LEFT JOIN silver.erp_country_region cr
                    ON src.country_region_code = cr.country_code
                LEFT JOIN silver.erp_sales_territory tr
                    ON src.territory_id = tr.territory_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                -----------------------------------------------------
                -- INSERT new business keys
                -----------------------------------------------------
                INSERT INTO silver.erp_state_province
                    (state_province_sk, state_province_id, state_province_code,
                     country_region_code, country_region_sk,
                     name, territory_id, territory_sk,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_state_province OVER (ORDER BY src.state_province_id),
                    src.state_province_id,
                    src.state_province_code,
                    src.country_region_code,
                    cr.country_sk,
                    src.name,
                    src.territory_id,
                    tr.territory_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_country_region cr
                    ON src.country_region_code = cr.country_code
                LEFT JOIN silver.erp_sales_territory tr
                    ON src.territory_id = tr.territory_id
                LEFT JOIN silver.erp_state_province tgt
                    ON tgt.state_province_id = src.state_province_id
                WHERE tgt.state_province_id IS NULL;

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
            THROW 51020, 'Unsupported load type for STATE_PROVINCE.', 1;
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

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: state_province';
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
GO
