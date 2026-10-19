/*
=========================================================
Silver Load – Person Address (SCD1)
=========================================================
Purpose:
    Loads silver.erp_person_address from
    bronze.erp_person_address with SCD Type 1 logic: one row
    per AddressID, changes overwrite the previous values. It
    trims text, replaces empty address line 1, city and
    postal code with 'Unknown', reads latitude and longitude
    from the spatial column, resolves state_province_sk and
    uses a row_hash to detect changes. Rows quarantined in
    etl.rejected for the current batch are excluded, and a
    default member (surrogate key -1, 'Unknown') is always
    kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction (it stops if Bronze is
    empty). INCREMENTAL reads only the Bronze rows loaded
    after the last Silver load (MAX(dwh_load_date) of the
    Silver table), then updates rows whose row_hash changed
    and inserts new business keys.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    silver.erp_state_province. SSIS passes the current
    @batch_id. The load type (full or incremental) is read
    from etl.config.

Warning:
    A FULL load truncates silver.erp_person_address and
    restarts its surrogate key sequence. Tables that store
    its surrogate keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_person_address]
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
        -- Read configuration (load_type only)
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'person_address'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for PERSON_ADDRESS.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – PERSON_ADDRESS (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                    b.AddressID AS address_id,

                    CASE WHEN TRIM(b.AddressLine1) = '' OR b.AddressLine1 IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.AddressLine1)
                    END AS address_line1,

                    CASE WHEN TRIM(b.AddressLine2) = '' OR b.AddressLine2 IS NULL
                        THEN NULL
                        ELSE TRIM(b.AddressLine2)
                    END AS address_line2,

                    CASE WHEN TRIM(b.City) = '' OR b.City IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.City)
                    END AS city,

                    b.StateProvinceID AS state_province_id,

                    CASE WHEN TRIM(b.PostalCode) = '' OR b.PostalCode IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.PostalCode)
                    END AS postal_code,

                    b.SpatialLocation.Lat  AS latitude,
                    b.SpatialLocation.Long AS longitude
                FROM bronze.erp_person_address b
                WHERE b.AddressID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'person_address'
                          AND r.business_key = CAST(b.AddressID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.address_id,
                cte.address_line1,
                cte.address_line2,
                cte.city,
                cte.state_province_id,
                cte.postal_code,
                cte.latitude,
                cte.longitude,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.address_id AS VARCHAR(20)),
                        cte.address_line1,
                        ISNULL(cte.address_line2, ''),
                        cte.city,
                        CAST(ISNULL(cte.state_province_id, -1) AS VARCHAR(20)),
                        cte.postal_code,
                        CAST(ISNULL(cte.latitude, 0.0) AS VARCHAR(50)),
                        CAST(ISNULL(cte.longitude, 0.0) AS VARCHAR(50))
                    )
                ) AS row_hash
            INTO #src_full
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
                THROW 51030, 'Bronze person_address is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_person_address;

                ALTER SEQUENCE dwh.seq_silver_person_address RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOWN MEMBER
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_person_address WHERE address_sk = -1)
                    INSERT INTO silver.erp_person_address (
                        address_sk, address_id,
                        address_line1, address_line2, city,
                        state_province_id, state_province_sk, postal_code,
                        latitude, longitude,
                        row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1,
                        'Unknown', NULL, 'Unknown',
                        -1, -1, 'Unknown',
                        0.0, 0.0,
                        NULL, @load_ts, @load_ts, -1
                    );

                -------------------------------------------------------
                -- INSERT ALL RECORDS + SURROGATE KEY
                -------------------------------------------------------
                INSERT INTO silver.erp_person_address
                    (address_sk, address_id,
                     address_line1, address_line2, city,
                     state_province_id, state_province_sk, postal_code,
                     latitude, longitude,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person_address OVER (ORDER BY src.address_id),
                    src.address_id,
                    src.address_line1,
                    src.address_line2,
                    src.city,
                    src.state_province_id,
                    pr.state_province_sk,
                    src.postal_code,
                    src.latitude,
                    src.longitude,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full src
                LEFT JOIN silver.erp_state_province pr
                    ON src.state_province_id = pr.state_province_id;

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
            -- Last Silver load timestamp
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_person_address;

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

            -----------------------------------------------------
            -- Build DELTA (#src_delta)
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.AddressID AS address_id,

                    CASE WHEN TRIM(b.AddressLine1) = '' OR b.AddressLine1 IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.AddressLine1)
                    END AS address_line1,

                    CASE WHEN TRIM(b.AddressLine2) = '' OR b.AddressLine2 IS NULL
                        THEN NULL
                        ELSE TRIM(b.AddressLine2)
                    END AS address_line2,

                    CASE WHEN TRIM(b.City) = '' OR b.City IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.City)
                    END AS city,

                    b.StateProvinceID AS state_province_id,

                    CASE WHEN TRIM(b.PostalCode) = '' OR b.PostalCode IS NULL
                        THEN 'Unknown'
                        ELSE TRIM(b.PostalCode)
                    END AS postal_code,

                    b.SpatialLocation.Lat  AS latitude,
                    b.SpatialLocation.Long AS longitude
                FROM bronze.erp_person_address b
                WHERE b.dwh_load_date > @last_silver_load
                  AND b.AddressID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'person_address'
                          AND r.business_key = CAST(b.AddressID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.address_id,
                cte.address_line1,
                cte.address_line2,
                cte.city,
                cte.state_province_id,
                cte.postal_code,
                cte.latitude,
                cte.longitude,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.address_id AS VARCHAR(20)),
                        cte.address_line1,
                        ISNULL(cte.address_line2, ''),
                        cte.city,
                        CAST(ISNULL(cte.state_province_id, -1) AS VARCHAR(20)),
                        cte.postal_code,
                        CAST(ISNULL(cte.latitude, 0.0) AS VARCHAR(50)),
                        CAST(ISNULL(cte.longitude, 0.0) AS VARCHAR(50))
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
                SET tgt.address_line1     = src.address_line1,
                    tgt.address_line2     = src.address_line2,
                    tgt.city              = src.city,
                    tgt.state_province_id = src.state_province_id,
                    tgt.postal_code       = src.postal_code,
                    tgt.latitude          = src.latitude,
                    tgt.longitude         = src.longitude,
                    tgt.row_hash          = src.row_hash,
                    tgt.dwh_load_date     = @load_ts,
                    tgt.dwh_batch_id      = @batch_id
                FROM silver.erp_person_address tgt
                INNER JOIN #src_delta src
                    ON src.address_id = tgt.address_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                -----------------------------------------------------
                -- INSERT new business keys
                -----------------------------------------------------
                INSERT INTO silver.erp_person_address
                    (address_sk, address_id,
                     address_line1, address_line2, city,
                     state_province_id, state_province_sk, postal_code,
                     latitude, longitude,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person_address OVER (ORDER BY src.address_id),
                    src.address_id,
                    src.address_line1,
                    src.address_line2,
                    src.city,
                    src.state_province_id,
                    pr.state_province_sk,
                    src.postal_code,
                    src.latitude,
                    src.longitude,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_state_province pr
                    ON src.state_province_id = pr.state_province_id
                LEFT JOIN silver.erp_person_address tgt
                    ON tgt.address_id = src.address_id
                WHERE tgt.address_id IS NULL;

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
            THROW 51020, 'Unsupported load type for PERSON_ADDRESS.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: person_address';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
