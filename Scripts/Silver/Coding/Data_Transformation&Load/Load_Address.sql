/*
=========================================================
Silver Load – Person Address (SCD1)
=========================================================
Purpose:
    This stored procedure loads the Silver-layer table
    for the Person Address domain. It applies SCD Type 1
    logic, performing a FULL or INCREMENTAL load based on
    the configuration stored in etl.config.

    The procedure cleans raw Bronze data, applies business
    rules (Unknown substitutions), computes row_hash for
    change detection, and manages surrogate key generation
    through dwh.seq_silver_person_address.

FULL Load:
    - Truncates the Silver table
    - Resets the surrogate key sequence
    - Reloads all valid rows from bronze.erp_person_address
    - Excludes rows quarantined in etl.rejected
    - Inserts all rows as new SCD1 records

INCREMENTAL Load:
    - Reads only rows with ModifiedDate > last watermark
    - Excludes quarantined rows
    - Updates existing Silver rows when row_hash changes
    - Inserts new business keys not yet present in Silver
    - Updates the watermark in etl.config

Data Quality:
    Rows rejected by Bronze DQ (NULL keys, duplicates,
    invalid foreign keys, etc.) are excluded from the load.
    The procedure assumes Bronze DQ has already run for the
    current batch.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS reads etl.config to determine the
    load type and watermark, then calls this procedure to
    populate the Silver layer.

Warning:
    Running this procedure will delete previous Silver data
    when load_type = 'full'. Ensure Bronze DQ has completed
    successfully before executing this load.
*/

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_person_address]
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
        WHERE object_name = 'person_address'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for person_address in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – PERSON_ADDRESS (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.person_address;

                ALTER SEQUENCE dwh.seq_silver_person_address RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.AddressID AS address_id,

                    CASE
                        WHEN b.AddressLine1 IS NULL OR LTRIM(RTRIM(b.AddressLine1)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.AddressLine1))
                    END AS address_line1,

                    CASE
                        WHEN b.AddressLine2 IS NULL OR LTRIM(RTRIM(b.AddressLine2)) = '' 
                            THEN NULL
                        ELSE LTRIM(RTRIM(b.AddressLine2))
                    END AS address_line2,

                    CASE
                        WHEN b.City IS NULL OR LTRIM(RTRIM(b.City)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.City))
                    END AS city,

                    b.StateProvinceID AS state_province_id,

                    CASE
                        WHEN b.PostalCode IS NULL OR LTRIM(RTRIM(b.PostalCode)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.PostalCode))
                    END AS postal_code,

                    b.SpatialLocation.Lat  AS latitude,
                    b.SpatialLocation.Long AS longitude,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.AddressID,
                            CASE
                                WHEN b.AddressLine1 IS NULL OR LTRIM(RTRIM(b.AddressLine1)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.AddressLine1))
                            END,
                            ISNULL(LTRIM(RTRIM(b.AddressLine2)), ''),
                            CASE
                                WHEN b.City IS NULL OR LTRIM(RTRIM(b.City)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.City))
                            END,
                            ISNULL(b.StateProvinceID, -1),
                            CASE
                                WHEN b.PostalCode IS NULL OR LTRIM(RTRIM(b.PostalCode)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.PostalCode))
                            END,
                            CAST(ISNULL(b.SpatialLocation.Lat, 0.0)  AS VARCHAR(50)),
                            CAST(ISNULL(b.SpatialLocation.Long, 0.0) AS VARCHAR(50))
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_person_address AS b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'person_address'
                      AND r.business_key = CAST(b.AddressID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.person_address
                    (address_sk, address_id,
                     address_line1, address_line2, city,
                     state_province_id, postal_code,
                     latitude, longitude,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person_address OVER (ORDER BY src.address_id),
                    src.address_id,
                    src.address_line1,
                    src.address_line2,
                    src.city,
                    src.state_province_id,
                    src.postal_code,
                    src.latitude,
                    src.longitude,
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
                    b.AddressID AS address_id,

                    CASE
                        WHEN b.AddressLine1 IS NULL OR LTRIM(RTRIM(b.AddressLine1)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.AddressLine1))
                    END AS address_line1,

                    CASE
                        WHEN b.AddressLine2 IS NULL OR LTRIM(RTRIM(b.AddressLine2)) = '' 
                            THEN NULL
                        ELSE LTRIM(RTRIM(b.AddressLine2))
                    END AS address_line2,

                    CASE
                        WHEN b.City IS NULL OR LTRIM(RTRIM(b.City)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.City))
                    END AS city,

                    b.StateProvinceID AS state_province_id,

                    CASE
                        WHEN b.PostalCode IS NULL OR LTRIM(RTRIM(b.PostalCode)) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.PostalCode))
                    END AS postal_code,

                    b.SpatialLocation.Lat  AS latitude,
                    b.SpatialLocation.Long AS longitude,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.AddressID,
                            CASE
                                WHEN b.AddressLine1 IS NULL OR LTRIM(RTRIM(b.AddressLine1)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.AddressLine1))
                            END,
                            ISNULL(LTRIM(RTRIM(b.AddressLine2)), ''),
                            CASE
                                WHEN b.City IS NULL OR LTRIM(RTRIM(b.City)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.City))
                            END,
                            ISNULL(b.StateProvinceID, -1),
                            CASE
                                WHEN b.PostalCode IS NULL OR LTRIM(RTRIM(b.PostalCode)) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.PostalCode))
                            END,
                            CAST(ISNULL(b.SpatialLocation.Lat, 0.0)  AS VARCHAR(50)),
                            CAST(ISNULL(b.SpatialLocation.Long, 0.0) AS VARCHAR(50))
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_person_address AS b
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected AS r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'person_address'
                          AND r.business_key = CAST(b.AddressID AS NVARCHAR(200))
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
                FROM silver.person_address AS tgt
                INNER JOIN #src_delta AS src
                    ON src.address_id = tgt.address_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.person_address
                    (address_sk, address_id,
                     address_line1, address_line2, city,
                     state_province_id, postal_code,
                     latitude, longitude,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person_address OVER (ORDER BY src.address_id),
                    src.address_id,
                    src.address_line1,
                    src.address_line2,
                    src.city,
                    src.state_province_id,
                    src.postal_code,
                    src.latitude,
                    src.longitude,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta AS src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.person_address AS tgt
                    WHERE tgt.address_id = src.address_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'person_address'
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
