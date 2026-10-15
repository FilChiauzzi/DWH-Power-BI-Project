CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_state_province]
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
        WHERE object_name = 'state_province'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for state_province in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – STATE_PROVINCE (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_state_province;

                ALTER SEQUENCE dwh.seq_silver_state_province RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.StateProvinceID AS state_province_id,
                    b.StateProvinceCode AS state_province_code,
                    b.CountryRegionCode AS country_region_code,
                    b.TerritoryID AS territory_id,

                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS name,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.StateProvinceID,
                            b.StateProvinceCode,
                            b.CountryRegionCode,
                            b.TerritoryID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.[Name]))
                            END
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_state_province AS b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'state_province'
                      AND r.business_key = CAST(b.StateProvinceID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
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
                FROM #src_full AS src
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
                    b.StateProvinceID AS state_province_id,
                    b.StateProvinceCode AS state_province_code,
                    b.CountryRegionCode AS country_region_code,
                    b.TerritoryID AS territory_id,

                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS name,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.StateProvinceID,
                            b.StateProvinceCode,
                            b.CountryRegionCode,
                            b.TerritoryID,
                            CASE
                                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                    THEN 'Unknown'
                                ELSE LTRIM(RTRIM(b.[Name]))
                            END
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_state_province AS b
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected AS r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'state_province'
                          AND r.business_key = CAST(b.StateProvinceID AS NVARCHAR(200))
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
                SET tgt.state_province_code = src.state_province_code,
                    tgt.country_region_code = src.country_region_code,
                    tgt.country_region_sk   = cr.country_sk,
                    tgt.name                = src.name,
                    tgt.territory_id        = src.territory_id,
                    tgt.territory_sk        = tr.territory_sk,
                    tgt.row_hash            = src.row_hash,
                    tgt.dwh_load_date       = @load_ts,
                    tgt.dwh_batch_id        = @batch_id
                FROM silver.erp_state_province AS tgt
                INNER JOIN #src_delta AS src
                    ON src.state_province_id = tgt.state_province_id
                LEFT JOIN silver.erp_country_region cr
                    ON src.country_region_code = cr.country_code
                LEFT JOIN silver.erp_sales_territory tr
                    ON src.territory_id = tr.territory_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
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
                FROM #src_delta AS src
                LEFT JOIN silver.erp_country_region cr
                    ON src.country_region_code = cr.country_code
                LEFT JOIN silver.erp_sales_territory tr
                    ON src.territory_id = tr.territory_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_state_province AS tgt
                    WHERE tgt.state_province_id = src.state_province_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'state_province'
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
            THROW 51020, 'Unsupported load type for STATE_PROVINCE.', 1;
        END

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

        THROW;
    END CATCH
END;
GO
