/*
=========================================================
Silver Load – Sales Person (SCD2)
=========================================================
Purpose:
    Loads the Sales Person dimension into the Silver layer
    using Slowly Changing Dimension Type 2 logic. Each change
    to business attributes generates a new version of the row.
    The procedure performs FULL or INCREMENTAL loads based on
    the configuration stored in etl.config.

SCD2 Behavior:
    - FULL: rebuilds the dimension from scratch, inserting
      one active version per business key.
    - INCREMENTAL: detects changes via row_hash. When a change
      is found, the current version is closed (is_active = 0)
      and a new version is inserted with updated attributes.
    - New business keys always generate a new active version.

Lookups:
    - employee_sk from silver.erp_employee
    - territory_sk from silver.erp_sales_territory

SSIS Usage:
    This procedure is executed inside the metadata-driven SSIS
    pipeline. SSIS loops through all active objects and calls
    the corresponding Silver Load procedure for each domain.
    The procedure inserts or updates rows in the Silver layer
    and updates the watermark for incremental loads.

Warning:
    Running this procedure with load_type = 'full' will delete
    all existing Silver data for Sales Person.
*/

CREATE OR ALTER PROCEDURE etl.usp_load_silver_sales_person
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
        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @last_wm   = last_watermark_value
        FROM etl.config
        WHERE object_name = 'sales_person'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SALES_PERSON.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES_PERSON (SCD2) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD (SCD2)
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_sales_person;

                ALTER SEQUENCE dwh.seq_silver_sales_person RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS person_id,
                    b.TerritoryID      AS territory_id,
                    b.SalesQuota,
                    b.Bonus,
                    b.CommissionPct,
                    b.SalesYTD,
                    b.SalesLastYear,

                    e.employee_sk,
                    t.territory_sk,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            b.TerritoryID,
                            b.SalesQuota,
                            b.Bonus,
                            b.CommissionPct,
                            b.SalesYTD,
                            b.SalesLastYear
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_sales_person b
                LEFT JOIN silver.erp_employee e
                    ON e.business_entity_id = b.BusinessEntityID
                LEFT JOIN silver.erp_sales_territory t
                    ON t.territory_id = b.TerritoryID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'sales_person'
                      AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_sales_person
                    (sales_person_sk, person_id, territory_id,
                     sales_quota, bonus, commission_pct,
                     sales_ytd, sales_last_year,
                     territory_sk, employee_sk,
                     row_hash, dwh_create_date, dwh_start_date,
                     dwh_end_date, is_active, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_sales_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.territory_id,
                    src.SalesQuota,
                    src.Bonus,
                    src.CommissionPct,
                    src.SalesYTD,
                    src.SalesLastYear,
                    src.territory_sk,
                    src.employee_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    DATEFROMPARTS(9999,12,31),
                    1,
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
        -- INCREMENTAL LOAD (SCD2)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD2) started';

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS person_id,
                    b.TerritoryID      AS territory_id,
                    b.SalesQuota,
                    b.Bonus,
                    b.CommissionPct,
                    b.SalesYTD,
                    b.SalesLastYear,

                    e.employee_sk,
                    t.territory_sk,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            b.TerritoryID,
                            b.SalesQuota,
                            b.Bonus,
                            b.CommissionPct,
                            b.SalesYTD,
                            b.SalesLastYear
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_sales_person b
                LEFT JOIN silver.erp_employee e
                    ON e.business_entity_id = b.BusinessEntityID
                LEFT JOIN silver.erp_sales_territory t
                    ON t.territory_id = b.TerritoryID
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_person'
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
                -- SCD2: CLOSE old version + INSERT new version
                ---------------------------------------------------------

                ---------------------------------------------------------
                -- CLOSE old version (set is_active = 0)
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.is_active     = 0,
                    tgt.dwh_end_date  = @load_ts,
                    tgt.dwh_load_date = @load_ts,
                    tgt.dwh_batch_id  = @batch_id
                FROM silver.erp_sales_person tgt
                INNER JOIN #src_delta src
                    ON src.person_id = tgt.person_id
                WHERE tgt.is_active = 1
                  AND tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new version (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_sales_person
                    (sales_person_sk, person_id, territory_id,
                     sales_quota, bonus, commission_pct,
                     sales_ytd, sales_last_year,
                     territory_sk, employee_sk,
                     row_hash, dwh_create_date, dwh_start_date,
                     dwh_end_date, is_active, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_sales_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.territory_id,
                    src.SalesQuota,
                    src.Bonus,
                    src.CommissionPct,
                    src.SalesYTD,
                    src.SalesLastYear,
                    src.territory_sk,
                    src.employee_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    DATEFROMPARTS(9999,12,31),
                    1,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_sales_person tgt
                    WHERE tgt.person_id = src.person_id
                      AND tgt.is_active = 1
                      AND tgt.row_hash = src.row_hash
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'sales_person'
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
            THROW 51020, 'Unsupported load type for SALES_PERSON.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_person';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
