/*
=========================================================
Silver Load – Sales Person (SCD2)
=========================================================
Purpose:
    Loads silver.erp_sales_person from
    bronze.erp_sales_person with SCD Type 2 logic: a change
    closes the active version and inserts a new one, so
    history is kept. It resolves the surrogate keys of the
    employee (silver.erp_employee) and of the territory
    (silver.erp_sales_territory) and uses a row_hash to
    detect changes. Rows quarantined in etl.rejected for the
    current batch are excluded, and a default member
    (surrogate key -1, 'Unknown') is always kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads all rows as first versions (it stops if Bronze
    is empty). INCREMENTAL reads the Bronze rows loaded
    after the last Silver load, closes the active version
    when the row_hash differs and inserts the new version.

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
    the Bronze quality check of the same object and after
    silver.erp_employee and silver.erp_sales_territory. SSIS
    passes the current @batch_id. The load type (full or
    incremental) is read from etl.config.

Warning:
    A FULL load truncates silver.erp_sales_person and
    restarts its surrogate key sequence. All history is
    lost. Tables that store its surrogate keys must be
    reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_sales_person]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @start_time        DATETIME2(3),
            @end_time          DATETIME2(3),
            @load_ts           DATETIME2(3) = SYSDATETIME(),
            @load_type         VARCHAR(20),
            @last_silver_load  DATETIME2(3),
            @rows_src          INT = 0,
            @rows_closed       INT = 0,
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
            @object_name    = 'sales_person',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type
        FROM etl.config
        WHERE object_name = 'sales_person'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for sales_person in etl.config.', 1;

        IF @load_type NOT IN ('full', 'incremental')
            THROW 51020, 'Unsupported load type for SALES PERSON.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES PERSON (SCD2) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD (SCD2)
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();


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


            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze sales_person is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_sales_person;

                ALTER SEQUENCE dwh.seq_silver_sales_person RESTART WITH 1;

                ---------------------------------------------------------
                -- SALES PERSON (SCD2) – UNKNOWN MEMBER
                ---------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_sales_person WHERE sales_person_sk = -1)
                    INSERT INTO silver.erp_sales_person (
                        sales_person_sk,
                        person_id,
                        territory_id,
                        sales_quota,
                        bonus,
                        commission_pct,
                        sales_ytd,
                        sales_last_year,
                        territory_sk,
                        employee_sk,
                        row_hash,
                        dwh_create_date,
                        dwh_start_date,
                        dwh_end_date,
                        is_active,
                        dwh_load_date,
                        dwh_batch_id
                    )
                    VALUES (
                        -1,                -- sales_person_sk
                        -1,                -- person_id
                        -1,                -- territory_id
                        0,                 -- sales_quota
                        0,                 -- bonus
                        0,                 -- commission_pct
                        0,                 -- sales_ytd
                        0,                 -- sales_last_year
                        -1,                -- territory_sk
                        -1,                -- employee_sk
                        NULL,              -- row_hash
                        @load_ts,          -- dwh_create_date
                        @load_ts,          -- dwh_start_date
                        DATEFROMPARTS(9999,12,31), -- dwh_end_date
                        1,                 -- is_active
                        @load_ts,          -- dwh_load_date
                        -1                 -- dwh_batch_id
                    );


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
        -- INCREMENTAL LOAD (SCD2)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD2) started';

        ---------------------------------------------------------
        -- Bookmark: last time Silver wrote something
        -- (empty Silver -> 1900, so everything is read)
        ---------------------------------------------------------
        SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
        FROM silver.erp_sales_person;

        -- Bronze -> Silver register: last batch processed with success
        DECLARE @last_silver_batch_id BIGINT =
            (SELECT ISNULL(last_silver_batch_id, 0)
             FROM etl.config
             WHERE object_name = 'sales_person');

        PRINT '>> Silver register (last batch processed): ' + CAST(@last_silver_batch_id AS NVARCHAR(20));

        PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

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
                WHERE b.dwh_batch_id > @last_silver_batch_id          -- not yet processed by Silver
                  AND b.dwh_operation <> 'D'                         -- deletions handled separately
                  AND NOT EXISTS (                                      -- latest version of the key only
                        SELECT 1
                        FROM bronze.erp_sales_person AS newer
                        WHERE newer.BusinessEntityID = b.BusinessEntityID
                          AND newer.dwh_batch_id > b.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    )
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
                        @rows_updated   = @rows_closed,
                        @watermark_from = @last_silver_load;
                    RETURN;
                END

                BEGIN TRANSACTION;

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

                SET @rows_closed = @@ROWCOUNT;

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
                 LEFT JOIN silver.erp_sales_person tgt
                    ON tgt.person_id = src.person_id
                      AND tgt.is_active = 1
                  WHERE  tgt.person_id IS NULL

                SET @rows_inserted = @@ROWCOUNT;


            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows updated:  ' + CAST(@rows_closed  AS NVARCHAR(20));
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

        -- Audit: INCREMENTAL load completed
        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @load_type,
            @rows_read      = @rows_src,
            @rows_inserted  = @rows_inserted,
            @rows_updated   = @rows_closed,
            @watermark_from = @last_silver_load;

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
