/*
=========================================================
Silver Load – Customer (SCD2)
=========================================================
Purpose:
    Loads silver.erp_customer from bronze.erp_customer with
    SCD Type 2 logic: a change of PersonID, StoreID or
    TerritoryID closes the active version and inserts a new
    one (dwh_start_date, dwh_end_date, is_active). It
    derives customer_type (store, store contact,
    individual), resolves person_sk, store_sk and
    territory_sk, and uses a row_hash to detect changes.

    FULL truncates, restarts the surrogate key sequence and
    reloads everything. INCREMENTAL reads only the Bronze
    rows loaded after the last Silver load
    (MAX(dwh_load_date) of the Silver table), then closes
    the active version when the row_hash changed and inserts
    the new version.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
    Incremental rows (CDC): Silver reads the Bronze rows not
    yet processed (dwh_batch_id > last_silver_batch_id in
    etl.config), only the latest version of each key (a key
    that arrived in several pending batches is loaded once,
    from its most recent batch) and never the deleted rows
    (dwh_operation = 'D'), which are handled separately.

    Deletions (soft delete, SCD2): a key whose latest pending
    Bronze version is a deletion (dwh_operation = 'D') gets its
    active version closed (is_active = 0, dwh_end_date = load
    time) and flagged dwh_is_deleted = 1; no new version is
    opened. If the key comes back later, a new active version
    is opened as for any changed key.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    silver.erp_person, silver.erp_store and
    silver.erp_sales_territory. SSIS passes the current
    @batch_id. The load type (full or incremental) is read
    from etl.config.

Warning:
    A FULL load truncates silver.erp_customer and restarts
    its surrogate key sequence. All history is lost. Tables
    that store its surrogate keys must be reloaded
    afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_customer]
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
            @object_name    = 'customer',
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
        WHERE object_name = 'customer'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for CUSTOMER.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – CUSTOMER (SCD2) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    c.CustomerID AS customer_id,
                    c.PersonID   AS person_id,
                    c.StoreID    AS store_id,
                    c.TerritoryID AS territory_id,
                    CASE
                        WHEN c.PersonID IS NULL AND c.StoreID IS NOT NULL THEN 'store'
                        WHEN c.PersonID IS NOT NULL AND c.StoreID IS NOT NULL THEN 'store contact'
                        ELSE 'individual customer'
                    END AS customer_type,
                    p.person_sk,
                    s.store_sk,
                    t.territory_sk,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            c.PersonID,
                            c.StoreID,
                            c.TerritoryID
                        )
                    ) AS row_hash,
                    c.ModifiedDate
                INTO #src_full
                FROM bronze.erp_customer c
                LEFT JOIN silver.erp_person p
                    ON c.PersonID = p.person_id
                LEFT JOIN silver.erp_store s
                    ON c.StoreID = s.store_id
                LEFT JOIN silver.erp_sales_territory t
                    ON c.TerritoryID = t.territory_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'customer'
                      AND r.business_key = CAST(c.CustomerID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze person is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_customer;

                ALTER SEQUENCE dwh.seq_silver_customer RESTART WITH 1;

                ---------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN
                ---------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_customer WHERE customer_sk = -1)
                    INSERT INTO silver.erp_customer (
                        customer_sk, customer_id, customer_type, person_sk, store_sk, territory_sk,
                        row_hash, dwh_create_date, dwh_start_date, dwh_end_date, is_active, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown', -1, -1, -1,
                        NULL, @load_ts, @load_ts, DATEFROMPARTS(9999,12,31), 1, @load_ts, -1
                    );

                ---------------------------------------------------------
                -- Insert FULL (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_customer
                    (customer_sk, customer_id, person_id, store_id, territory_id,
                     customer_type, person_sk, store_sk, territory_sk,
                     row_hash, dwh_start_date, dwh_end_date, is_active,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_customer OVER (ORDER BY src.customer_id),
                    src.customer_id,
                    src.person_id,
                    src.store_id,
                    src.territory_id,
                    src.customer_type,
                    src.person_sk,
                    src.store_sk,
                    src.territory_sk,
                    src.row_hash,
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
        FROM silver.erp_customer;

        -- Bronze -> Silver register: last batch processed with success
        DECLARE @last_silver_batch_id BIGINT =
            (SELECT ISNULL(last_silver_batch_id, 0)
             FROM etl.config
             WHERE object_name = 'customer');

        PRINT '>> Silver register (last batch processed): ' + CAST(@last_silver_batch_id AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    c.CustomerID AS customer_id,
                    c.PersonID   AS person_id,
                    c.StoreID    AS store_id,
                    c.TerritoryID AS territory_id,
                    CASE
                        WHEN c.PersonID IS NULL AND c.StoreID IS NOT NULL THEN 'store'
                        WHEN c.PersonID IS NOT NULL AND c.StoreID IS NOT NULL THEN 'store contact'
                        ELSE 'individual customer'
                    END AS customer_type,
                    p.person_sk,
                    s.store_sk,
                    t.territory_sk,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            c.PersonID,
                            c.StoreID,
                            c.TerritoryID
                        )
                    ) AS row_hash,
                    c.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_customer c
                LEFT JOIN silver.erp_person p
                    ON c.PersonID = p.person_id
                LEFT JOIN silver.erp_store s
                    ON c.StoreID = s.store_id
                LEFT JOIN silver.erp_sales_territory t
                    ON c.TerritoryID = t.territory_id
                WHERE c.dwh_batch_id > @last_silver_batch_id          -- not yet processed by Silver
                  AND c.dwh_operation <> 'D'                         -- deletions handled separately
                  AND NOT EXISTS (                                      -- latest version of the key only
                        SELECT 1
                        FROM bronze.erp_customer AS newer
                        WHERE newer.CustomerID = c.CustomerID
                          AND newer.dwh_batch_id > c.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'customer'
                          AND r.business_key = CAST(c.CustomerID AS NVARCHAR(200))
                    );

                SELECT @rows_src = COUNT(*) FROM #src_delta;

                PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                -- Deleted keys: latest pending version is a deletion ('D')
                DECLARE @rows_deleted_src INT = 0, @rows_deleted INT = 0;

                DROP TABLE IF EXISTS #src_deleted;

                SELECT b.CustomerID AS customer_id
                INTO #src_deleted
                FROM bronze.erp_customer AS b
                WHERE b.dwh_batch_id > @last_silver_batch_id
                  AND b.dwh_operation = 'D'
                  AND NOT EXISTS (
                        SELECT 1
                        FROM bronze.erp_customer AS newer
                        WHERE newer.CustomerID = b.CustomerID
                          AND newer.dwh_batch_id > b.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    );

                SET @rows_deleted_src = @@ROWCOUNT;
                PRINT '>> Deleted keys in source: ' + CAST(@rows_deleted_src AS NVARCHAR(20));

                IF @rows_src = 0 AND @rows_deleted_src = 0
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
                -- Close previous versions (SCD2)
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.dwh_end_date = @load_ts,
                    tgt.is_active     = 0,
                    tgt.dwh_load_date = @load_ts,
                    tgt.dwh_batch_id  = @batch_id
                FROM silver.erp_customer tgt
                INNER JOIN #src_delta src
                    ON src.customer_id = tgt.customer_id
                WHERE tgt.is_active = 1
                  AND tgt.row_hash <> src.row_hash;

                SET @rows_closed = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Insert new versions (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_customer
                    (customer_sk, customer_id, person_id, store_id, territory_id,
                     customer_type, person_sk, store_sk, territory_sk,
                     row_hash, dwh_start_date, dwh_end_date, is_active,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_customer OVER (ORDER BY src.customer_id),
                    src.customer_id,
                    src.person_id,
                    src.store_id,
                    src.territory_id,
                    src.customer_type,
                    src.person_sk,
                    src.store_sk,
                    src.territory_sk,
                    src.row_hash,
                    @load_ts,
                     DATEFROMPARTS(9999,12,31),
                    1,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_customer tgt
                    ON tgt.customer_id = src.customer_id
                      AND tgt.is_active = 1
                WHERE tgt.customer_id IS NULL;

                SET @rows_inserted = @@ROWCOUNT;

                -- Deletions (SCD2): close the active version, open none
                UPDATE tgt
                SET tgt.is_active        = 0,
                    tgt.dwh_end_date     = @load_ts,
                    tgt.dwh_is_deleted   = 1,
                    tgt.dwh_deleted_date = @load_ts,
                    tgt.dwh_load_date    = @load_ts,
                    tgt.dwh_batch_id     = @batch_id
                FROM silver.erp_customer AS tgt
                INNER JOIN #src_deleted AS d
                    ON tgt.customer_id = d.customer_id
                WHERE tgt.is_active = 1;

                SET @rows_deleted = @@ROWCOUNT;
                PRINT '>> Rows deleted (versions closed): ' + CAST(@rows_deleted AS NVARCHAR(20));

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows closed:   ' + CAST(@rows_closed   AS NVARCHAR(20));
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
            THROW 51020, 'Unsupported load type for CUSTOMER.', 1;
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
        PRINT 'ERROR during Silver load for: customer';
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
