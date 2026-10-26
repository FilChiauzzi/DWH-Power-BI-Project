/*
=========================================================
Silver Load – Person (SCD1)
=========================================================
Purpose:
    Loads silver.erp_person from bronze.erp_person with SCD
    Type 1 logic: one row per person_id, changes overwrite
    the previous values. It cleans the data and uses a
    row_hash to detect changes. Rows quarantined in
    etl.rejected for the current batch are excluded, and a
    default member (surrogate key -1, 'Unknown') is always
    kept.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction (it stops if Bronze is
    empty). INCREMENTAL reads only the Bronze rows loaded
    after the last Silver load (MAX(dwh_load_date) of the
    Silver table), then updates rows whose row_hash changed
    and inserts new business keys.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
    Incremental rows (CDC): Silver reads the Bronze rows not
    yet processed (dwh_batch_id > last_silver_batch_id in
    etl.config), only the latest version of each key (a key
    that arrived in several pending batches is loaded once,
    from its most recent batch) and never the deleted rows
    (dwh_operation = 'D'), which are handled separately.

    Deletions (soft delete): a key whose latest pending
    Bronze version is a deletion (dwh_operation = 'D') is not
    removed from Silver: the row is kept and flagged
    dwh_is_deleted = 1 with dwh_deleted_date, so history and
    the facts pointing to it are preserved. If the key comes
    back later (new insert in the source), the row is updated
    and the flag is reset to 0.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object. SSIS passes
    the current @batch_id. The load type (full or
    incremental) is read from etl.config.

    Employee and Customer depend on this table.

Warning:
    A FULL load truncates silver.erp_person and restarts its
    surrogate key sequence. Tables that store its surrogate
    keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_person]
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
            @object_name    = 'person',
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

            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze person is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_person;

                ALTER SEQUENCE dwh.seq_silver_person RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_person WHERE person_sk = -1)
                    INSERT INTO silver.erp_person (
                        person_sk, person_id, person_name, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown', NULL, SYSDATETIME(), SYSDATETIME(), -1 
                        );

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
            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -- (empty Silver -> 1900, so everything is read)
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_person;

            -- Bronze -> Silver register: last batch processed with success
            DECLARE @last_silver_batch_id BIGINT =
                (SELECT ISNULL(last_silver_batch_id, 0)
                 FROM etl.config
                 WHERE object_name = 'person');

            PRINT '>> Silver register (last batch processed): ' + CAST(@last_silver_batch_id AS NVARCHAR(20));

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

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
                WHERE b.dwh_batch_id > @last_silver_batch_id          -- not yet processed by Silver
                  AND b.dwh_operation <> 'D'                         -- deletions handled separately
                  AND NOT EXISTS (                                      -- latest version of the key only
                        SELECT 1
                        FROM bronze.erp_person AS newer
                        WHERE newer.BusinessEntityID = b.BusinessEntityID
                          AND newer.dwh_batch_id > b.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    )
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

                -- Deleted keys: latest pending version is a deletion ('D')
                DECLARE @rows_deleted_src INT = 0, @rows_deleted INT = 0;

                DROP TABLE IF EXISTS #src_deleted;

                SELECT b.BusinessEntityID AS person_id
                INTO #src_deleted
                FROM bronze.erp_person AS b
                WHERE b.dwh_batch_id > @last_silver_batch_id
                  AND b.dwh_operation = 'D'
                  AND NOT EXISTS (
                        SELECT 1
                        FROM bronze.erp_person AS newer
                        WHERE newer.BusinessEntityID = b.BusinessEntityID
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
                        @rows_updated   = @rows_updated,
                        @watermark_from = @last_silver_load;
                    RETURN;
                END

                BEGIN TRANSACTION;
                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.person_name    = src.person_name,
                    tgt.row_hash       = src.row_hash,
                    tgt.dwh_load_date  = @load_ts,
                    tgt.dwh_batch_id   = @batch_id,
                    tgt.dwh_is_deleted = 0,
                    tgt.dwh_deleted_date = NULL
                FROM silver.erp_person tgt
                INNER JOIN #src_delta src
                    ON src.person_id = tgt.person_id
                WHERE (tgt.row_hash <> src.row_hash OR tgt.dwh_is_deleted = 1);   -- changed, or back after a deletion

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
                LEFT JOIN  silver.erp_person tgt
                    ON tgt.person_id = src.person_id
                WHERE tgt.person_id IS NULL;

                SET @rows_inserted = @@ROWCOUNT;

                -- Deletions (soft delete): keep the row, flag it
                UPDATE tgt
                SET tgt.dwh_is_deleted   = 1,
                    tgt.dwh_deleted_date = @load_ts,
                    tgt.dwh_load_date    = @load_ts,
                    tgt.dwh_batch_id     = @batch_id
                FROM silver.erp_person AS tgt
                INNER JOIN #src_deleted AS d
                    ON tgt.person_id = d.person_id
                WHERE tgt.dwh_is_deleted = 0;

                SET @rows_deleted = @@ROWCOUNT;
                PRINT '>> Rows deleted (flagged): ' + CAST(@rows_deleted AS NVARCHAR(20));

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
        PRINT 'ERROR during Silver load for: person';
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
