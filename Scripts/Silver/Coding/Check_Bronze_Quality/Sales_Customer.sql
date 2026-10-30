/*
=========================================================
Bronze Data Quality Check – Customer
=========================================================
Purpose:
    Validates bronze.erp_customer before the Silver load.
    CustomerID NULL or duplicated and TerritoryID NULL are
    blocking errors. PersonID and StoreID may be NULL
    (optional foreign keys).

    Blocking errors quarantine the row; non-blocking
    warnings are only logged.

    Every execution is logged in etl.load_audit (layer silver,
    step dq_check): status, duration, row counts and, on
    failure, the error.

    Rows checked (CDC):
    only the Bronze rows that Silver will load: batch greater
    than last_silver_batch_id in etl.config, latest version of
    each key (when a key arrived in several pending batches,
    only its most recent batch is checked, exactly like the
    Silver load), deleted rows (dwh_operation = 'D') excluded:
    they carry only the key. With a full load Bronze holds only
    the current batch, so every row is checked as before.

    Severity policy (same in every quality check):
      critical  the row cannot be identified (business key NULL
                or duplicated): logged AND set aside in
                etl.rejected, it does not reach Silver;
      error     the row is identified but a value is wrong:
                logged, the row goes on to Silver;
      warning   a value is suspicious but plausible: logged,
                the row goes on to Silver.
    Structure screens check that every foreign key points to
    a row that exists in the Silver parent table (loaded
    earlier in the run); a missing parent is an error and the
    Silver load uses the unknown member (-1).
    Business rule screens (point 5 of subsystem 4) test values
    against each other: dates in the right order, codes in their
    allowed range, a customer that is a person or a store.
    (Kimball, subsystem 4: tag rather than suspend whenever
    possible.)

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline,
    before the Silver load of the same object. SSIS passes
    @batch_id and @max_reject_rate (default 20). Every
    finding is logged in etl.data_quality_log; blocking
    errors are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_customer]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @rows_checked     INT           = 0,
        @rows_rejected    INT           = 0,
        @errors           INT           = 0,
        @reject_rate      DECIMAL(9,4)  = 0;

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
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - customer | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Rows to check (CDC aware)
            ---------------------------------------------------------
            -- Rows to check: Bronze rows that Silver will load.
            --   * batch greater than the Silver register;
            --   * latest batch of each key (older pending versions are
            --     superseded, Silver loads only the latest);
            --   * deleted rows excluded: a D row carries only its key.
            DECLARE @last_silver_batch_id BIGINT =
                (SELECT ISNULL(last_silver_batch_id, 0)
                 FROM etl.config
                 WHERE object_name = 'customer');

            DROP TABLE IF EXISTS #b;

            SELECT *
            INTO #b
            FROM (
                SELECT b.*,
                       DENSE_RANK() OVER (PARTITION BY b.CustomerID
                                          ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
                FROM bronze.erp_customer AS b
                WHERE b.dwh_batch_id > @last_silver_batch_id
            ) AS pending
            WHERE pending.dwh_version_rank = 1
              AND pending.dwh_operation <> 'D';

            SELECT @rows_checked = COUNT(*)
            FROM #b;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'customer', 'Source empty', NULL,
                     'No Bronze rows to check: nothing new, or only deletions', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: CustomerID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 1: CustomerID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'CustomerID NULL',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'CustomerID is NULL',
                   'critical'
            FROM #b b
            WHERE b.CustomerID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'customer',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'NULL_BK',
                   'CustomerID is NULL',
                   'critical',
                   JSON_OBJECT(
                       'CustomerID':    b.CustomerID,
                       'PersonID':      b.PersonID,
                       'StoreID':       b.StoreID,
                       'TerritoryID':   b.TerritoryID,
                       'AccountNumber': b.AccountNumber,
                       'ModifiedDate':  b.ModifiedDate
                       NULL ON NULL
                   )
            FROM #b b
            WHERE b.CustomerID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate CustomerID -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate CustomerID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Duplicate CustomerID',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'Duplicate CustomerID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'critical'
            FROM #b b
            GROUP BY b.CustomerID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'customer',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate CustomerID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'critical',
                   JSON_OBJECT(
                       'CustomerID':    b.CustomerID,
                       'PersonID':      b.PersonID,
                       'StoreID':       b.StoreID,
                       'TerritoryID':   b.TerritoryID,
                       'AccountNumber': b.AccountNumber,
                       'ModifiedDate':  b.ModifiedDate
                       NULL ON NULL
                   )
            FROM #b b
            INNER JOIN (
                SELECT CustomerID, COUNT(*) AS cnt
                FROM #b
                GROUP BY CustomerID
                HAVING COUNT(*) > 1
            ) d
                ON d.CustomerID = b.CustomerID;

            ---------------------------------------------------------
            -- RULE 3: TerritoryID NULL -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 3: TerritoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'TerritoryID NULL',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'TerritoryID is NULL',
                   'error'
            FROM #b b
            WHERE b.TerritoryID IS NULL;


            ---------------------------------------------------------
            -- RULE 4: Territory not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 4: Territory not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Territory not found',
                   CAST(x.CustomerID AS NVARCHAR(200)),
                   CONCAT('TerritoryID ', x.TerritoryID, ' does not exist in silver.erp_sales_territory - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.TerritoryID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_sales_territory AS p
                              WHERE p.territory_id = x.TerritoryID);

            ---------------------------------------------------------
            -- RULE 5: Person not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 5: Person not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Person not found',
                   CAST(x.CustomerID AS NVARCHAR(200)),
                   CONCAT('PersonID ', x.PersonID, ' does not exist in silver.erp_person - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.PersonID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_person AS p
                              WHERE p.person_id = x.PersonID);

            ---------------------------------------------------------
            -- RULE 6: Store not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 6: Store not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Store not found',
                   CAST(x.CustomerID AS NVARCHAR(200)),
                   CONCAT('StoreID ', x.StoreID, ' does not exist in silver.erp_store - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.StoreID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_store AS p
                              WHERE p.store_id = x.StoreID);


            ---------------------------------------------------------
            -- RULE 7: Neither person nor store -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 7: Neither person nor store (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Customer without person or store',
                   CAST(x.CustomerID AS NVARCHAR(200)),
                   'PersonID and StoreID are both NULL: the customer is neither a person nor a store',
                   'error'
            FROM #b AS x
            WHERE x.PersonID IS NULL AND x.StoreID IS NULL;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'customer'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'customer';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, SYSDATETIME()) AS NVARCHAR(20)) + ' ms';

        ---------------------------------------------------------
        -- Threshold guard
        ---------------------------------------------------------
        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - customer completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        -- Audit: quality check completed
        EXEC etl.usp_audit_end
            @audit_id      = @audit_id,
            @status        = 'succeeded',
            @rows_read     = @rows_checked,
            @rows_rejected = @rows_rejected;
        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: customer';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id      = @audit_id,
                @status        = 'failed',
                @rows_read     = @rows_checked,
                @rows_rejected = @rows_rejected;

        THROW;
    END CATCH
END;
GO
