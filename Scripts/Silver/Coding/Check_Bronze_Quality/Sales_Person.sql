/*
=========================================================
Bronze Data Quality Check – Sales Person
=========================================================
Purpose:
    Validates bronze.erp_sales_person before the Silver
    load. It checks for NULL or duplicated BusinessEntityID,
    NULL TerritoryID and NULL commission, quota, bonus and
    sales figures.

    Critical findings set the row aside; errors and
    warnings are only logged (see the severity policy).

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
    (Kimball, subsystem 4: tag rather than suspend whenever
    possible.)

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline,
    before the Silver load of the same object. SSIS passes
    @batch_id and @max_reject_rate (default 20). Every
    finding is logged in etl.data_quality_log; critical
    findings are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE etl.usp_check_bronze_quality_load_sales_person
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
            @object_name    = 'sales_person',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality – sales_person | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
             WHERE object_name = 'sales_person');

        DROP TABLE IF EXISTS #b;

        SELECT *
        INTO #b
        FROM (
            SELECT b.*,
                   DENSE_RANK() OVER (PARTITION BY b.BusinessEntityID
                                      ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
            FROM bronze.erp_sales_person AS b
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
                (@batch_id, 'bronze', 'sales_person', 'Source empty', NULL,
                 'No Bronze rows to check: nothing new, or only deletions', 'warning');
        END

        ---------------------------------------------------------
        -- RULE 1: BusinessEntityID NULL -> critical (row set aside)
        ---------------------------------------------------------
        PRINT '-- Rule 1: BusinessEntityID NULL';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'BusinessEntityID NULL',
               NULL,
               'BusinessEntityID is NULL',
               'critical'
        FROM #b
        WHERE BusinessEntityID IS NULL;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'sales_person',
               NULL,
               'NULL_BK',
               'BusinessEntityID is NULL',
               'critical',
               JSON_OBJECT(
                    'BusinessEntityID': BusinessEntityID,
                    'TerritoryID': TerritoryID,
                    'SalesQuota': SalesQuota,
                    'Bonus': Bonus,
                    'CommissionPct': CommissionPct,
                    'SalesYTD': SalesYTD,
                    'SalesLastYear': SalesLastYear,
                    'ModifiedDate': ModifiedDate
               NULL ON NULL)
        FROM #b
        WHERE BusinessEntityID IS NULL;

        ---------------------------------------------------------
        -- RULE 2: Duplicate BusinessEntityID -> critical (row set aside)
        ---------------------------------------------------------
        PRINT '-- Rule 2: Duplicate BusinessEntityID';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Duplicate BusinessEntityID',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Duplicate BusinessEntityID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
               'critical'
        FROM #b b
        GROUP BY b.BusinessEntityID
        HAVING COUNT(*) > 1;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'sales_person',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'DUP_BK',
               'Duplicate BusinessEntityID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
               'critical',
               JSON_OBJECT(
                    'BusinessEntityID': b.BusinessEntityID,
                    'TerritoryID': b.TerritoryID,
                    'SalesQuota': b.SalesQuota,
                    'Bonus': b.Bonus,
                    'CommissionPct': b.CommissionPct,
                    'SalesYTD': b.SalesYTD,
                    'SalesLastYear': b.SalesLastYear,
                    'ModifiedDate': b.ModifiedDate
               NULL ON NULL)
        FROM #b b
        INNER JOIN (
            SELECT BusinessEntityID, COUNT(*) AS cnt
            FROM #b
            GROUP BY BusinessEntityID
            HAVING COUNT(*) > 1
        ) d
            ON d.BusinessEntityID = b.BusinessEntityID;

        ---------------------------------------------------------
        -- RULE 3: TerritoryID NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 3: TerritoryID NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'TerritoryID NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'TerritoryID is NULL',
               'warning'
        FROM #b b
        WHERE b.TerritoryID IS NULL;

        ---------------------------------------------------------
        -- RULE 4: CommissionPct NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 4: CommissionPct NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'CommissionPct NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'CommissionPct is NULL',
               'warning'
        FROM #b b
        WHERE b.CommissionPct IS NULL;

        ---------------------------------------------------------
        -- RULE 5: SalesQuota NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 5: SalesQuota NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesQuota NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesQuota is NULL',
               'warning'
        FROM #b b
        WHERE b.SalesQuota IS NULL;

        ---------------------------------------------------------
        -- RULE 6: Bonus NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 6: Bonus NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Bonus NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Bonus is NULL',
               'warning'
        FROM #b b
        WHERE b.Bonus IS NULL;

        ---------------------------------------------------------
        -- RULE 7: SalesYTD NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 7: SalesYTD NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesYTD NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesYTD is NULL',
               'warning'
        FROM #b b
        WHERE b.SalesYTD IS NULL;

        ---------------------------------------------------------
        -- RULE 8: SalesLastYear NULL -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 8: SalesLastYear NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesLastYear NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesLastYear is NULL',
               'warning'
        FROM #b b
        WHERE b.SalesLastYear IS NULL;


        ---------------------------------------------------------
        -- RULE 9: Employee not found in Silver -> error (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 9: Employee not found (error)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Employee not found',
               CAST(x.BusinessEntityID AS NVARCHAR(200)),
               CONCAT('BusinessEntityID ', x.BusinessEntityID, ' does not exist in silver.erp_employee - Silver uses -1'),
               'error'
        FROM #b AS x
        WHERE x.BusinessEntityID IS NOT NULL                 -- a NULL is not a broken relationship
          AND NOT EXISTS (SELECT 1
                          FROM silver.erp_employee AS p
                          WHERE p.business_entity_id = x.BusinessEntityID);

        ---------------------------------------------------------
        -- RULE 10: Territory not found in Silver -> error (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 10: Territory not found (error)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Territory not found',
               CAST(x.BusinessEntityID AS NVARCHAR(200)),
               CONCAT('TerritoryID ', x.TerritoryID, ' does not exist in silver.erp_sales_territory - Silver uses -1'),
               'error'
        FROM #b AS x
        WHERE x.TerritoryID IS NOT NULL                 -- a NULL is not a broken relationship
          AND NOT EXISTS (SELECT 1
                          FROM silver.erp_sales_territory AS p
                          WHERE p.territory_id = x.TerritoryID);

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_person'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_person';

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
        PRINT 'Bronze Data Quality – sales_person completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
        PRINT 'ERROR during Bronze DQ check for: sales_person';
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
