/*
=========================================================
Bronze Data Quality Check – Person
=========================================================
Purpose:
    Validates bronze.erp_person before the Silver load.
    BusinessEntityID NULL or duplicated is a blocking error;
    FirstName or LastName NULL or empty is a warning (Silver
    uses 'Unknown').

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

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_person]
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
            @object_name    = 'person',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - person | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                 WHERE object_name = 'person');

            DROP TABLE IF EXISTS #b;

            SELECT *
            INTO #b
            FROM (
                SELECT b.*,
                       DENSE_RANK() OVER (PARTITION BY b.BusinessEntityID
                                          ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
                FROM bronze.erp_person AS b
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
                    (@batch_id, 'bronze', 'person', 'Source empty', NULL,
                     'No Bronze rows to check: nothing new, or only deletions', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: BusinessEntityID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 1: BusinessEntityID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'BusinessEntityID NULL',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'BusinessEntityID is NULL',
                   'critical'
            FROM #b AS b
            WHERE b.BusinessEntityID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'NULL_BK',
                   'BusinessEntityID is NULL',
                   'critical',
                   JSON_OBJECT('BusinessEntityID': b.BusinessEntityID,
                               'FirstName':        b.FirstName,
                               'LastName':         b.LastName,
                               'PersonType':       b.PersonType,
                               'EmailPromotion':   b.EmailPromotion,
                               'rowguid':          b.rowguid,
                               'ModifiedDate':     b.ModifiedDate
                               NULL ON NULL)
            FROM #b AS b
            WHERE b.BusinessEntityID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate BusinessEntityID -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate BusinessEntityID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'Duplicate BusinessEntityID',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'Duplicate BusinessEntityID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'critical'
            FROM #b AS b
            GROUP BY b.BusinessEntityID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate BusinessEntityID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'critical',
                   JSON_OBJECT('BusinessEntityID': b.BusinessEntityID,
                               'FirstName':        b.FirstName,
                               'LastName':         b.LastName,
                               'PersonType':       b.PersonType,
                               'EmailPromotion':   b.EmailPromotion,
                               'rowguid':          b.rowguid,
                               'ModifiedDate':     b.ModifiedDate
                               NULL ON NULL)
            FROM #b AS b
            INNER JOIN (
                SELECT BusinessEntityID, COUNT(*) AS cnt
                FROM #b
                GROUP BY BusinessEntityID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.BusinessEntityID = b.BusinessEntityID;

            ---------------------------------------------------------
            -- RULE 3: FirstName NULL or empty -> warning (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 3: FirstName NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'FirstName NULL or empty',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'FirstName is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM #b AS b
            WHERE b.FirstName IS NULL
               OR LTRIM(RTRIM(b.FirstName)) = '';

            ---------------------------------------------------------
            -- RULE 4: LastName NULL or empty -> warning (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 4: LastName NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'LastName NULL or empty',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'LastName is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM #b AS b
            WHERE b.LastName IS NULL
               OR LTRIM(RTRIM(b.LastName)) = '';

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person';

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
        PRINT 'Bronze Data Quality - person completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
        PRINT 'ERROR during Bronze DQ check for: person';
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
