/*
=========================================================
Bronze Data Quality Check – State Province
=========================================================
Purpose:
    Validates bronze.erp_state_province before the Silver
    load. StateProvinceID NULL or duplicated and
    CountryRegionCode NULL are blocking errors; Name NULL or
    empty and TerritoryID NULL are warnings.

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
    finding is logged in the error event schema; critical
    findings are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_state_province]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @rows_checked     INT = 0,
        @rows_rejected    INT = 0,
        @errors           INT = 0,
        @reject_rate      DECIMAL(9,4) = 0;

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
            @object_name    = 'state_province',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality – state_province | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                 WHERE object_name = 'state_province');

            DROP TABLE IF EXISTS #b;

            SELECT *
            INTO #b
            FROM (
                SELECT b.*,
                       DENSE_RANK() OVER (PARTITION BY b.StateProvinceID
                                          ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
                FROM bronze.erp_state_province AS b
                WHERE b.dwh_batch_id > @last_silver_batch_id
            ) AS pending
            WHERE pending.dwh_version_rank = 1
              AND pending.dwh_operation <> 'D';

            SELECT @rows_checked = COUNT(*)
            FROM #b;

            -- The errors found by the screens (saved at the end by etl.usp_save_error_events)
            DROP TABLE IF EXISTS #errors;
            CREATE TABLE #errors (row_id INT IDENTITY(1,1), screen_name NVARCHAR(200), screen_type VARCHAR(20),
                                  severity VARCHAR(20), exception_action VARCHAR(20), fields NVARCHAR(400),
                                  record_id NVARCHAR(200), error_condition NVARCHAR(500));

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
                VALUES ('Source empty', 'business rule', 'warning', 'pass', '', NULL,
                        'No Bronze rows to check: nothing new, or only deletions');
            END

            ---------------------------------------------------------
            -- RULE 1: StateProvinceID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 1: StateProvinceID NULL';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'StateProvinceID NULL', 'column', 'critical', 'set aside', '',
                   NULL,
                   'StateProvinceID is NULL'
            FROM #b
            WHERE StateProvinceID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'state_province',
                   NULL,
                   'NULL_BK',
                   'Business key StateProvinceID is NULL',
                   'critical',
                   JSON_OBJECT(
                        'StateProvinceID': b.StateProvinceID,
                        'StateProvinceCode': b.StateProvinceCode,
                        'CountryRegionCode': b.CountryRegionCode,
                        'Name': b.Name,
                        'TerritoryID': b.TerritoryID,
                        'ModifiedDate': b.ModifiedDate
                        NULL ON NULL
                   )
            FROM #b b
            WHERE b.StateProvinceID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate StateProvinceID -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate StateProvinceID';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'Duplicate StateProvinceID', 'structure', 'critical', 'set aside', '',
                   CAST(StateProvinceID AS NVARCHAR(200)),
                   'Duplicate StateProvinceID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows'
            FROM #b
            GROUP BY StateProvinceID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'state_province',
                   CAST(b.StateProvinceID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate StateProvinceID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'critical',
                   JSON_OBJECT(
                        'StateProvinceID': b.StateProvinceID,
                        'StateProvinceCode': b.StateProvinceCode,
                        'CountryRegionCode': b.CountryRegionCode,
                        'Name': b.Name,
                        'TerritoryID': b.TerritoryID,
                        'ModifiedDate': b.ModifiedDate
                        NULL ON NULL
                   )
            FROM #b b
            INNER JOIN (
                SELECT StateProvinceID, COUNT(*) AS cnt
                FROM #b
                GROUP BY StateProvinceID
                HAVING COUNT(*) > 1
            ) d
                ON d.StateProvinceID = b.StateProvinceID;

            ---------------------------------------------------------
            -- RULE 3: CountryRegionCode NULL -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 3: CountryRegionCode NULL';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'CountryRegionCode NULL', 'column', 'error', 'pass', '',
                   CAST(StateProvinceID AS NVARCHAR(200)),
                   'CountryRegionCode is NULL'
            FROM #b
            WHERE CountryRegionCode IS NULL;

            ---------------------------------------------------------
            -- RULE 4: Name NULL or empty -> warning (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 4: Name NULL or empty (warning)';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'Name NULL or empty', 'column', 'warning', 'pass', '',
                   CAST(StateProvinceID AS NVARCHAR(200)),
                   'Name is NULL or empty - Silver substitutes ''Unknown'''
            FROM #b
            WHERE Name IS NULL
               OR LTRIM(RTRIM(Name)) = '';

            ---------------------------------------------------------
            -- RULE 5: TerritoryID NULL -> warning (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 5: TerritoryID NULL (warning)';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'TerritoryID NULL', 'column', 'warning', 'pass', '',
                   CAST(StateProvinceID AS NVARCHAR(200)),
                   'TerritoryID is NULL - optional foreign key'
            FROM #b
            WHERE TerritoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 6: Country not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 6: Country not found (error)';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'Country not found', 'structure', 'error', 'pass', 'CountryRegionCode',
                   CAST(x.StateProvinceID AS NVARCHAR(200)),
                   CONCAT('CountryRegionCode ', x.CountryRegionCode, ' does not exist in silver.erp_country_region - Silver uses -1')
            FROM #b AS x
            WHERE x.CountryRegionCode IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_country_region AS p
                              WHERE p.country_code = x.CountryRegionCode);

            ---------------------------------------------------------
            -- RULE 7: Territory not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 7: Territory not found (error)';

            INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
            SELECT 'Territory not found', 'structure', 'error', 'pass', 'TerritoryID',
                   CAST(x.StateProvinceID AS NVARCHAR(200)),
                   CONCAT('TerritoryID ', x.TerritoryID, ' does not exist in silver.erp_sales_territory - Silver uses -1')
            FROM #b AS x
            WHERE x.TerritoryID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_sales_territory AS p
                              WHERE p.territory_id = x.TerritoryID);

            -- Save the errors in the error event schema (same transaction as the screens)
            SELECT @errors = COUNT(*) FROM #errors WHERE severity IN ('error', 'critical');

            EXEC etl.usp_save_error_events
                 @batch_id    = @batch_id,
                 @layer       = 'bronze',
                 @object_name = 'state_province',
                 @etl_module  = @proc_name;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'state_province';

        IF @rows_checked > 0
            SET @reject_rate = CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

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
        PRINT 'Bronze Data Quality – state_province completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
        PRINT 'ERROR during Bronze DQ check for: state_province';
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
