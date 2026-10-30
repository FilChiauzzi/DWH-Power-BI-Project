/*
=========================================================
Bronze Data Quality Check – Product
=========================================================
Purpose:
    Validates bronze.erp_product before the Silver load. It
    checks for NULL or duplicated ProductID, NULL or empty
    Name and ProductNumber, NULL SellStartDate, NULL
    ProductSubcategoryID and negative numeric values.

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
    Business rule screens (point 5 of subsystem 4) test values
    against each other: dates in the right order, codes in their
    allowed range, a customer that is a person or a store.
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

USE [DWH_AdventureWorks2025]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_product]
    @batch_id BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start_time DATETIME2(3) = SYSDATETIME(),
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
            @object_name    = 'product',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality – product | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
             WHERE object_name = 'product');

        DROP TABLE IF EXISTS #b;

        SELECT *
        INTO #b
        FROM (
            SELECT b.*,
                   DENSE_RANK() OVER (PARTITION BY b.ProductID
                                      ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
            FROM bronze.erp_product AS b
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
        -- RULE 1: ProductID NULL -> critical (row set aside)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'ProductID NULL', 'column', 'critical', 'set aside', 'ProductID',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductID is NULL'
        FROM #b AS p
        WHERE p.ProductID IS NULL;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'product',
               CAST(p.ProductID AS NVARCHAR(200)),
               'NULL_BK',
               'Business key ProductID is NULL',
               'critical',
               JSON_OBJECT('ProductID': p.ProductID,
                           'Name': p.Name,
                           'ProductNumber': p.ProductNumber,
                           'ProductSubcategoryID': p.ProductSubcategoryID
                           NULL ON NULL)
        FROM #b AS p
        WHERE p.ProductID IS NULL;

        ---------------------------------------------------------
        -- RULE 2: Duplicate ProductID -> critical (row set aside)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Duplicate ProductID', 'structure', 'critical', 'set aside', 'ProductID',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Duplicate ProductID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows'
        FROM #b AS p
        GROUP BY p.ProductID
        HAVING COUNT(*) > 1;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'product',
               CAST(p.ProductID AS NVARCHAR(200)),
               'DUP_BK',
               'Duplicate ProductID detected',
               'critical',
               JSON_OBJECT('ProductID': p.ProductID,
                           'Name': p.Name,
                           'ProductNumber': p.ProductNumber,
                           'ProductSubcategoryID': p.ProductSubcategoryID
                           NULL ON NULL)
        FROM #b AS p
        INNER JOIN (
            SELECT ProductID
            FROM #b
            GROUP BY ProductID
            HAVING COUNT(*) > 1
        ) AS d
            ON d.ProductID = p.ProductID;

        ---------------------------------------------------------
        -- RULE 3: Name NULL or empty -> error (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Name NULL or empty', 'column', 'error', 'pass', 'Name',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Name is NULL or empty'
        FROM #b AS p
        WHERE p.Name IS NULL
           OR LTRIM(RTRIM(p.Name)) = '';

        ---------------------------------------------------------
        -- RULE 4: ProductNumber NULL or empty -> error (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'ProductNumber NULL or empty', 'column', 'error', 'pass', 'ProductNumber',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductNumber is NULL or empty'
        FROM #b AS p
        WHERE p.ProductNumber IS NULL
           OR LTRIM(RTRIM(p.ProductNumber)) = '';

        ---------------------------------------------------------
        -- RULE 5: SellStartDate NULL -> error (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'SellStartDate NULL', 'column', 'error', 'pass', 'SellStartDate',
               CAST(p.ProductID AS NVARCHAR(200)),
               'SellStartDate is NULL'
        FROM #b AS p
        WHERE p.SellStartDate IS NULL;

        ---------------------------------------------------------
        -- RULE 6: Numeric fields NULL or negative -> error (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Negative or NULL numeric value', 'column', 'error', 'pass', 'StandardCost,ListPrice,SafetyStockLevel,ReorderPoint,Weight,DaysToManufacture',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more numeric fields are negative or NULL'
        FROM #b AS p
        WHERE p.StandardCost < 0 OR p.StandardCost IS NULL
           OR p.ListPrice < 0 OR p.ListPrice IS NULL
           OR p.SafetyStockLevel < 0 OR p.SafetyStockLevel IS NULL
           OR p.ReorderPoint < 0 OR p.ReorderPoint IS NULL
           OR p.Weight < 0                       -- a NULL weight is normal (no weight)
           OR p.DaysToManufacture < 0 OR p.DaysToManufacture IS NULL;

        ---------------------------------------------------------
        -- RULE 7: Weight without unit of measure -> error (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Weight without unit', 'column', 'error', 'pass', 'Weight,WeightUnitMeasureCode',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Weight is filled but WeightUnitMeasureCode is NULL'
        FROM #b AS p
        WHERE p.Weight IS NOT NULL
           AND p.WeightUnitMeasureCode IS NULL;

        ---------------------------------------------------------
        -- RULE 8: Descriptive fields empty -> warning (row passes)
        ---------------------------------------------------------
        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Descriptive field empty', 'column', 'warning', 'pass', 'Color,Size,ProductLine,Class,Style',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more descriptive fields are empty'
        FROM #b AS p
        WHERE LTRIM(RTRIM(p.Color)) = ''
           OR LTRIM(RTRIM(p.Size)) = ''
           OR LTRIM(RTRIM(p.ProductLine)) = ''
           OR LTRIM(RTRIM(p.Class)) = ''
           OR LTRIM(RTRIM(p.Style)) = '';

        ---------------------------------------------------------
        -- RULE 9: Subcategory not found in Silver -> error (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 9: Subcategory not found (error)';

        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Subcategory not found', 'structure', 'error', 'pass', 'ProductSubcategoryID',
               CAST(x.ProductID AS NVARCHAR(200)),
               CONCAT('ProductSubcategoryID ', x.ProductSubcategoryID, ' does not exist in silver.erp_subcategory - Silver uses -1')
        FROM #b AS x
        WHERE x.ProductSubcategoryID IS NOT NULL                 -- a NULL is not a broken relationship
          AND NOT EXISTS (SELECT 1
                          FROM silver.erp_subcategory AS p
                          WHERE p.subcategory_id = x.ProductSubcategoryID);

        ---------------------------------------------------------
        -- RULE 10: SellEndDate before SellStartDate -> error (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 10: SellEndDate before SellStartDate (error)';

        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Sell dates inverted', 'business rule', 'error', 'pass', 'SellEndDate,SellStartDate',
               CAST(x.ProductID AS NVARCHAR(200)),
               'SellEndDate is before SellStartDate'
        FROM #b AS x
        WHERE x.SellEndDate < x.SellStartDate;

        ---------------------------------------------------------
        -- RULE 11: ListPrice below StandardCost -> warning (row passes)
        ---------------------------------------------------------
        PRINT '-- Rule 11: ListPrice below StandardCost (warning)';

        INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
        SELECT 'Price below cost', 'column', 'warning', 'pass', 'ListPrice,StandardCost',
               CAST(x.ProductID AS NVARCHAR(200)),
               CONCAT('ListPrice ', x.ListPrice, ' is below StandardCost ', x.StandardCost)
        FROM #b AS x
        WHERE x.ListPrice > 0 AND x.ListPrice < x.StandardCost;

            -- Save the errors in the error event schema (same transaction as the screens)
            SELECT @errors = COUNT(*) FROM #errors WHERE severity IN ('error', 'critical');

            EXEC etl.usp_save_error_events
                 @batch_id    = @batch_id,
                 @layer       = 'bronze',
                 @object_name = 'product',
                 @etl_module  = @proc_name;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'product';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';

        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality – product completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
        PRINT 'ERROR during Bronze DQ check for: product';
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

