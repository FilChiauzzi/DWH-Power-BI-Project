/*
=========================================================
Bronze Data Quality Check – Product
=========================================================
Purpose:
    Validates bronze.erp_product before the Silver load. It
    checks for NULL or duplicated ProductID, NULL or empty
    Name and ProductNumber, NULL SellStartDate, NULL
    ProductSubcategoryID and negative numeric values.

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

        IF @rows_checked = 0
        BEGIN
            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            VALUES
                (@batch_id, 'bronze', 'product', 'Source empty', NULL,
                 'No Bronze rows to check: nothing new, or only deletions', 'warning');
        END

        ---------------------------------------------------------
        -- RULE 1: ProductID NULL (critical)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'ProductID NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductID is NULL',
               'critical'
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
        -- RULE 2: Duplicate ProductID (critical)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Duplicate ProductID',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Duplicate ProductID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
               'critical'
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
        -- RULE 3: Name NULL or empty (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Name NULL or empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Name is NULL or empty',
               'error'
        FROM #b AS p
        WHERE p.Name IS NULL
           OR LTRIM(RTRIM(p.Name)) = '';

        ---------------------------------------------------------
        -- RULE 4: ProductNumber NULL or empty (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'ProductNumber NULL or empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductNumber is NULL or empty',
               'error'
        FROM #b AS p
        WHERE p.ProductNumber IS NULL
           OR LTRIM(RTRIM(p.ProductNumber)) = '';

        ---------------------------------------------------------
        -- RULE 5: SellStartDate NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'SellStartDate NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'SellStartDate is NULL',
               'error'
        FROM #b AS p
        WHERE p.SellStartDate IS NULL;

        ---------------------------------------------------------
        -- RULE 6: ProductSubcategoryID NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'SubCategoryID NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductSubcategoryID is NULL',
               'error'
        FROM #b AS p
        WHERE p.ProductSubcategoryID IS NULL;

        ---------------------------------------------------------
        -- RULE 7: Numeric fields NULL or negative (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Negative or NULL numeric value',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more numeric fields are negative or NULL',
               'error'
        FROM #b AS p
        WHERE p.StandardCost < 0 OR p.StandardCost IS NULL
           OR p.ListPrice < 0 OR p.ListPrice IS NULL
           OR p.SafetyStockLevel < 0 OR p.SafetyStockLevel IS NULL
           OR p.ReorderPoint < 0 OR p.ReorderPoint IS NULL
           OR p.Weight < 0 OR p.Weight IS NULL
           OR p.DaysToManufacture < 0 OR p.DaysToManufacture IS NULL;

        ---------------------------------------------------------
        -- RULE 8: UnitMeasureCode NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'UnitMeasureCode NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'SizeUnitMeasureCode or WeightUnitMeasureCode is NULL',
               'error'
        FROM #b AS p
        WHERE p.SizeUnitMeasureCode IS NULL
           OR p.WeightUnitMeasureCode IS NULL;

        ---------------------------------------------------------
        -- RULE 9: Descriptive fields empty (warning)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Descriptive field empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more descriptive fields are empty',
               'warning'
        FROM #b AS p
        WHERE LTRIM(RTRIM(p.Color)) = ''
           OR LTRIM(RTRIM(p.Size)) = ''
           OR LTRIM(RTRIM(p.ProductLine)) = ''
           OR LTRIM(RTRIM(p.Class)) = ''
           OR LTRIM(RTRIM(p.Style)) = '';

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'product'
          AND severity IN ('error','critical');

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

