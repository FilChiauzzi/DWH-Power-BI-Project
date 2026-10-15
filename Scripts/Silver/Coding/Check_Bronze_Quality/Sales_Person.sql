/*
=========================================================
Bronze Data Quality Check – Sales Person
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Sales Person domain.
    It checks for null business keys, duplicate keys, and
    missing or invalid business attributes. Blocking rules
    (errors) prevent the row from entering the Silver layer,
    while non-blocking rules (warnings) allow the row to pass
    but log potential data issues.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure for
    each imported source table. Each procedure logs issues
    into etl.data_quality_log and returns the number of
    detected errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Sales Person object in the Bronze layer.
*/

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

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality – sales_person | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

        ---------------------------------------------------------
        -- Count source rows
        ---------------------------------------------------------
        SELECT @rows_checked = COUNT(*)
        FROM bronze.erp_sales_person;

        ---------------------------------------------------------
        -- RULE 0: source empty -> warning
        ---------------------------------------------------------
        IF @rows_checked = 0
        BEGIN
            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            VALUES
                (@batch_id, 'bronze', 'sales_person', 'Source empty', NULL,
                 'bronze.erp_sales_person contains no rows', 'warning');
        END

        ---------------------------------------------------------
        -- RULE 1: BusinessEntityID NULL -> ERROR
        ---------------------------------------------------------
        PRINT '-- Rule 1: BusinessEntityID NULL';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'BusinessEntityID NULL',
               NULL,
               'BusinessEntityID is NULL',
               'error'
        FROM bronze.erp_sales_person
        WHERE BusinessEntityID IS NULL;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'sales_person',
               NULL,
               'NULL_BK',
               'BusinessEntityID is NULL',
               'error',
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
        FROM bronze.erp_sales_person
        WHERE BusinessEntityID IS NULL;

        ---------------------------------------------------------
        -- RULE 2: Duplicate BusinessEntityID -> ERROR
        ---------------------------------------------------------
        PRINT '-- Rule 2: Duplicate BusinessEntityID';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Duplicate BusinessEntityID',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Duplicate BusinessEntityID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
               'error'
        FROM bronze.erp_sales_person b
        GROUP BY b.BusinessEntityID
        HAVING COUNT(*) > 1;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'sales_person',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'DUP_BK',
               'Duplicate BusinessEntityID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
               'error',
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
        FROM bronze.erp_sales_person b
        INNER JOIN (
            SELECT BusinessEntityID, COUNT(*) AS cnt
            FROM bronze.erp_sales_person
            GROUP BY BusinessEntityID
            HAVING COUNT(*) > 1
        ) d
            ON d.BusinessEntityID = b.BusinessEntityID;

        ---------------------------------------------------------
        -- RULE 3: TerritoryID NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 3: TerritoryID NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'TerritoryID NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'TerritoryID is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.TerritoryID IS NULL;

        ---------------------------------------------------------
        -- RULE 4: CommissionPct NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 4: CommissionPct NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'CommissionPct NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'CommissionPct is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.CommissionPct IS NULL;

        ---------------------------------------------------------
        -- RULE 5: SalesQuota NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 5: SalesQuota NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesQuota NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesQuota is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.SalesQuota IS NULL;

        ---------------------------------------------------------
        -- RULE 6: Bonus NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 6: Bonus NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'Bonus NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Bonus is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.Bonus IS NULL;

        ---------------------------------------------------------
        -- RULE 7: SalesYTD NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 7: SalesYTD NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesYTD NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesYTD is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.SalesYTD IS NULL;

        ---------------------------------------------------------
        -- RULE 8: SalesLastYear NULL -> WARNING
        ---------------------------------------------------------
        PRINT '-- Rule 8: SalesLastYear NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'sales_person',
               'SalesLastYear NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'SalesLastYear is NULL',
               'warning'
        FROM bronze.erp_sales_person b
        WHERE b.SalesLastYear IS NULL;

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

        THROW;
    END CATCH
END;
GO
