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

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - customer | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_customer;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'customer', 'Source empty', NULL,
                     'bronze.erp_customer contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: CustomerID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 1: CustomerID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'CustomerID NULL',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'CustomerID is NULL',
                   'error'
            FROM bronze.erp_customer b
            WHERE b.CustomerID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'customer',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'NULL_BK',
                   'CustomerID is NULL',
                   'error',
                   JSON_OBJECT(
                       'CustomerID':    b.CustomerID,
                       'PersonID':      b.PersonID,
                       'StoreID':       b.StoreID,
                       'TerritoryID':   b.TerritoryID,
                       'AccountNumber': b.AccountNumber,
                       'ModifiedDate':  b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_customer b
            WHERE b.CustomerID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate CustomerID -> error
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate CustomerID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'Duplicate CustomerID',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'Duplicate CustomerID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_customer b
            GROUP BY b.CustomerID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'customer',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate CustomerID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT(
                       'CustomerID':    b.CustomerID,
                       'PersonID':      b.PersonID,
                       'StoreID':       b.StoreID,
                       'TerritoryID':   b.TerritoryID,
                       'AccountNumber': b.AccountNumber,
                       'ModifiedDate':  b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_customer b
            INNER JOIN (
                SELECT CustomerID, COUNT(*) AS cnt
                FROM bronze.erp_customer
                GROUP BY CustomerID
                HAVING COUNT(*) > 1
            ) d
                ON d.CustomerID = b.CustomerID;

            ---------------------------------------------------------
            -- RULE 3: TerritoryID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 3: TerritoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'customer',
                   'TerritoryID NULL',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'TerritoryID is NULL',
                   'error'
            FROM bronze.erp_customer b
            WHERE b.TerritoryID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'customer',
                   CAST(b.CustomerID AS NVARCHAR(200)),
                   'NULL_TERRITORY',
                   'TerritoryID is NULL',
                   'error',
                   JSON_OBJECT(
                       'CustomerID':    b.CustomerID,
                       'PersonID':      b.PersonID,
                       'StoreID':       b.StoreID,
                       'TerritoryID':   b.TerritoryID,
                       'AccountNumber': b.AccountNumber,
                       'ModifiedDate':  b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_customer b
            WHERE b.TerritoryID IS NULL;

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

        THROW;
    END CATCH
END;
GO
