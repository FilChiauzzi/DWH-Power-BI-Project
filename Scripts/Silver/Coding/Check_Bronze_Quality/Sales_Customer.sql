/*
=========================================================
Bronze Data Quality – customer
=========================================================
Purpose:
    Validates the raw CUSTOMER data before loading it into
    the Silver layer. Ensures structural integrity of the
    business key (CustomerID) and mandatory attributes
    required for dimensional modeling.

Checks implemented:
    RULE 0 – Source empty (warning)
        If the Bronze table contains zero rows, a warning is logged.
        The pipeline continues, but Silver will load nothing.

    RULE 1 – CustomerID NULL (error, blocking)
        CustomerID is the business key and cannot be NULL.
        All rows with NULL CustomerID are logged as errors and
        moved to etl.rejected.

    RULE 2 – Duplicate CustomerID (error, blocking)
        CustomerID must be unique.
        Any CustomerID appearing more than once is logged as an
        error and all duplicate rows are moved to etl.rejected.

    RULE 3 – TerritoryID NULL (error, blocking)
        TerritoryID is a mandatory foreign key to SalesTerritory.
        Rows with NULL TerritoryID are rejected.

Warnings:
    No additional warnings are implemented.
    AccountNumber is not validated because it is not imported
    into Silver and has no impact on dimensional integrity.

Not checked:
    PersonID and StoreID may be NULL (optional foreign keys).
    ModifiedDate may be NULL (not required for SCD2 logic).
    rowguid is not relevant for DW processing.

Rejected rows:
    All blocking errors (NULL BK, duplicate BK, NULL TerritoryID)
    are inserted into etl.rejected with:
        - batch_id
        - layer = 'bronze'
        - object_name = 'customer'
        - business_key
        - rule_code
        - rule_description
        - severity
        - raw_row (JSON_OBJECT)

Severity model:
    error     → row is rejected, pipeline may stop
    warning   → row is accepted but logged

Threshold guard:
    If the percentage of rejected rows exceeds @max_reject_rate
    (default 20%), the stored procedure throws an exception and
    stops the pipeline intentionally. This prevents loading
    corrupted or incomplete source files.

Summary:
    After all checks:
        - rows_checked
        - rows_rejected
        - errors
        - reject_rate (%)
    are printed for monitoring and auditing.

SSIS usage:
    EXEC etl.usp_check_bronze_quality_load_customer ?
        param 0: batch_id
        param 1: max_reject_rate (optional)

Notes:
    This procedure is fully aligned with the Bronze DQ framework
    used for person, store, product, territory, and country_region.
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
