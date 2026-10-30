/*
=========================================================
Bronze Data Quality Check – Sales Order Header
=========================================================
Purpose:
    Validates bronze.erp_sales_order_header (FACT) before
    the Silver load. It checks for NULL or duplicated
    SalesOrderID, NULL OrderDate, inconsistent dates,
    negative amounts, TotalDue mismatch and missing
    mandatory foreign keys (CustomerID, ShipMethodID).

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
    finding is logged in etl.data_quality_log; critical
    findings are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025]
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_sales_order_header]
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
            @object_name    = 'sales_order_header',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_order_header | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                 WHERE object_name = 'sales_order_header');

            DROP TABLE IF EXISTS #b;

            SELECT *
            INTO #b
            FROM (
                SELECT b.*,
                       DENSE_RANK() OVER (PARTITION BY b.SalesOrderID
                                          ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
                FROM bronze.erp_sales_order_header AS b
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
                    (@batch_id, 'bronze', 'sales_order_header', 'Source empty', NULL,
                     'No Bronze rows to check: nothing new, or only deletions', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: SalesOrderID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 1: SalesOrderID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'SalesOrderID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'SalesOrderID is NULL',
                   'critical'
            FROM #b AS b
            WHERE b.SalesOrderID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'NULL_BK',
                   'SalesOrderID is NULL',
                   'critical',
                    JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'RevisionNumber': b.RevisionNumber,
                        'OrderDate': b.OrderDate,
                        'DueDate': b.DueDate,
                        'ShipDate': b.ShipDate,
                        'Status': b.[Status],
                        'OnlineOrderFlag': b.OnlineOrderFlag,
                        'SalesOrderNumber': b.SalesOrderNumber,
                        'PurchaseOrderNumber': b.PurchaseOrderNumber,
                        'AccountNumber': b.AccountNumber,
                        'CustomerID': b.CustomerID,
                        'SalesPersonID': b.SalesPersonID,
                        'TerritoryID': b.TerritoryID,
                        'BillToAddressID': b.BillToAddressID,
                        'ShipToAddressID': b.ShipToAddressID,
                        'ShipMethodID': b.ShipMethodID,
                        'CreditCardID': b.CreditCardID,
                        'CreditCardApprovalCode': b.CreditCardApprovalCode,
                        'CurrencyRateID': b.CurrencyRateID,
                        'SubTotal': b.SubTotal,
                        'TaxAmt': b.TaxAmt,
                        'Freight': b.Freight,
                        'TotalDue': b.TotalDue,
                        'Comment': b.Comment,
                        'rowguid': b.rowguid,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                    )
            FROM #b AS b
            WHERE b.SalesOrderID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate SalesOrderID -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate SalesOrderID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Duplicate SalesOrderID',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'Duplicate SalesOrderID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'critical'
            FROM #b AS b
            GROUP BY b.SalesOrderID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate SalesOrderID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'critical',
                    JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'RevisionNumber': b.RevisionNumber,
                        'OrderDate': b.OrderDate,
                        'DueDate': b.DueDate,
                        'ShipDate': b.ShipDate,
                        'Status': b.[Status],
                        'OnlineOrderFlag': b.OnlineOrderFlag,
                        'SalesOrderNumber': b.SalesOrderNumber,
                        'PurchaseOrderNumber': b.PurchaseOrderNumber,
                        'AccountNumber': b.AccountNumber,
                        'CustomerID': b.CustomerID,
                        'SalesPersonID': b.SalesPersonID,
                        'TerritoryID': b.TerritoryID,
                        'BillToAddressID': b.BillToAddressID,
                        'ShipToAddressID': b.ShipToAddressID,
                        'ShipMethodID': b.ShipMethodID,
                        'CreditCardID': b.CreditCardID,
                        'CreditCardApprovalCode': b.CreditCardApprovalCode,
                        'CurrencyRateID': b.CurrencyRateID,
                        'SubTotal': b.SubTotal,
                        'TaxAmt': b.TaxAmt,
                        'Freight': b.Freight,
                        'TotalDue': b.TotalDue,
                        'Comment': b.Comment,
                        'rowguid': b.rowguid,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                    )
            FROM #b AS b
            INNER JOIN (
                SELECT SalesOrderID, COUNT(*) AS cnt
                FROM #b
                GROUP BY SalesOrderID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.SalesOrderID = b.SalesOrderID;

            ---------------------------------------------------------
            -- RULE 3: OrderDate NULL -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 3: OrderDate NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'OrderDate NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'OrderDate is NULL',
                   'error'
            FROM #b AS b
            WHERE b.OrderDate IS NULL;

            ---------------------------------------------------------
            -- RULE 4: DueDate < OrderDate -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 4: DueDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'DueDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'DueDate is earlier than OrderDate',
                   'error'
            FROM #b AS b
            WHERE b.DueDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 5: ShipDate < OrderDate -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 5: ShipDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ShipDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ShipDate is earlier than OrderDate',
                   'error'
            FROM #b AS b
            WHERE b.ShipDate IS NOT NULL
              AND b.ShipDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 6: ModifiedDate < OrderDate -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 6: ModifiedDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ModifiedDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ModifiedDate is earlier than OrderDate',
                   'error'
            FROM #b AS b
            WHERE b.ModifiedDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 7: Numeric fields < 0 -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 7: Numeric fields < 0';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Negative numeric value',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'One or more numeric fields are negative',
                   'error'
            FROM #b AS b
            WHERE b.SubTotal < 0
               OR b.TaxAmt < 0
               OR b.Freight < 0;

            ---------------------------------------------------------
            -- RULE 8: TotalDue mismatch -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 8: TotalDue mismatch';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'TotalDue mismatch',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'TotalDue <> SubTotal + TaxAmt + Freight',
                   'error'
            FROM #b AS b
            WHERE b.TotalDue <> (b.SubTotal + b.TaxAmt + b.Freight);

            ---------------------------------------------------------
            -- RULE 9: CustomerID NULL -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 9: CustomerID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'CustomerID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'CustomerID is NULL',
                   'error'
            FROM #b AS b
            WHERE b.CustomerID IS NULL;

            ---------------------------------------------------------
            -- RULE 10: ShipMethodID NULL -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 10: ShipMethodID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ShipMethodID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ShipMethodID is NULL',
                   'error'
            FROM #b AS b
            WHERE b.ShipMethodID IS NULL;


            ---------------------------------------------------------
            -- RULE 11: Customer not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 11: Customer not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Customer not found',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('CustomerID ', x.CustomerID, ' does not exist in silver.erp_customer - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.CustomerID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_customer AS p
                              WHERE p.customer_id = x.CustomerID);

            ---------------------------------------------------------
            -- RULE 12: Sales person not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 12: Sales person not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Sales person not found',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('SalesPersonID ', x.SalesPersonID, ' does not exist in silver.erp_sales_person - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.SalesPersonID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_sales_person AS p
                              WHERE p.person_id = x.SalesPersonID);

            ---------------------------------------------------------
            -- RULE 13: Territory not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 13: Territory not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Territory not found',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('TerritoryID ', x.TerritoryID, ' does not exist in silver.erp_sales_territory - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.TerritoryID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_sales_territory AS p
                              WHERE p.territory_id = x.TerritoryID);

            ---------------------------------------------------------
            -- RULE 14: Ship method not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 14: Ship method not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Ship method not found',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('ShipMethodID ', x.ShipMethodID, ' does not exist in silver.erp_ship_method - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.ShipMethodID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_ship_method AS p
                              WHERE p.ship_method_id = x.ShipMethodID);


            ---------------------------------------------------------
            -- RULE 15: Status outside 1-6 -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 15: Status outside 1-6 (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Status invalid',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('Status ', x.Status, ' is not between 1 and 6'),
                   'error'
            FROM #b AS x
            WHERE x.Status NOT BETWEEN 1 AND 6;

            ---------------------------------------------------------
            -- RULE 16: OrderDate in the future -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 16: OrderDate in the future (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'OrderDate in the future',
                   CAST(x.SalesOrderID AS NVARCHAR(200)),
                   CONCAT('OrderDate ', CONVERT(VARCHAR(10), x.OrderDate, 23), ' is after today'),
                   'error'
            FROM #b AS x
            WHERE x.OrderDate > SYSDATETIME();

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_order_header'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_order_header';

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
            THROW 51011, 'Rejection rate above threshold: SALES ORDER HEADER source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_order_header completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
        PRINT 'ERROR during Bronze DQ check for: sales_order_header';
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
