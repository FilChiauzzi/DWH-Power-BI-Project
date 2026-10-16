/*
=========================================================
Bronze Data Quality Check – SalesOrderHeader (FACT)
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the SalesOrderHeader
    domain (FACT). It ensures that all incoming raw records
    from the OLTP source meet the minimum quality standards
    before being consumed by the Silver FACT load.

    The procedure checks for null or invalid business keys,
    inconsistent dates, negative numeric values, mismatched
    totals, missing mandatory foreign keys, and other
    structural or technical anomalies. Any invalid record is
    logged and quarantined for further inspection.

Pipeline Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure
    before loading the Silver layer. All detected issues are
    logged into etl.data_quality_log and invalid rows are
    stored in etl.rejected with the full raw JSON payload.

Incremental Strategy:
    The Bronze layer is loaded using a watermark-based
    incremental strategy. Data Quality checks are always
    executed on the Bronze table itself (FULL validation),
    regardless of whether new changes were detected. CDC is
    not used at this stage; it is reserved exclusively for
    the Silver FACT incremental load.

Warning:
    Running this procedure will delete previous quality logs
    for the SalesOrderHeader object in the Bronze layer.
    Ensure that downstream processes do not rely on older
    Bronze-level quality entries.
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

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_order_header | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_sales_order_header;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'sales_order_header', 'Source empty', NULL,
                     'bronze.erp_sales_order_header contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: SalesOrderID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: SalesOrderID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'SalesOrderID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'SalesOrderID is NULL',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.SalesOrderID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'NULL_BK',
                   'SalesOrderID is NULL',
                   'error',
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
            FROM bronze.erp_sales_order_header AS b
            WHERE b.SalesOrderID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate SalesOrderID -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate SalesOrderID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Duplicate SalesOrderID',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'Duplicate SalesOrderID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            GROUP BY b.SalesOrderID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate SalesOrderID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
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
            FROM bronze.erp_sales_order_header AS b
            INNER JOIN (
                SELECT SalesOrderID, COUNT(*) AS cnt
                FROM bronze.erp_sales_order_header
                GROUP BY SalesOrderID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.SalesOrderID = b.SalesOrderID;

            ---------------------------------------------------------
            -- RULE 3: OrderDate NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 3: OrderDate NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'OrderDate NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'OrderDate is NULL',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.OrderDate IS NULL;

            ---------------------------------------------------------
            -- RULE 4: DueDate < OrderDate -> error
            ---------------------------------------------------------
            PRINT '-- Rule 4: DueDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'DueDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'DueDate is earlier than OrderDate',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.DueDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 5: ShipDate < OrderDate -> error
            ---------------------------------------------------------
            PRINT '-- Rule 5: ShipDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ShipDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ShipDate is earlier than OrderDate',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.ShipDate IS NOT NULL
              AND b.ShipDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 6: ModifiedDate < OrderDate -> error
            ---------------------------------------------------------
            PRINT '-- Rule 6: ModifiedDate < OrderDate';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ModifiedDate < OrderDate',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ModifiedDate is earlier than OrderDate',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.ModifiedDate < b.OrderDate;

            ---------------------------------------------------------
            -- RULE 7: Numeric fields < 0 -> error
            ---------------------------------------------------------
            PRINT '-- Rule 7: Numeric fields < 0';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'Negative numeric value',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'One or more numeric fields are negative',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.SubTotal < 0
               OR b.TaxAmt < 0
               OR b.Freight < 0;

            ---------------------------------------------------------
            -- RULE 8: TotalDue mismatch -> error
            ---------------------------------------------------------
            PRINT '-- Rule 8: TotalDue mismatch';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'TotalDue mismatch',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'TotalDue <> SubTotal + TaxAmt + Freight',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.TotalDue <> (b.SubTotal + b.TaxAmt + b.Freight);

            ---------------------------------------------------------
            -- RULE 9: CustomerID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 9: CustomerID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'CustomerID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'CustomerID is NULL',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.CustomerID IS NULL;

            ---------------------------------------------------------
            -- RULE 10: ShipMethodID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 10: ShipMethodID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_header',
                   'ShipMethodID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'ShipMethodID is NULL',
                   'error'
            FROM bronze.erp_sales_order_header AS b
            WHERE b.ShipMethodID IS NULL;

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

        THROW;
    END CATCH
END;
GO
