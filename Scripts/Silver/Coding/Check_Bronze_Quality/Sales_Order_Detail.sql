/*
=========================================================
Bronze Data Quality Check – Sales Order Detail
=========================================================
Purpose:
    Validates bronze.erp_sales_order_detail (FACT) before
    the Silver load. It checks for NULL SalesOrderID or
    SalesOrderDetailID, duplicated order lines (the grain of
    the fact), OrderQty not positive, negative UnitPrice or
    UnitPriceDiscount, and LineTotal not consistent with
    price, discount and quantity.

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
CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_sales_order_detail]
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
        PRINT 'Bronze Data Quality - sales_order_detail | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_sales_order_detail;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'sales_order_detail', 'Source empty', NULL,
                     'bronze.erp_sales_order_detail contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: SalesOrderID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: SalesOrderID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'SalesOrderID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'SalesOrderID is NULL',
                   'error'
            FROM bronze.erp_sales_order_detail AS b
            WHERE b.SalesOrderID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'NULL_BK',
                   'SalesOrderID is NULL',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'CarrierTrackingNumber': b.CarrierTrackingNumber,
                        'OrderQty': b.OrderQty,
                        'ProductID': b.ProductID,
                        'SpecialOfferID': b.SpecialOfferID,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'rowguid': b.rowguid,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM bronze.erp_sales_order_detail AS b
            WHERE b.SalesOrderID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: SalesOrderDetailID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 2: SalesOrderDetailID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'SalesOrderDetailID NULL',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'SalesOrderDetailID is NULL',
                   'error'
            FROM bronze.erp_sales_order_detail AS b
            WHERE b.SalesOrderDetailID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'NULL_LINE',
                   'SalesOrderDetailID is NULL',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'CarrierTrackingNumber': b.CarrierTrackingNumber,
                        'OrderQty': b.OrderQty,
                        'ProductID': b.ProductID,
                        'SpecialOfferID': b.SpecialOfferID,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'rowguid': b.rowguid,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM bronze.erp_sales_order_detail AS b
            WHERE b.SalesOrderDetailID IS NULL;

            ---------------------------------------------------------
            -- RULE 3: Duplicate (SalesOrderID, SalesOrderDetailID)
            ---------------------------------------------------------
            PRINT '-- Rule 3: Duplicate SalesOrderID + SalesOrderDetailID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'Duplicate line',
                   'Duplicate SalesOrderID + SalesOrderDetailID',
                   'error'
            FROM (
                SELECT SalesOrderID, SalesOrderDetailID
                FROM bronze.erp_sales_order_detail
                GROUP BY SalesOrderID, SalesOrderDetailID
                HAVING COUNT(*) > 1
            ) d
            JOIN bronze.erp_sales_order_detail b
                ON b.SalesOrderID = d.SalesOrderID
               AND b.SalesOrderDetailID = d.SalesOrderDetailID;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'DUP_LINE',
                   'Duplicate SalesOrderID + SalesOrderDetailID',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'CarrierTrackingNumber': b.CarrierTrackingNumber,
                        'OrderQty': b.OrderQty,
                        'ProductID': b.ProductID,
                        'SpecialOfferID': b.SpecialOfferID,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'rowguid': b.rowguid,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM (
                SELECT SalesOrderID, SalesOrderDetailID
                FROM bronze.erp_sales_order_detail
                GROUP BY SalesOrderID, SalesOrderDetailID
                HAVING COUNT(*) > 1
            ) d
            JOIN bronze.erp_sales_order_detail b
                ON b.SalesOrderID = d.SalesOrderID
               AND b.SalesOrderDetailID = d.SalesOrderDetailID;

            ---------------------------------------------------------
            -- RULE 4: OrderQty <= 0 -> error
            ---------------------------------------------------------
            PRINT '-- Rule 4: OrderQty <= 0';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'OrderQty invalid',
                   'OrderQty <= 0',
                   'error'
            FROM bronze.erp_sales_order_detail b
            WHERE b.OrderQty <= 0;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'QTY_INVALID',
                   'OrderQty <= 0',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'OrderQty': b.OrderQty,
                        'ProductID': b.ProductID,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM bronze.erp_sales_order_detail b
            WHERE b.OrderQty <= 0;

            ---------------------------------------------------------
            -- RULE 5: UnitPrice < 0 OR UnitPriceDiscount < 0
            ---------------------------------------------------------
            PRINT '-- Rule 5: Negative prices';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'Negative price',
                   'UnitPrice or UnitPriceDiscount < 0',
                   'error'
            FROM bronze.erp_sales_order_detail b
            WHERE b.UnitPrice < 0
               OR b.UnitPriceDiscount < 0;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'NEG_PRICE',
                   'UnitPrice or UnitPriceDiscount < 0',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM bronze.erp_sales_order_detail b
            WHERE b.UnitPrice < 0
               OR b.UnitPriceDiscount < 0;

            ---------------------------------------------------------
            -- RULE 6: LineTotal mismatch
            ---------------------------------------------------------
            PRINT '-- Rule 6: LineTotal mismatch';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'LineTotal mismatch',
                   'LineTotal <> UnitPrice * (1 - UnitPriceDiscount) * OrderQty (tolerance 0.01)',
                   'error'
            FROM bronze.erp_sales_order_detail b
            WHERE ABS(b.LineTotal
                      - CAST(b.UnitPrice AS DECIMAL(19,4))
                      * (1 - CAST(b.UnitPriceDiscount AS DECIMAL(9,4)))
                      * b.OrderQty) > 0.01;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'LINE_MISMATCH',
                   'LineTotal <> UnitPrice * (1 - UnitPriceDiscount) * OrderQty (tolerance 0.01)',
                   'error',
                   JSON_OBJECT(
                        'SalesOrderID': b.SalesOrderID,
                        'SalesOrderDetailID': b.SalesOrderDetailID,
                        'OrderQty': b.OrderQty,
                        'UnitPrice': b.UnitPrice,
                        'UnitPriceDiscount': b.UnitPriceDiscount,
                        'LineTotal': b.LineTotal,
                        'ModifiedDate': b.ModifiedDate,
                        'dwh_load_date': b.dwh_load_date,
                        'dwh_batch_id': b.dwh_batch_id,
                        'dwh_source_row_num': b.dwh_source_row_num
                        NULL ON NULL
                   )
            FROM bronze.erp_sales_order_detail b
            WHERE ABS(b.LineTotal
                      - CAST(b.UnitPrice AS DECIMAL(19,4))
                      * (1 - CAST(b.UnitPriceDiscount AS DECIMAL(9,4)))
                      * b.OrderQty) > 0.01;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_order_detail'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_order_detail';

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
            THROW 51011, 'Rejection rate above threshold: SALES ORDER DETAIL source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_order_detail completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: sales_order_detail';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;


