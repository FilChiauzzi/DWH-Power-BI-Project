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
            @object_name    = 'sales_order_detail',
            @step_name      = 'dq_check',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_order_detail | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                 WHERE object_name = 'sales_order_detail');

            DROP TABLE IF EXISTS #b;

            SELECT *
            INTO #b
            FROM (
                SELECT b.*,
                       DENSE_RANK() OVER (PARTITION BY b.SalesOrderID, b.SalesOrderDetailID
                                          ORDER BY b.dwh_batch_id DESC) AS dwh_version_rank
                FROM bronze.erp_sales_order_detail AS b
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
                    (@batch_id, 'bronze', 'sales_order_detail', 'Source empty', NULL,
                     'No Bronze rows to check: nothing new, or only deletions', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: SalesOrderID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 1: SalesOrderID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'SalesOrderID NULL',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'SalesOrderID is NULL',
                   'critical'
            FROM #b AS b
            WHERE b.SalesOrderID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   CAST(b.SalesOrderID AS NVARCHAR(200)),
                   'NULL_BK',
                   'SalesOrderID is NULL',
                   'critical',
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
            FROM #b AS b
            WHERE b.SalesOrderID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: SalesOrderDetailID NULL -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 2: SalesOrderDetailID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'SalesOrderDetailID NULL',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'SalesOrderDetailID is NULL',
                   'critical'
            FROM #b AS b
            WHERE b.SalesOrderDetailID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'NULL_LINE',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'SalesOrderDetailID is NULL',
                   'critical',
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
            FROM #b AS b
            WHERE b.SalesOrderDetailID IS NULL;

            ---------------------------------------------------------
            -- RULE 3: Duplicate (SalesOrderID, SalesOrderDetailID) -> critical (row set aside)
            ---------------------------------------------------------
            PRINT '-- Rule 3: Duplicate SalesOrderID + SalesOrderDetailID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'Duplicate line',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'Duplicate SalesOrderID + SalesOrderDetailID',
                   'critical'
            FROM (
                SELECT SalesOrderID, SalesOrderDetailID
                FROM #b
                GROUP BY SalesOrderID, SalesOrderDetailID
                HAVING COUNT(*) > 1
            ) d
            JOIN #b b
                ON b.SalesOrderID = d.SalesOrderID
               AND b.SalesOrderDetailID = d.SalesOrderDetailID;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'DUP_LINE',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'Duplicate SalesOrderID + SalesOrderDetailID',
                   'critical',
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
                FROM #b
                GROUP BY SalesOrderID, SalesOrderDetailID
                HAVING COUNT(*) > 1
            ) d
            JOIN #b b
                ON b.SalesOrderID = d.SalesOrderID
               AND b.SalesOrderDetailID = d.SalesOrderDetailID;

            ---------------------------------------------------------
            -- RULE 4: OrderQty <= 0 -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 4: OrderQty <= 0';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'OrderQty invalid',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'OrderQty <= 0',
                   'error'
            FROM #b b
            WHERE b.OrderQty <= 0;

            ---------------------------------------------------------
            -- RULE 5: UnitPrice < 0 OR UnitPriceDiscount < 0 -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 5: Negative prices';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'Negative price',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'UnitPrice or UnitPriceDiscount < 0',
                   'error'
            FROM #b b
            WHERE b.UnitPrice < 0
               OR b.UnitPriceDiscount < 0;

            ---------------------------------------------------------
            -- RULE 6: LineTotal mismatch -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 6: LineTotal mismatch';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'LineTotal mismatch',
                   CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID),
                   'LineTotal <> UnitPrice * (1 - UnitPriceDiscount) * OrderQty (tolerance 0.01)',
                   'error'
            FROM #b b
            WHERE ABS(b.LineTotal
                      - CAST(b.UnitPrice AS DECIMAL(19,4))
                      * (1 - CAST(b.UnitPriceDiscount AS DECIMAL(9,4)))
                      * b.OrderQty) > 0.01;


            ---------------------------------------------------------
            -- RULE 7: Order header not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 7: Order header not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'Order header not found',
                   CONCAT(x.SalesOrderID, '-', x.SalesOrderDetailID),
                   CONCAT('SalesOrderID ', x.SalesOrderID, ' does not exist in silver.erp_sales_order_header - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.SalesOrderID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_sales_order_header AS p
                              WHERE p.sales_order_id = x.SalesOrderID);

            ---------------------------------------------------------
            -- RULE 8: Product not found in Silver -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 8: Product not found (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'Product not found',
                   CONCAT(x.SalesOrderID, '-', x.SalesOrderDetailID),
                   CONCAT('ProductID ', x.ProductID, ' does not exist in silver.erp_product - Silver uses -1'),
                   'error'
            FROM #b AS x
            WHERE x.ProductID IS NOT NULL                 -- a NULL is not a broken relationship
              AND NOT EXISTS (SELECT 1
                              FROM silver.erp_product AS p
                              WHERE p.product_id = x.ProductID);


            ---------------------------------------------------------
            -- RULE 9: UnitPriceDiscount above 100% -> error (row passes)
            ---------------------------------------------------------
            PRINT '-- Rule 9: UnitPriceDiscount above 100% (error)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_order_detail',
                   'Discount above 100%',
                   CONCAT(x.SalesOrderID, '-', x.SalesOrderDetailID),
                   CONCAT('UnitPriceDiscount ', x.UnitPriceDiscount, ' is above 1 (100%)'),
                   'error'
            FROM #b AS x
            WHERE x.UnitPriceDiscount > 1;

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
        PRINT 'ERROR during Bronze DQ check for: sales_order_detail';
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

