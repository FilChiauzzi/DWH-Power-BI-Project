/*
=========================================================
Silver Load – Sales Order Detail (FACT, SCD1)
=========================================================
Purpose:
    Loads silver.erp_sales_order_detail from
    bronze.erp_sales_order_detail. The key SalesOrderID +
    SalesOrderDetailID is the primary key (no surrogate
    key). It cleans the data, uses a row_hash to detect
    changes and resolves product_sk from the SCD2 product
    version valid at the order date of the Silver header. A
    missing product becomes -1 (Unknown).

    A detail is excluded if its own key or its header is
    quarantined in etl.rejected for the batch. FULL
    truncates and reloads. INCREMENTAL reads the Bronze rows
    loaded after the last Silver load (MAX(dwh_load_date) of
    the Silver table).

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
    Incremental rows (CDC): Silver reads the Bronze rows not
    yet processed (dwh_batch_id > last_silver_batch_id in
    etl.config), only the latest version of each key (a key
    that arrived in several pending batches is loaded once,
    from its most recent batch) and never the deleted rows
    (dwh_operation = 'D'), which are handled separately.

    Deletions (soft delete): a key whose latest pending
    Bronze version is a deletion (dwh_operation = 'D') is not
    removed from Silver: the row is kept and flagged
    dwh_is_deleted = 1 with dwh_deleted_date, so history and
    the facts pointing to it are preserved. If the key comes
    back later (new insert in the source), the row is updated
    and the flag is reset to 0.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    the Silver header load and silver.erp_product. SSIS
    passes the current @batch_id. The load type (full or
    incremental) is read from etl.config.

    The Gold fact load runs after this one.

Warning:
    A FULL load truncates silver.erp_sales_order_detail. If
    a rejected header is fixed later, its details are not
    re-read: run a FULL load. Deleted rows are not detected.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_sales_order_detail]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time        DATETIME2(3),
        @end_time          DATETIME2(3),
        @load_ts           DATETIME2(3) = SYSDATETIME(),
        @load_type         VARCHAR(20),
        @last_silver_load  DATETIME2(3),
        @rows_src          INT = 0,
        @rows_updated      INT = 0,
        @rows_inserted     INT = 0;

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
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        ---------------------------------------------------------
        -- Read configuration (load_type only)
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'sales_order_detail'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SALES_ORDER_DETAIL.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES ORDER DETAIL (FACT SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            -----------------------------------------------------
            -- Build FULL source (#src_full)
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    COALESCE(b.SalesOrderID, -1)        AS sales_order_id,
                    COALESCE(b.SalesOrderDetailID, -1)  AS sales_order_detail_id,
                    b.CarrierTrackingNumber,
                    b.OrderQty,
                    COALESCE(b.ProductID, -1)           AS product_id,
                    b.UnitPrice,
                    b.UnitPriceDiscount,
                    b.LineTotal,
                    b.ModifiedDate AS modified_date
                FROM bronze.erp_sales_order_detail b
                WHERE b.SalesOrderDetailID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_order_detail'
                          AND r.business_key = CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID)
                    )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_order_header'
                          AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.*,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.sales_order_id        AS VARCHAR(20)),
                        CAST(cte.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(cte.CarrierTrackingNumber, ''),
                        CAST(cte.OrderQty              AS VARCHAR(20)),
                        CAST(cte.product_id            AS VARCHAR(20)),
                        CAST(cte.UnitPrice             AS VARCHAR(50)),
                        CAST(cte.UnitPriceDiscount     AS VARCHAR(50)),
                        CAST(cte.LineTotal             AS VARCHAR(50)),
                        CAST(cte.modified_date         AS VARCHAR(30))
                    )
                ) AS row_hash
            INTO #src_full
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
                THROW 51030, 'Bronze sales_order_detail is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_sales_order_detail;

                -- Lookup product SK + INSERT (single statement: the CTE is valid here)
                ;WITH src_lkp AS (
                    SELECT
                        f.*,
                        COALESCE(p.product_sk, -1) AS product_sk
                    FROM #src_full f
                    LEFT JOIN silver.erp_sales_order_header h
                        ON h.sales_order_id = f.sales_order_id
                    LEFT JOIN silver.erp_product p
                        ON p.product_id = f.product_id
                       AND h.order_date >= p.dwh_start_date
                       AND h.order_date <  p.dwh_end_date
                )
                INSERT INTO silver.erp_sales_order_detail
                    (sales_order_id,
                     sales_order_detail_id,
                     carrier_tracking_number,
                     order_qty,
                     product_id,
                     unit_price,
                     unit_price_discount,
                     line_total,
                     modified_date,
                     product_sk,
                     row_hash,
                     dwh_create_date,
                     dwh_load_date,
                     dwh_batch_id)
                SELECT
                    src.sales_order_id,
                    src.sales_order_detail_id,
                    src.CarrierTrackingNumber,
                    src.OrderQty,
                    src.product_id,
                    src.UnitPrice,
                    src.UnitPriceDiscount,
                    src.LineTotal,
                    src.modified_date,
                    src.product_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM src_lkp src;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_full;

            SET @end_time = SYSDATETIME();

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
            -- Audit: FULL load completed
            EXEC etl.usp_audit_end
                @audit_id      = @audit_id,
                @status        = 'succeeded',
                @load_type     = 'full',
                @rows_read     = @rows_src,
                @rows_inserted = @rows_inserted;
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD1)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_sales_order_detail;

            -- Bronze -> Silver register: last batch processed with success
            DECLARE @last_silver_batch_id BIGINT =
                (SELECT ISNULL(last_silver_batch_id, 0)
                 FROM etl.config
                 WHERE object_name = 'sales_order_detail');

            PRINT '>> Silver register (last batch processed): ' + CAST(@last_silver_batch_id AS NVARCHAR(20));

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

            -----------------------------------------------------
            -- Build DELTA (#src_delta)
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    COALESCE(b.SalesOrderID, -1)        AS sales_order_id,
                    COALESCE(b.SalesOrderDetailID, -1)  AS sales_order_detail_id,
                    b.CarrierTrackingNumber,
                    b.OrderQty,
                    COALESCE(b.ProductID, -1)           AS product_id,
                    b.UnitPrice,
                    b.UnitPriceDiscount,
                    b.LineTotal,
                    b.ModifiedDate AS modified_date
                FROM bronze.erp_sales_order_detail b
                WHERE b.dwh_batch_id > @last_silver_batch_id          -- not yet processed by Silver
                  AND b.dwh_operation <> 'D'                         -- deletions handled separately
                  AND NOT EXISTS (                                      -- latest version of the key only
                        SELECT 1
                        FROM bronze.erp_sales_order_detail AS newer
                        WHERE newer.SalesOrderID = b.SalesOrderID AND newer.SalesOrderDetailID = b.SalesOrderDetailID
                          AND newer.dwh_batch_id > b.dwh_batch_id
                          AND newer.dwh_batch_id > @last_silver_batch_id
                    )
                  AND b.SalesOrderDetailID IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_order_detail'
                          AND r.business_key = CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID)
                    )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_order_header'
                          AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                    )
            )
            SELECT
                cte.*,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(cte.sales_order_id        AS VARCHAR(20)),
                        CAST(cte.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(cte.CarrierTrackingNumber, ''),
                        CAST(cte.OrderQty              AS VARCHAR(20)),
                        CAST(cte.product_id            AS VARCHAR(20)),
                        CAST(cte.UnitPrice             AS VARCHAR(50)),
                        CAST(cte.UnitPriceDiscount     AS VARCHAR(50)),
                        CAST(cte.LineTotal             AS VARCHAR(50)),
                        CAST(cte.modified_date         AS VARCHAR(30))
                    )
                ) AS row_hash
            INTO #src_delta
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_delta;

            PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            -- Deleted keys: latest pending version is a deletion ('D')
            DECLARE @rows_deleted_src INT = 0, @rows_deleted INT = 0;

            DROP TABLE IF EXISTS #src_deleted;

            SELECT b.SalesOrderID AS sales_order_id, b.SalesOrderDetailID AS sales_order_detail_id
            INTO #src_deleted
            FROM bronze.erp_sales_order_detail AS b
            WHERE b.dwh_batch_id > @last_silver_batch_id
              AND b.dwh_operation = 'D'
              AND NOT EXISTS (
                    SELECT 1
                    FROM bronze.erp_sales_order_detail AS newer
                    WHERE newer.SalesOrderID = b.SalesOrderID AND newer.SalesOrderDetailID = b.SalesOrderDetailID
                      AND newer.dwh_batch_id > b.dwh_batch_id
                      AND newer.dwh_batch_id > @last_silver_batch_id
                );

            SET @rows_deleted_src = @@ROWCOUNT;
            PRINT '>> Deleted keys in source: ' + CAST(@rows_deleted_src AS NVARCHAR(20));

            IF @rows_src = 0 AND @rows_deleted_src = 0
            BEGIN
                PRINT '>> No delta found. Nothing to do.';
                DROP TABLE #src_delta;
                PRINT '====================================================';
                -- Audit: no delta, nothing to load
                EXEC etl.usp_audit_end
                    @audit_id       = @audit_id,
                    @status         = 'succeeded',
                    @load_type      = @load_type,
                    @rows_read      = @rows_src,
                    @rows_inserted  = @rows_inserted,
                    @rows_updated   = @rows_updated,
                    @watermark_from = @last_silver_load;
                RETURN;
            END

            -----------------------------------------------------
            -- Lookup product SK into a temp table: it is used by
            -- BOTH the UPDATE and the INSERT (a CTE would be valid
            -- for one statement only)
            -----------------------------------------------------
            SELECT
                d.*,
                COALESCE(p.product_sk, -1) AS product_sk
            INTO #src_delta_lkp
            FROM #src_delta d
            LEFT JOIN silver.erp_sales_order_header h
                ON h.sales_order_id = d.sales_order_id
            LEFT JOIN silver.erp_product p
                ON p.product_id = d.product_id
               AND h.order_date >= p.dwh_start_date
               AND h.order_date <  p.dwh_end_date;

            -----------------------------------------------------
            -- UPDATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                -- UPDATE changed rows
                UPDATE tgt
                SET tgt.carrier_tracking_number = src.CarrierTrackingNumber,
                    tgt.order_qty               = src.OrderQty,
                    tgt.product_id              = src.product_id,
                    tgt.unit_price              = src.UnitPrice,
                    tgt.unit_price_discount     = src.UnitPriceDiscount,
                    tgt.line_total              = src.LineTotal,
                    tgt.modified_date           = src.modified_date,
                    tgt.product_sk              = src.product_sk,
                    tgt.row_hash                = src.row_hash,
                    tgt.dwh_load_date           = @load_ts,
                    tgt.dwh_batch_id            = @batch_id,
                    tgt.dwh_is_deleted          = 0,
                    tgt.dwh_deleted_date        = NULL
                FROM silver.erp_sales_order_detail tgt
                JOIN #src_delta_lkp src
                    ON tgt.sales_order_id        = src.sales_order_id
                   AND tgt.sales_order_detail_id = src.sales_order_detail_id
                WHERE (tgt.row_hash <> src.row_hash OR tgt.dwh_is_deleted = 1);   -- changed, or back after a deletion

                SET @rows_updated = @@ROWCOUNT;

                -- INSERT new business keys
                INSERT INTO silver.erp_sales_order_detail
                    (sales_order_id,
                     sales_order_detail_id,
                     carrier_tracking_number,
                     order_qty,
                     product_id,
                     unit_price,
                     unit_price_discount,
                     line_total,
                     modified_date,
                     product_sk,
                     row_hash,
                     dwh_create_date,
                     dwh_load_date,
                     dwh_batch_id)
                SELECT
                    src.sales_order_id,
                    src.sales_order_detail_id,
                    src.CarrierTrackingNumber,
                    src.OrderQty,
                    src.product_id,
                    src.UnitPrice,
                    src.UnitPriceDiscount,
                    src.LineTotal,
                    src.modified_date,
                    src.product_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta_lkp src
                LEFT JOIN silver.erp_sales_order_detail tgt
                    ON tgt.sales_order_id        = src.sales_order_id
                   AND tgt.sales_order_detail_id = src.sales_order_detail_id
                WHERE tgt.sales_order_detail_id IS NULL;

                SET @rows_inserted = @@ROWCOUNT;

                -- Deletions (soft delete): keep the row, flag it
                UPDATE tgt
                SET tgt.dwh_is_deleted   = 1,
                    tgt.dwh_deleted_date = @load_ts,
                    tgt.dwh_load_date    = @load_ts,
                    tgt.dwh_batch_id     = @batch_id
                FROM silver.erp_sales_order_detail AS tgt
                INNER JOIN #src_deleted AS d
                    ON tgt.sales_order_id = d.sales_order_id
                   AND tgt.sales_order_detail_id = d.sales_order_detail_id
                WHERE tgt.dwh_is_deleted = 0;

                SET @rows_deleted = @@ROWCOUNT;
                PRINT '>> Rows deleted (flagged): ' + CAST(@rows_deleted AS NVARCHAR(20));

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;
            DROP TABLE #src_delta_lkp;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
        END

        ---------------------------------------------------------
        -- Unsupported load type
        ---------------------------------------------------------
        ELSE
        BEGIN
            THROW 51020, 'Unsupported load type for SALES_ORDER_DETAIL.', 1;
        END

        -- Audit: INCREMENTAL load completed
        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @load_type,
            @rows_read      = @rows_src,
            @rows_inserted  = @rows_inserted,
            @rows_updated   = @rows_updated,
            @watermark_from = @last_silver_load;

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')       IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta')      IS NOT NULL DROP TABLE #src_delta;
        IF OBJECT_ID('tempdb..#src_delta_lkp')  IS NOT NULL DROP TABLE #src_delta_lkp;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_order_detail';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id  = @audit_id,
                @status    = 'failed',
                @load_type = @load_type;

        THROW;
    END CATCH
END;
GO
