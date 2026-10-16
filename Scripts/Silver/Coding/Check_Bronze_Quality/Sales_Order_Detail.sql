/*
=========================================================
Bronze Data Quality – Sales Order Detail
=========================================================
Purpose:
    This stored procedure validates the Bronze-layer table
    for the Sales Order Detail domain. It applies blocking
    and non-blocking quality rules, logs issues into
    etl.data_quality_log, and inserts rejected rows into
    etl.rejected. The procedure enforces business-key
    integrity and numeric consistency before the Silver
    pipeline is allowed to run.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Bronze Quality pipeline. If the rejection rate exceeds
    the configured threshold, the pipeline stops on purpose
    to prevent corrupted data from propagating to Silver.

Notes:
    - SalesOrderID and SalesOrderDetailID are mandatory
      business keys: NULL values are blocking errors.
    - Duplicate (SalesOrderID, SalesOrderDetailID) pairs
      are blocking errors because the FACT grain must be
      unique at the order-line level.
    - OrderQty must be strictly positive.
    - UnitPrice and UnitPriceDiscount must be non-negative.
    - LineTotal must match the formula:
          (UnitPrice - UnitPriceDiscount) * OrderQty
    - All rejected rows are stored as JSON in etl.rejected
      to support auditability and troubleshooting.
    - The final rejection rate is compared against
      @max_reject_rate; exceeding it raises an exception
      and stops the ETL pipeline intentionally.

SCD:
    - No SCD logic is applied at Bronze level.
    - This procedure only validates raw transactional data
      before Silver transformations and dimension lookups.
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
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @load_ts          DATETIME2(3) = SYSDATETIME(),
        @load_type        VARCHAR(20),
        @last_wm          DATETIME,
        @new_wm           DATETIME,
        @rows_src         INT = 0,
        @rows_updated     INT = 0,
        @rows_inserted    INT = 0;

    BEGIN TRY
        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @last_wm   = last_watermark_value
        FROM etl.config
        WHERE object_name = 'sales_order_detail'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SALES_ORDER_DETAIL.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES ORDER DETAIL | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_sales_order_detail;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    COALESCE(b.SalesOrderID, -1)        AS sales_order_id,
                    COALESCE(b.SalesOrderDetailID, -1)  AS sales_order_detail_id,
                    b.CarrierTrackingNumber,
                    b.OrderQty,
                    COALESCE(b.ProductID, -1)           AS product_id,
                    b.UnitPrice,
                    b.UnitPriceDiscount,
                    b.LineTotal,
                    b.ModifiedDate                      AS modified_date,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(b.SalesOrderID AS VARCHAR(20)),
                            CAST(b.SalesOrderDetailID AS VARCHAR(20)),
                            CAST(b.OrderQty AS VARCHAR(20)),
                            CAST(b.ProductID AS VARCHAR(20)),
                            CAST(b.UnitPrice AS VARCHAR(50)),
                            CAST(b.UnitPriceDiscount AS VARCHAR(50)),
                            CAST(b.LineTotal AS VARCHAR(50)),
                            CAST(b.ModifiedDate AS VARCHAR(30))
                        )
                    ) AS row_hash
                INTO #src_full
                FROM bronze.erp_sales_order_detail b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id = @batch_id
                      AND r.layer = 'bronze'
                      AND r.object_name = 'sales_order_detail'
                      AND r.business_key = CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID)
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id = @batch_id
                      AND r.layer = 'bronze'
                      AND r.object_name = 'sales_order_header'
                      AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Lookup SKs con CTE (solo Product SCD2)
                ---------------------------------------------------------
                ;WITH src_lkp AS (
                    SELECT
                        f.*,
                        COALESCE(p.product_sk, -1) AS product_sk
                    FROM #src_full f
                    LEFT JOIN silver.erp_product p
                        ON f.product_id = p.product_id
                       AND (
                            p.is_active = 1
                            OR (f.modified_date >= p.dwh_start_date AND f.modified_date < p.dwh_end_date)
                       )
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
            PRINT '====================================================';
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA (#src_delta)
                ---------------------------------------------------------
                SELECT
                    COALESCE(b.SalesOrderID, -1)        AS sales_order_id,
                    COALESCE(b.SalesOrderDetailID, -1)  AS sales_order_detail_id,
                    b.CarrierTrackingNumber,
                    b.OrderQty,
                    COALESCE(b.ProductID, -1)           AS product_id,
                    b.UnitPrice,
                    b.UnitPriceDiscount,
                    b.LineTotal,
                    b.ModifiedDate                      AS modified_date,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(b.SalesOrderID AS VARCHAR(20)),
                            CAST(b.SalesOrderDetailID AS VARCHAR(20)),
                            CAST(b.OrderQty AS VARCHAR(20)),
                            CAST(b.ProductID AS VARCHAR(20)),
                            CAST(b.UnitPrice AS VARCHAR(50)),
                            CAST(b.UnitPriceDiscount AS VARCHAR(50)),
                            CAST(b.LineTotal AS VARCHAR(50)),
                            CAST(b.ModifiedDate AS VARCHAR(30))
                        )
                    ) AS row_hash
                INTO #src_delta
                FROM bronze.erp_sales_order_detail b
                WHERE b.ModifiedDate > @last_wm
                AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id = @batch_id
                      AND r.layer = 'bronze'
                      AND r.object_name = 'sales_order_detail'
                      AND r.business_key = CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID)
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id = @batch_id
                      AND r.layer = 'bronze'
                      AND r.object_name = 'sales_order_header'
                      AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_delta;

                IF @rows_src = 0
                BEGIN
                    PRINT '>> No delta found.';
                    DROP TABLE #src_delta;
                    ROLLBACK TRANSACTION;
                    RETURN;
                END

                ---------------------------------------------------------
                -- Lookup SKs con CTE (solo Product SCD2)
                ---------------------------------------------------------
                ;WITH src_lkp AS (
                    SELECT
                        d.*,
                        COALESCE(p.product_sk, -1) AS product_sk
                    FROM #src_delta d
                    LEFT JOIN silver.erp_product p
                        ON d.product_id = p.product_id
                       AND (
                            p.is_active = 1
                            OR (d.modified_date >= p.dwh_start_date AND d.modified_date < p.dwh_end_date)
                       )
                )

                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.carrier_tracking_number = src.CarrierTrackingNumber,
                    tgt.order_qty              = src.OrderQty,
                    tgt.product_id             = src.product_id,
                    tgt.unit_price             = src.UnitPrice,
                    tgt.unit_price_discount    = src.UnitPriceDiscount,
                    tgt.line_total             = src.LineTotal,
                    tgt.modified_date          = src.ModifiedDate,
                    tgt.product_sk             = src.product_sk,
                    tgt.row_hash               = src.row_hash,
                    tgt.dwh_load_date          = @load_ts,
                    tgt.dwh_batch_id           = @batch_id
                FROM silver.erp_sales_order_detail tgt
                JOIN src_lkp src
                    ON tgt.sales_order_id = src.sales_order_id
                   AND tgt.sales_order_detail_id = src.sales_order_detail_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
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
                    src.ModifiedDate,
                    src.product_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM src_lkp src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_sales_order_detail tgt
                    WHERE tgt.sales_order_id = src.sales_order_id
                      AND tgt.sales_order_detail_id = src.sales_order_detail_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(modified_date) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'sales_order_detail'
                  AND is_active = 1;

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '====================================================';
        END

        ELSE
        BEGIN
            THROW 51020, 'Unsupported load type for SALES_ORDER_DETAIL.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_order_detail';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT '====================================================';

        THROW;
    END CATCH
END;

