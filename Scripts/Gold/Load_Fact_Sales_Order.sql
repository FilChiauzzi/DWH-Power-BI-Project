/*
=========================================================
Gold Fact Load – Sales Order (grain: one row per order line)
=========================================================
Purpose:
    Loads gold.fact_sales_order from the Silver layer
    (silver.erp_sales_order_detail + silver.erp_sales_order_header).

    GRAIN: sales_order_id + sales_order_detail_id.
    Measures come from the detail (order_qty, unit_price,
    unit_price_discount, line_total). From the header we take
    only dates and dimension keys. Header amounts (sub_total,
    tax_amount, freight, total_due) are NOT loaded: they belong
    to the whole order and would be repeated on every line,
    so SUM() would count them several times.

Load types (etl.config, object_name = 'fact_sales_order'):
    - full:        builds the source, then TRUNCATE + INSERT
                   in one transaction.
    - incremental: picks rows that SILVER wrote after the last
                   Gold load, using silver.dwh_load_date
                   (detail OR header). The ERP ModifiedDate is
                   NOT used for the delta: a header change
                   (ShipDate, Status) does not touch the detail
                   ModifiedDate.
                   Then UPDATE (row_hash changed) + INSERT (new keys).

Rules:
    - row_hash must contain EVERY column written by the UPDATE,
      and must be identical in FULL and INCREMENTAL.
    - Both Silver tables must have dwh_load_date, set on every
      INSERT and UPDATE.
    - Silver must run BEFORE Gold in the same batch.
    - Deleted rows are not detected: schedule a periodic FULL.

Before the first run (header amounts removed from the fact):
    ALTER TABLE gold.fact_sales_order
        DROP COLUMN sub_total, tax_amount, freight, total_due;
    Then run one FULL load (the hash definition has changed).

Warning:
    FULL truncates gold.fact_sales_order before reloading.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_gold_fact_sales_order]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @load_type       VARCHAR(20),
        @rows_src        INT          = 0,
        @rows_updated    INT          = 0,
        @rows_inserted   INT          = 0,
        @load_ts         DATETIME2(3) = SYSDATETIME(),
        @start_time      DATETIME2(3),
        @end_time        DATETIME2(3),
        @last_gold_load  DATETIME2(3);

    BEGIN TRY
        ---------------------------------------------------------
        -- Read load_type from etl.config (no watermark needed)
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'fact_sales_order'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for GOLD FACT SALES ORDER.', 1;

        PRINT '====================================================';
        PRINT 'Gold Load – FACT SALES ORDER | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            -----------------------------------------------------
            -- Build the source first (nothing is truncated yet)
            -----------------------------------------------------
            SELECT
                d.sales_order_id,
                d.sales_order_detail_id,

                CAST(h.order_date    AS DATE) AS order_date,
                CAST(h.due_date      AS DATE) AS due_date,
                CAST(h.ship_date     AS DATE) AS ship_date,
                CAST(d.modified_date AS DATE) AS modified_date,

                COALESCE(h.customer_sk,  -1) AS customer_sk,
                COALESCE(h.person_sk,    -1) AS person_sk,
                COALESCE(h.territory_sk, -1) AS territory_sk,
                COALESCE(d.product_sk,   -1) AS product_sk,

                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total,

                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(d.sales_order_id        AS VARCHAR(20)),
                        CAST(d.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(CONVERT(VARCHAR(10), h.order_date,    23), ''),
                        ISNULL(CONVERT(VARCHAR(10), h.due_date,      23), ''),
                        ISNULL(CONVERT(VARCHAR(10), h.ship_date,     23), ''),
                        ISNULL(CONVERT(VARCHAR(10), d.modified_date, 23), ''),
                        CAST(COALESCE(h.customer_sk,  -1) AS VARCHAR(20)),
                        CAST(COALESCE(h.person_sk,    -1) AS VARCHAR(20)),
                        CAST(COALESCE(h.territory_sk, -1) AS VARCHAR(20)),
                        CAST(COALESCE(d.product_sk,   -1) AS VARCHAR(20)),
                        CAST(d.order_qty             AS VARCHAR(20)),
                        CAST(d.unit_price            AS VARCHAR(50)),
                        CAST(d.unit_price_discount   AS VARCHAR(50)),
                        CAST(d.line_total            AS VARCHAR(50))
                    )
                ) AS row_hash
            INTO #src_full
            FROM silver.erp_sales_order_detail d
            INNER JOIN silver.erp_sales_order_header h
                ON d.sales_order_id = h.sales_order_id;

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> FULL source rows: ' + CAST(@rows_src AS NVARCHAR(20));

            -- Safety: never empty the fact because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Silver source is empty: FULL load stopped to avoid emptying the fact table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE gold.fact_sales_order;

                INSERT INTO gold.fact_sales_order (
                    sales_order_id,
                    sales_order_detail_id,
                    order_date,
                    due_date,
                    ship_date,
                    modified_date,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    row_hash,
                    dwh_create_date,
                    dwh_load_date,
                    dwh_batch_id
                )
                SELECT
                    sales_order_id,
                    sales_order_detail_id,
                    order_date,
                    due_date,
                    ship_date,
                    modified_date,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_full;

            SET @end_time = SYSDATETIME();

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            -----------------------------------------------------
            -- Last time Gold wrote something (empty Gold -> 1900)
            -----------------------------------------------------
            SELECT @last_gold_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM gold.fact_sales_order;

            PRINT '>> Last Gold load: ' + CONVERT(NVARCHAR(30), @last_gold_load, 121);

            -----------------------------------------------------
            -- Delta: rows that Silver wrote after the last Gold
            -- load, in the detail OR in the header
            -----------------------------------------------------
            SELECT
                d.sales_order_id,
                d.sales_order_detail_id,

                CAST(h.order_date    AS DATE) AS order_date,
                CAST(h.due_date      AS DATE) AS due_date,
                CAST(h.ship_date     AS DATE) AS ship_date,
                CAST(d.modified_date AS DATE) AS modified_date,

                COALESCE(h.customer_sk,  -1) AS customer_sk,
                COALESCE(h.person_sk,    -1) AS person_sk,
                COALESCE(h.territory_sk, -1) AS territory_sk,
                COALESCE(d.product_sk,   -1) AS product_sk,

                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total,

                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(d.sales_order_id        AS VARCHAR(20)),
                        CAST(d.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(CONVERT(VARCHAR(10), h.order_date,    23), ''),
                        ISNULL(CONVERT(VARCHAR(10), h.due_date,      23), ''),
                        ISNULL(CONVERT(VARCHAR(10), h.ship_date,     23), ''),
                        ISNULL(CONVERT(VARCHAR(10), d.modified_date, 23), ''),
                        CAST(COALESCE(h.customer_sk,  -1) AS VARCHAR(20)),
                        CAST(COALESCE(h.person_sk,    -1) AS VARCHAR(20)),
                        CAST(COALESCE(h.territory_sk, -1) AS VARCHAR(20)),
                        CAST(COALESCE(d.product_sk,   -1) AS VARCHAR(20)),
                        CAST(d.order_qty             AS VARCHAR(20)),
                        CAST(d.unit_price            AS VARCHAR(50)),
                        CAST(d.unit_price_discount   AS VARCHAR(50)),
                        CAST(d.line_total            AS VARCHAR(50))
                    )
                ) AS row_hash
            INTO #src_delta
            FROM silver.erp_sales_order_detail d
            INNER JOIN silver.erp_sales_order_header h
                ON d.sales_order_id = h.sales_order_id
            WHERE d.dwh_load_date > @last_gold_load
               OR h.dwh_load_date > @last_gold_load;

            SELECT @rows_src = COUNT(*) FROM #src_delta;

            PRINT '>> Delta rows from Silver: ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
            BEGIN
                PRINT '>> No delta found. Incremental load skipped.';
                DROP TABLE #src_delta;
                PRINT '====================================================';
                RETURN;
            END

            -----------------------------------------------------
            -- UPDATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                -- UPDATE changed rows (row_hash differs)
                UPDATE tgt
                SET tgt.order_date           = src.order_date,
                    tgt.due_date             = src.due_date,
                    tgt.ship_date            = src.ship_date,
                    tgt.modified_date        = src.modified_date,
                    tgt.customer_sk          = src.customer_sk,
                    tgt.person_sk            = src.person_sk,
                    tgt.territory_sk         = src.territory_sk,
                    tgt.product_sk           = src.product_sk,
                    tgt.order_qty            = src.order_qty,
                    tgt.unit_price           = src.unit_price,
                    tgt.unit_price_discount  = src.unit_price_discount,
                    tgt.line_total           = src.line_total,
                    tgt.row_hash             = src.row_hash,
                    tgt.dwh_load_date        = @load_ts,
                    tgt.dwh_batch_id         = @batch_id
                FROM gold.fact_sales_order tgt
                JOIN #src_delta src
                    ON tgt.sales_order_id        = src.sales_order_id
                   AND tgt.sales_order_detail_id = src.sales_order_detail_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                -- INSERT new business keys
                INSERT INTO gold.fact_sales_order (
                    sales_order_id,
                    sales_order_detail_id,
                    order_date,
                    due_date,
                    ship_date,
                    modified_date,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    row_hash,
                    dwh_create_date,
                    dwh_load_date,
                    dwh_batch_id
                )
                SELECT
                    src.sales_order_id,
                    src.sales_order_detail_id,
                    src.order_date,
                    src.due_date,
                    src.ship_date,
                    src.modified_date,
                    src.customer_sk,
                    src.person_sk,
                    src.territory_sk,
                    src.product_sk,
                    src.order_qty,
                    src.unit_price,
                    src.unit_price_discount,
                    src.line_total,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM gold.fact_sales_order tgt
                    WHERE tgt.sales_order_id        = src.sales_order_id
                      AND tgt.sales_order_detail_id = src.sales_order_detail_id
                );

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

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
            THROW 51020, 'Unsupported load type for GOLD FACT SALES ORDER.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Gold load for: fact_sales_order';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
