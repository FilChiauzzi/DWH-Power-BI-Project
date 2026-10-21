/*
=========================================================
Gold Fact Load – Sales Order
=========================================================
Purpose:
    Loads gold.fact_sales_order at order-line grain
    (sales_order_id + sales_order_detail_id) from
    silver.erp_sales_order_detail and
    silver.erp_sales_order_header. Measures come from the
    detail; dates and dimension keys from the header;
    missing keys become -1.

    The header total_due belongs to the whole order, so it
    is allocated across its lines: each line gets its
    line_total plus an equal share of the remaining amount
    (tax and freight), rounded to 4 decimals. The last line
    takes the remainder, so the lines always add up to the
    header total_due.

    Release 1: the load type is forced to FULL inside the
    procedure. FULL builds the source, then truncates and
    reloads the fact in one transaction (it stops if Silver
    is empty). The INCREMENTAL branch is implemented but not
    active yet.

    Every execution is logged in etl.load_audit (layer gold,
    step load): status, duration, row counts and, on
    failure, the error.
SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline as the
    last step of the batch, after the Silver header and
    detail loads. SSIS passes the current @batch_id.

Warning:
    Every run truncates gold.fact_sales_order and reloads
    it. Orders without lines in the Silver detail do not
    appear in the fact.
*/

USE [DWH_AdventureWorks2025]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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
            @layer          = 'gold',
            @object_name    = 'fact_sales_order',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        ---------------------------------------------------------
        -- Read load_type from etl.config (no watermark needed)
        ---------------------------------------------------------
        SET @load_type = 'full'

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
            ;WITH base AS (
                SELECT
                    d.sales_order_id,
                    d.sales_order_detail_id,

                    CAST(h.order_date    AS DATE) AS order_date,
                    CAST(h.due_date      AS DATE) AS due_date,
                    CAST(h.ship_date     AS DATE) AS ship_date,
                    CAST(d.modified_date AS DATE) AS modified_date,

                    COALESCE(h.[status],           -1) AS status_id,
                    COALESCE(h.online_order_flag,  -1) AS online_order_id,
                    COALESCE(h.customer_sk,        -1) AS customer_sk,
                    COALESCE(h.person_sk,          -1) AS person_sk,
                    COALESCE(h.territory_sk,       -1) AS territory_sk,
                    COALESCE(d.product_sk,         -1) AS product_sk,

                    d.order_qty,
                    d.unit_price,
                    d.unit_price_discount,
                    d.line_total,
                    CAST(d.line_total AS DECIMAL(19,4)) AS line_total_4,
                    ROW_NUMBER() OVER (PARTITION BY d.sales_order_id
                                       ORDER BY d.sales_order_detail_id) AS rn,
                    COUNT(*)     OVER (PARTITION BY d.sales_order_id)     AS n,
                    CAST(h.total_due AS DECIMAL(19,4))
                      - SUM(CAST(d.line_total AS DECIMAL(19,4)))
                          OVER (PARTITION BY d.sales_order_id)            AS extra
                FROM silver.erp_sales_order_detail d
                INNER JOIN silver.erp_sales_order_header h
                    ON d.sales_order_id = h.sales_order_id
            ),
            calc AS (
                SELECT
                    b.*,
                    CAST(
                        b.line_total_4
                        + CASE
                              WHEN b.rn = b.n
                                  THEN b.extra - (b.n - 1) * ROUND(b.extra / b.n, 4)
                              ELSE ROUND(b.extra / b.n, 4)
                          END
                    AS DECIMAL(19,4)) AS total_due
                FROM base b
            )
            SELECT
                c.sales_order_id,
                c.sales_order_detail_id,
                c.order_date,
                c.due_date,
                c.ship_date,
                c.modified_date,
                c.status_id,
                c.online_order_id,
                c.customer_sk,
                c.person_sk,
                c.territory_sk,
                c.product_sk,
                c.order_qty,
                c.unit_price,
                c.unit_price_discount,
                c.line_total,
                c.total_due,

                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(c.sales_order_id        AS VARCHAR(20)),
                        CAST(c.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(CONVERT(VARCHAR(10), c.order_date,    23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.due_date,      23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.ship_date,     23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.modified_date, 23), ''),
                        CAST(c.status_id        AS NVARCHAR(30)),
                        CAST(c.online_order_id  AS NVARCHAR(30)),
                        CAST(c.customer_sk      AS VARCHAR(20)),
                        CAST(c.person_sk        AS VARCHAR(20)),
                        CAST(c.territory_sk     AS VARCHAR(20)),
                        CAST(c.product_sk       AS VARCHAR(20)),
                        CAST(c.order_qty             AS VARCHAR(20)),
                        CAST(c.unit_price            AS VARCHAR(50)),
                        CAST(c.unit_price_discount   AS VARCHAR(50)),
                        CAST(c.line_total            AS VARCHAR(50)),
                        CAST(c.total_due             AS VARCHAR(50))
                    )
                ) AS row_hash
            INTO #src_full
            FROM calc c;

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
                    status_id,
                    online_order_id,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    total_due,
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
                    status_id,
                    online_order_id,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    total_due,
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
            -- load, in the detail OR in the header.
            -- WARNING: the allocation is computed on ALL the lines
            -- of the order; the delta filter is applied only at the end
            -- (flag is_delta), altrimenti n e extra sarebbero sbagliati.
            -----------------------------------------------------
            ;WITH base AS (
                SELECT
                    d.sales_order_id,
                    d.sales_order_detail_id,

                    CAST(h.order_date    AS DATE) AS order_date,
                    CAST(h.due_date      AS DATE) AS due_date,
                    CAST(h.ship_date     AS DATE) AS ship_date,
                    CAST(d.modified_date AS DATE) AS modified_date,

                    COALESCE(h.[status],           -1) AS status_id,
                    COALESCE(h.online_order_flag,  -1) AS online_order_id,
                    COALESCE(h.customer_sk,        -1) AS customer_sk,
                    COALESCE(h.person_sk,          -1) AS person_sk,
                    COALESCE(h.territory_sk,       -1) AS territory_sk,
                    COALESCE(d.product_sk,         -1) AS product_sk,

                    d.order_qty,
                    d.unit_price,
                    d.unit_price_discount,
                    d.line_total,

                    CAST(d.line_total AS DECIMAL(19,4)) AS line_total_4,

                    ROW_NUMBER() OVER (PARTITION BY d.sales_order_id
                                       ORDER BY d.sales_order_detail_id) AS rn,
                    COUNT(*)     OVER (PARTITION BY d.sales_order_id)     AS n,

                    CAST(h.total_due AS DECIMAL(19,4))
                      - SUM(CAST(d.line_total AS DECIMAL(19,4)))
                          OVER (PARTITION BY d.sales_order_id)            AS extra,

                    CASE WHEN d.dwh_load_date > @last_gold_load
                           OR h.dwh_load_date > @last_gold_load
                         THEN 1 ELSE 0 END                                AS is_delta
                FROM silver.erp_sales_order_detail d
                INNER JOIN silver.erp_sales_order_header h
                    ON d.sales_order_id = h.sales_order_id
            ),
            calc AS (
                SELECT
                    b.*,
                    CAST(
                        b.line_total_4
                        + CASE
                              WHEN b.rn = b.n
                                  THEN b.extra - (b.n - 1) * ROUND(b.extra / b.n, 4)
                              ELSE ROUND(b.extra / b.n, 4)
                          END
                    AS DECIMAL(19,4)) AS total_due
                FROM base b
            )
            SELECT
                c.sales_order_id,
                c.sales_order_detail_id,
                c.order_date,
                c.due_date,
                c.ship_date,
                c.modified_date,
                c.status_id,
                c.online_order_id,
                c.customer_sk,
                c.person_sk,
                c.territory_sk,
                c.product_sk,
                c.order_qty,
                c.unit_price,
                c.unit_price_discount,
                c.line_total,
                c.total_due,

                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        CAST(c.sales_order_id        AS VARCHAR(20)),
                        CAST(c.sales_order_detail_id AS VARCHAR(20)),
                        ISNULL(CONVERT(VARCHAR(10), c.order_date,    23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.due_date,      23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.ship_date,     23), ''),
                        ISNULL(CONVERT(VARCHAR(10), c.modified_date, 23), ''),
                        CAST(c.status_id        AS NVARCHAR(30)),
                        CAST(c.online_order_id  AS NVARCHAR(30)),
                        CAST(c.customer_sk      AS VARCHAR(20)),
                        CAST(c.person_sk        AS VARCHAR(20)),
                        CAST(c.territory_sk     AS VARCHAR(20)),
                        CAST(c.product_sk       AS VARCHAR(20)),
                        CAST(c.order_qty             AS VARCHAR(20)),
                        CAST(c.unit_price            AS VARCHAR(50)),
                        CAST(c.unit_price_discount   AS VARCHAR(50)),
                        CAST(c.line_total            AS VARCHAR(50)),
                        CAST(c.total_due             AS VARCHAR(50))
                    )
                ) AS row_hash
            INTO #src_delta
            FROM calc c
            WHERE c.is_delta = 1;

            SELECT @rows_src = COUNT(*) FROM #src_delta;

            PRINT '>> Delta rows from Silver: ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
            BEGIN
                PRINT '>> No delta found. Incremental load skipped.';
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
                    @watermark_from = @last_gold_load;
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
                    tgt.status_id            = src.status_id,          -- era src.[status]
                    tgt.online_order_id      = src.online_order_id,    -- era src.online_order_flag
                    tgt.customer_sk          = src.customer_sk,
                    tgt.person_sk            = src.person_sk,
                    tgt.territory_sk         = src.territory_sk,
                    tgt.product_sk           = src.product_sk,
                    tgt.order_qty            = src.order_qty,
                    tgt.unit_price           = src.unit_price,
                    tgt.unit_price_discount  = src.unit_price_discount,
                    tgt.line_total           = src.line_total,
                    tgt.total_due            = src.total_due,
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
                    status_id,
                    online_order_id,
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    total_due,
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
                    src.status_id,            -- era src.[status]
                    src.online_order_id,      -- era src.online_order_flag
                    src.customer_sk,
                    src.person_sk,
                    src.territory_sk,
                    src.product_sk,
                    src.order_qty,
                    src.unit_price,
                    src.unit_price_discount,
                    src.line_total,
                    src.total_due,
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

        -- Audit: INCREMENTAL load completed
        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @load_type,
            @rows_read      = @rows_src,
            @rows_inserted  = @rows_inserted,
            @rows_updated   = @rows_updated,
            @watermark_from = @last_gold_load;

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
