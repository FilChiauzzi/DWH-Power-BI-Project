/*
=========================================================
Gold Fact Load – Sales Order
=========================================================
Purpose:
    This stored procedure loads the gold.fact_sales_order
    table using a robust incremental strategy based on the
    ERP ModifiedDate field and row_hash change detection.
    
    The procedure supports two load types:
        - FULL: rebuilds the entire fact table by truncating
          and reloading all rows from the Silver layer.
        - INCREMENTAL: loads only new or updated rows by
          comparing Silver.ModifiedDate with the maximum
          ModifiedDate already present in Gold, and applies
          row_hash-based change detection for updates.

Incremental Logic:
    The delta is determined without using any watermark.
    Instead, the procedure identifies new or changed rows
    by comparing:
        - Silver.modified_date > MAX(Gold.modified_date)
        - Silver.row_hash <> Gold.row_hash

    Two operations are performed:
        - UPDATE: existing business keys with changed data
          (row_hash mismatch) are updated.
        - INSERT: new business keys not present in Gold are
          inserted.

Technical Fields:
    The fact table includes row_hash for change detection,
    dwh_create_date and dwh_load_date for ETL auditing, and
    dwh_batch_id for batch traceability.

Warning:
    Running a FULL load will truncate the entire
    gold.fact_sales_order table before repopulating it.
*/

CREATE OR ALTER PROCEDURE [etl].[usp_load_gold_fact_sales_order]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @load_type        VARCHAR(20),
        @rows_src         INT = 0,
        @rows_updated     INT = 0,
        @rows_inserted    INT = 0,
        @load_ts          DATETIME2(3) = SYSDATETIME(),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @max_gold_date    DATETIME;

    BEGIN TRY
        ---------------------------------------------------------
        -- Read load_type from etl.config (NO watermark)
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

            BEGIN TRANSACTION;

                TRUNCATE TABLE gold.fact_sales_order;

                ---------------------------------------------------------
                -- FULL source (#src_full)
                ---------------------------------------------------------
                SELECT
                    d.sales_order_id,
                    d.sales_order_detail_id,

                    CAST(h.order_date AS DATE) AS order_date,
                    CAST(h.due_date AS DATE)  AS due_date,
                    CAST(h.ship_date AS DATE) AS ship_date,
                    CAST(d.modified_date AS DATE) AS modified_date,

                    h.customer_sk,
                    h.person_sk,
                    h.territory_sk,
                    d.product_sk,

                    d.order_qty,
                    d.unit_price,
                    d.unit_price_discount,
                    d.line_total,
                    h.sub_total,
                    h.tax_amount,
                    h.freight,
                    h.total_due,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(d.sales_order_id AS VARCHAR(20)),
                            CAST(d.sales_order_detail_id AS VARCHAR(20)),
                            CAST(d.order_qty AS VARCHAR(20)),
                            CAST(d.unit_price AS VARCHAR(50)),
                            CAST(d.unit_price_discount AS VARCHAR(50)),
                            CAST(d.line_total AS VARCHAR(50)),
                            CAST(h.total_due AS VARCHAR(50)),
                            CAST(d.modified_date AS VARCHAR(30))
                        )
                    ) AS row_hash
                INTO #src_full
                FROM silver.erp_sales_order_detail d
                INNER JOIN silver.erp_sales_order_header h
                    ON d.sales_order_id = h.sales_order_id;

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> FULL source rows: ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL
                ---------------------------------------------------------
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
                    sub_total,
                    tax_amount,
                    freight,
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
                    customer_sk,
                    person_sk,
                    territory_sk,
                    product_sk,
                    order_qty,
                    unit_price,
                    unit_price_discount,
                    line_total,
                    sub_total,
                    tax_amount,
                    freight,
                    total_due,
                    row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_full;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_full;

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
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
                -- Get MAX modified_date from Gold
                ---------------------------------------------------------
                SELECT @max_gold_date = MAX(modified_date)
                FROM gold.fact_sales_order;

                PRINT '>> Gold MAX modified_date: ' + CONVERT(NVARCHAR(30), @max_gold_date, 121);

                ---------------------------------------------------------
                -- Build DELTA from Silver
                ---------------------------------------------------------
                SELECT
                    d.sales_order_id,
                    d.sales_order_detail_id,

                    CAST(h.order_date AS DATE) AS order_date,
                    CAST(h.due_date AS DATE)  AS due_date,
                    CAST(h.ship_date AS DATE) AS ship_date,
                    CAST(d.modified_date AS DATE) AS modified_date,

                    h.customer_sk,
                    h.person_sk,
                    h.territory_sk,
                    d.product_sk,

                    d.order_qty,
                    d.unit_price,
                    d.unit_price_discount,
                    d.line_total,
                    h.sub_total,
                    h.tax_amount,
                    h.freight,
                    h.total_due,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(d.sales_order_id AS VARCHAR(20)),
                            CAST(d.sales_order_detail_id AS VARCHAR(20)),
                            CAST(d.order_qty AS VARCHAR(20)),
                            CAST(d.unit_price AS VARCHAR(50)),
                            CAST(d.unit_price_discount AS VARCHAR(50)),
                            CAST(d.line_total AS VARCHAR(50)),
                            CAST(h.total_due AS VARCHAR(50)),
                            CAST(d.modified_date AS VARCHAR(30))
                        )
                    ) AS row_hash
                INTO #src_delta
                FROM silver.erp_sales_order_detail d
                INNER JOIN silver.erp_sales_order_header h
                    ON d.sales_order_id = h.sales_order_id
                WHERE d.modified_date > @max_gold_date;

                SELECT @rows_src = COUNT(*) FROM #src_delta;

                PRINT '>> Delta rows from Silver: ' + CAST(@rows_src AS NVARCHAR(20));

                IF @rows_src = 0
                BEGIN
                    PRINT '>> No delta found. Incremental load skipped.';
                    DROP TABLE #src_delta;
                    ROLLBACK TRANSACTION;
                    RETURN;
                END

                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
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
                    tgt.sub_total            = src.sub_total,
                    tgt.tax_amount           = src.tax_amount,
                    tgt.freight              = src.freight,
                    tgt.total_due            = src.total_due,
                    tgt.row_hash             = src.row_hash,
                    tgt.dwh_load_date        = @load_ts,
                    tgt.dwh_batch_id         = @batch_id
                FROM gold.fact_sales_order tgt
                JOIN #src_delta src
                    ON tgt.sales_order_id = src.sales_order_id
                   AND tgt.sales_order_detail_id = src.sales_order_detail_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
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
                    sub_total,
                    tax_amount,
                    freight,
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
                    src.customer_sk,
                    src.person_sk,
                    src.territory_sk,
                    src.product_sk,
                    src.order_qty,
                    src.unit_price,
                    src.unit_price_discount,
                    src.line_total,
                    src.sub_total,
                    src.tax_amount,
                    src.freight,
                    src.total_due,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM gold.fact_sales_order tgt
                    WHERE tgt.sales_order_id = src.sales_order_id
                      AND tgt.sales_order_detail_id = src.sales_order_detail_id
                );

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '====================================================';
        END

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
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
