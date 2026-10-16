/*
=========================================================
Silver Load – Sales Order Header (FACT)
=========================================================
Purpose:
    This stored procedure loads the Silver-layer FACT table
    for the Sales Order Header domain. It applies standard
    Silver-layer transformation rules, computes row_hash
    for change detection, resolves dimension surrogate keys
    (SCD1/SCD2), and performs SCD Type 1 incremental updates.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Silver Load pipeline. The Bronze layer must be validated
    before this step. The procedure supports both FULL and
    INCREMENTAL load modes based on etl.config settings.

Notes:
    - The FACT uses the business key (SalesOrderID) as PK.
      No surrogate key is generated for FACT tables.
    - FULL load truncates the Silver FACT and reloads all
      records from Bronze.
    - INCREMENTAL load selects only rows with ModifiedDate
      greater than the last watermark and applies SCD1 logic:
        * UPDATE rows where row_hash has changed
        * INSERT new SalesOrderID not present in Silver
    - Dimension lookups:
        * Customer (SCD2): resolved using is_active = 1 OR
          order_date BETWEEN dwh_start_date AND dwh_end_date.
        * SalesPerson (SCD2): same SCD2 logic as Customer.
        * Territory (SCD1): resolved via direct business key.
    - row_hash is used to detect changes in FACT attributes.
    - Watermark (ModifiedDate) is updated after each load.
    - Quarantine is applied via etl.rejected to exclude
      invalid Bronze rows before loading Silver.

SCD Type:
    - FACT: SCD Type 1 (overwrite changes)
    - Customer: SCD Type 2
    - SalesPerson: SCD Type 2
    - Territory: SCD Type 1
*/

USE [DWH_AdventureWorks2025];
GO

ALTER PROCEDURE [etl].[usp_load_silver_sales_order_header]
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
        WHERE object_name = 'sales_order_header'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SALES_ORDER_HEADER.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES ORDER HEADER (FACT) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_sales_order_header;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.SalesOrderID      AS sales_order_id,
                    b.OrderDate         AS order_date,
                    b.DueDate           AS due_date,
                    b.ShipDate          AS ship_date,
                    b.Status            AS status,
                    b.OnlineOrderFlag   AS online_order_flag,
                    b.SalesOrderNumber  AS sales_order_number,
                    b.PurchaseOrderNumber AS purchase_order_number,
                    b.AccountNumber     AS account_number,
                    b.CustomerID        AS customer_id,
                    b.SalesPersonID     AS sales_person_id,
                    b.TerritoryID       AS territory_id,
                    b.CurrencyRateID    AS currency_rate_id,
                    b.SubTotal          AS sub_total,
                    b.TaxAmt            AS tax_amount,
                    b.Freight           AS freight,
                    b.TotalDue          AS total_due,
                    b.ModifiedDate      AS modified_date,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(b.SalesOrderID     AS VARCHAR(20)),
                            CAST(b.OrderDate        AS VARCHAR(30)),
                            CAST(b.DueDate          AS VARCHAR(30)),
                            CAST(b.ShipDate         AS VARCHAR(30)),
                            CAST(b.Status           AS VARCHAR(10)),
                            CAST(b.OnlineOrderFlag  AS VARCHAR(10)),
                            b.SalesOrderNumber,
                            b.PurchaseOrderNumber,
                            b.AccountNumber,
                            CAST(b.CustomerID       AS VARCHAR(20)),
                            CAST(b.SalesPersonID    AS VARCHAR(20)),
                            CAST(b.TerritoryID      AS VARCHAR(20)),
                            CAST(b.CurrencyRateID   AS VARCHAR(20)),
                            CAST(b.SubTotal         AS VARCHAR(50)),
                            CAST(b.TaxAmt           AS VARCHAR(50)),
                            CAST(b.Freight          AS VARCHAR(50)),
                            CAST(b.TotalDue         AS VARCHAR(50)),
                            CAST(b.ModifiedDate     AS VARCHAR(30))
                        )
                    ) AS row_hash
                INTO #src_full
                FROM bronze.erp_sales_order_header b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'sales_order_header'
                      AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Lookup SKs con CTE
                ---------------------------------------------------------
                ;WITH src_lkp AS (
                    SELECT
                        f.*,
                        COALESCE(c.customer_sk, -1) AS customer_sk,
                        COALESCE(sp.sales_person_sk, -1) AS person_sk,
                        COALESCE(t.territory_sk, -1) AS territory_sk
                    FROM #src_full f
                    LEFT JOIN silver.erp_customer c
                        ON f.customer_id = c.customer_id
                       AND (
                            c.is_active = 1
                            OR (f.order_date >= c.dwh_start_date AND f.order_date < c.dwh_end_date)
                       )
                    LEFT JOIN silver.erp_sales_person sp
                        ON f.sales_person_id = sp.person_id
                       AND (
                            sp.is_active = 1
                            OR (f.order_date >= sp.dwh_start_date AND f.order_date < sp.dwh_end_date)
                       )
                    LEFT JOIN silver.erp_sales_territory t
                        ON f.territory_id = t.territory_id
                )
                INSERT INTO silver.erp_sales_order_header
                    (sales_order_id,
                     order_date, due_date, ship_date,
                     status, online_order_flag,
                     sales_order_number, purchase_order_number, account_number,
                     customer_id, sales_person_id, territory_id, currency_rate_id,
                     sub_total, tax_amount, freight, total_due,
                     modified_date,
                     customer_sk, person_sk, territory_sk,
                     row_hash,
                     dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    src.sales_order_id,
                    src.order_date,
                    src.due_date,
                    src.ship_date,
                    src.status,
                    src.online_order_flag,
                    src.sales_order_number,
                    src.purchase_order_number,
                    src.account_number,
                    src.customer_id,
                    src.sales_person_id,
                    src.territory_id,
                    src.currency_rate_id,
                    src.sub_total,
                    src.tax_amount,
                    src.freight,
                    src.total_due,
                    src.modified_date,
                    src.customer_sk,
                    src.person_sk,
                    src.territory_sk,
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
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD1)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD1) started';

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.SalesOrderID      AS sales_order_id,
                    b.OrderDate         AS order_date,
                    b.DueDate           AS due_date,
                    b.ShipDate          AS ship_date,
                    b.Status            AS status,
                    b.OnlineOrderFlag   AS online_order_flag,
                    b.SalesOrderNumber  AS sales_order_number,
                    b.PurchaseOrderNumber AS purchase_order_number,
                    b.AccountNumber     AS account_number,
                    b.CustomerID        AS customer_id,
                    b.SalesPersonID     AS sales_person_id,
                    b.TerritoryID       AS territory_id,
                    b.CurrencyRateID    AS currency_rate_id,
                    b.SubTotal          AS sub_total,
                    b.TaxAmt            AS tax_amount,
                    b.Freight           AS freight,
                    b.TotalDue          AS total_due,
                    b.ModifiedDate      AS modified_date
                INTO #src_delta
                FROM bronze.erp_sales_order_header b
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'sales_order_header'
                          AND r.business_key = CAST(b.SalesOrderID AS NVARCHAR(200))
                    );

                SELECT @rows_src = COUNT(*) FROM #src_delta;

                PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                IF @rows_src = 0
                BEGIN
                    PRINT '>> No delta found. Nothing to do.';
                    DROP TABLE #src_delta;
                    ROLLBACK TRANSACTION;

                    SET @end_time = SYSDATETIME();
                    PRINT '>> INCREMENTAL load completed (no changes)';
                    PRINT '>> Duration: ' +
                          CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
                    PRINT '====================================================';
                    RETURN;
                END

                ---------------------------------------------------------
                -- Compute row_hash
                ---------------------------------------------------------
                UPDATE #src_delta
                SET row_hash = HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(sales_order_id   AS VARCHAR(20)),
                            CAST(order_date       AS VARCHAR(30)),
                            CAST(due_date         AS VARCHAR(30)),
                            CAST(ship_date        AS VARCHAR(30)),
                            CAST(status           AS VARCHAR(10)),
                            CAST(online_order_flag AS VARCHAR(10)),
                            sales_order_number,
                            purchase_order_number,
                            account_number,
                            CAST(customer_id      AS VARCHAR(20)),
                            CAST(sales_person_id  AS VARCHAR(20)),
                            CAST(territory_id     AS VARCHAR(20)),
                            CAST(currency_rate_id AS VARCHAR(20)),
                            CAST(sub_total        AS VARCHAR(50)),
                            CAST(tax_amount       AS VARCHAR(50)),
                            CAST(freight          AS VARCHAR(50)),
                            CAST(total_due        AS VARCHAR(50)),
                            CAST(modified_date    AS VARCHAR(30))
                        )
                    );

                ---------------------------------------------------------
                -- Lookup SKs con CTE
                ---------------------------------------------------------
                ;WITH src_lkp AS (
                    SELECT
                        d.*,
                        COALESCE(c.customer_sk, -1) AS customer_sk,
                        COALESCE(sp.sales_person_sk, -1) AS person_sk,
                        COALESCE(t.territory_sk, -1) AS territory_sk
                    FROM #src_delta d
                    LEFT JOIN silver.erp_customer c
                        ON d.customer_id = c.customer_id
                       AND (
                            c.is_active = 1
                            OR (d.order_date >= c.dwh_start_date AND d.order_date < c.dwh_end_date)
                       )
                    LEFT JOIN silver.erp_sales_person sp
                        ON d.sales_person_id = sp.person_id
                       AND (
                            sp.is_active = 1
                            OR (d.order_date >= sp.dwh_start_date AND d.order_date < sp.dwh_end_date)
                       )
                    LEFT JOIN silver.erp_sales_territory t
                        ON d.territory_id = t.territory_id
                )

                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.order_date         = src.order_date,
                    tgt.due_date           = src.due_date,
                    tgt.ship_date          = src.ship_date,
                    tgt.status             = src.status,
                    tgt.online_order_flag  = src.online_order_flag,
                    tgt.sales_order_number = src.sales_order_number,
                    tgt.purchase_order_number = src.purchase_order_number,
                    tgt.account_number     = src.account_number,
                    tgt.customer_id        = src.customer_id,
                    tgt.sales_person_id    = src.sales_person_id,
                    tgt.territory_id       = src.territory_id,
                    tgt.currency_rate_id   = src.currency_rate_id,
                    tgt.sub_total          = src.sub_total,
                    tgt.tax_amount         = src.tax_amount,
                    tgt.freight            = src.freight,
                    tgt.total_due          = src.total_due,
                    tgt.modified_date      = src.modified_date,
                    tgt.customer_sk        = src.customer_sk,
                    tgt.person_sk          = src.person_sk,
                    tgt.territory_sk       = src.territory_sk,
                    tgt.row_hash           = src.row_hash,
                    tgt.dwh_load_date      = @load_ts,
                    tgt.dwh_batch_id       = @batch_id
                FROM silver.erp_sales_order_header tgt
                JOIN src_lkp src
                    ON tgt.sales_order_id = src.sales_order_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_sales_order_header
                    (sales_order_id,
                     order_date, due_date, ship_date,
                     status, online_order_flag,
                     sales_order_number, purchase_order_number, account_number,
                     customer_id, sales_person_id, territory_id, currency_rate_id,
                     sub_total, tax_amount, freight, total_due,
                     modified_date,
                     customer_sk, person_sk, territory_sk,
                     row_hash,
                     dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    src.sales_order_id,
                    src.order_date,
                    src.due_date,
                    src.ship_date,
                    src.status,
                    src.online_order_flag,
                    src.sales_order_number,
                    src.purchase_order_number,
                    src.account_number,
                    src.customer_id,
                    src.sales_person_id,
                    src.territory_id,
                    src.currency_rate_id,
                    src.sub_total,
                    src.tax_amount,
                    src.freight,
                    src.total_due,
                    src.modified_date,
                    src.customer_sk,
                    src.person_sk,
                    src.territory_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM src_lkp src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_sales_order_header tgt
                    WHERE tgt.sales_order_id = src.sales_order_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(modified_date) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'sales_order_header'
                  AND is_active = 1;

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
            THROW 51020, 'Unsupported load type for SALES_ORDER_HEADER.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')       IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta')      IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_order_header';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
