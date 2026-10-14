/*
=========================================================
Silver Load – Product (SCD2)
=========================================================
Purpose:
    Loads the Silver-layer Product table using SCD Type 2.
    Applies cleaning rules, resolves foreign keys, computes
    row_hash, and inserts deduplicated records.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Silver Load pipeline. Bronze must be validated first.
    FULL load truncates and reloads the entire Silver table.
    INCREMENTAL load performs SCD2 logic.

Notes:
    - Surrogate keys generated via dwh.seq_silver_product
    - Deduplication via ROW_NUMBER() over rowguid
    - SCD2 fields: dwh_start_date, dwh_end_date, is_active
    - Full just for the first time, scheduled refresh with incremnetal load
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_product]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start_time DATETIME2(3) = SYSDATETIME(),
            @start_time       DATETIME2(3),
            @end_time         DATETIME2(3),
            @load_ts          DATETIME2(3) = SYSDATETIME(),
            @load_type        VARCHAR(20),
            @last_wm          DATETIME,
            @new_wm           DATETIME,
            @rows_src         INT = 0,
            @rows_closed      INT = 0,
            @rows_inserted    INT = 0;

    BEGIN TRY
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @last_wm   = last_watermark_value
        FROM etl.config
        WHERE object_name = 'product'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for product in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – PRODUCT (SCD2) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_product;
                ALTER SEQUENCE dwh.seq_silver_product RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.ProductID AS product_id,
                    CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                         THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END AS product_name,
                    b.ProductNumber AS product_number,
                    b.MakeFlag AS make_flag,
                    CASE WHEN b.MakeFlag = 1 THEN 'Manufactured'
                         WHEN b.MakeFlag = 0 THEN 'Purchased'
                         ELSE 'Unknown' END AS make_flag_desc,
                    b.FinishedGoodsFlag AS finished_goods_flag,
                    CASE WHEN b.FinishedGoodsFlag = 1 THEN 'Salable'
                         WHEN b.FinishedGoodsFlag = 0 THEN 'Not salable'
                         ELSE 'Unknown' END AS finished_goods_flag_desc,
                    CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                         THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END AS color,
                    b.SafetyStockLevel AS safety_stock_level,
                    b.ReorderPoint AS reorder_point,
                    b.StandardCost AS standard_cost,
                    b.ListPrice AS list_price,
                    CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                         THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END AS size,
                    b.SizeUnitMeasureCode AS size_unit_measure_code,
                    CASE WHEN b.SizeUnitMeasureCode = 'CM' THEN 'Centimeters'
                         WHEN b.SizeUnitMeasureCode = 'IN' THEN 'Inches'
                         WHEN b.SizeUnitMeasureCode = 'MM' THEN 'Millimeters'
                         ELSE 'Unknown' END AS size_unit_desc,
                    b.WeightUnitMeasureCode AS weight_unit_measure_code,
                    CASE WHEN b.WeightUnitMeasureCode = 'KG' THEN 'Kilograms'
                         WHEN b.WeightUnitMeasureCode = 'LB' THEN 'Pounds'
                         ELSE 'Unknown' END AS weight_unit_desc,
                    b.Weight AS weight,
                    b.DaysToManufacture AS days_to_manufacture,
                    b.ProductLine AS product_line,
                    CASE WHEN b.ProductLine = 'R' THEN 'Road'
                         WHEN b.ProductLine = 'M' THEN 'Mountain'
                         WHEN b.ProductLine = 'T' THEN 'Touring'
                         WHEN b.ProductLine = 'S' THEN 'Standard'
                         ELSE 'Unknown' END AS product_line_desc,
                    b.Class AS class,
                    CASE WHEN b.Class = 'H' THEN 'High'
                         WHEN b.Class = 'M' THEN 'Medium'
                         WHEN b.Class = 'L' THEN 'Low'
                         ELSE 'Unknown' END AS class_desc,
                    b.Style AS style,
                    CASE WHEN b.Style = 'W' THEN 'Womens'
                         WHEN b.Style = 'M' THEN 'Mens'
                         WHEN b.Style = 'U' THEN 'Universal'
                         ELSE 'Unknown' END AS style_desc,
                    CAST(b.SellStartDate AS DATE) AS sell_start_date,
                    CAST(b.SellEndDate   AS DATE) AS sell_end_date,
                    sc.subcategory_sk AS product_subcategory_sk,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.ProductID,
                            CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                                 THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END,
                            b.ProductNumber,
                            b.MakeFlag,
                            b.FinishedGoodsFlag,
                            CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                                 THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END,
                            CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                                 THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END,
                            b.SizeUnitMeasureCode,
                            b.WeightUnitMeasureCode,
                            b.Weight,
                            b.DaysToManufacture,
                            b.ProductLine,
                            b.Class,
                            b.Style,
                            CAST(b.SellStartDate AS DATE),
                            CAST(b.SellEndDate   AS DATE),
                            sc.subcategory_sk
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_product AS b
                LEFT JOIN silver.erp_subcategory AS sc
                    ON b.ProductSubcategoryID = sc.subcategory_id
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected AS r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'product'
                          AND r.business_key = CAST(b.ProductID AS NVARCHAR(200))
                      );

                SET @rows_src = @@ROWCOUNT;
                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD2: only active rows)
                ---------------------------------------------------------
                INSERT INTO silver.erp_product
                    (product_sk, product_id, product_name, product_number,
                     make_flag, make_flag_desc,
                     finished_goods_flag, finished_goods_flag_desc,
                     color,
                     safety_stock_level, reorder_point,
                     standard_cost, list_price,
                     size, size_unit_measure_code, size_unit_desc,
                     weight_unit_measure_code, weight_unit_desc,
                     weight,
                     days_to_manufacture,
                     product_line, product_line_desc,
                     class, class_desc,
                     style, style_desc,
                     sell_start_date, sell_end_date,
                     product_subcategory_sk,
                     row_hash,
                     dwh_create_date, dwh_start_date, dwh_end_date,
                     is_active,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_product OVER (ORDER BY src.product_id),
                    src.product_id,
                    src.product_name,
                    src.product_number,
                    src.make_flag,
                    src.make_flag_desc,
                    src.finished_goods_flag,
                    src.finished_goods_flag_desc,
                    src.color,
                    src.safety_stock_level,
                    src.reorder_point,
                    src.standard_cost,
                    src.list_price,
                    src.size,
                    src.size_unit_measure_code,
                    src.size_unit_desc,
                    src.weight_unit_measure_code,
                    src.weight_unit_desc,
                    src.weight,
                    src.days_to_manufacture,
                    src.product_line,
                    src.product_line_desc,
                    src.class,
                    src.class_desc,
                    src.style,
                    src.style_desc,
                    src.sell_start_date,
                    src.sell_end_date,
                    src.product_subcategory_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    DATEFROMPARTS(9999,12,31),
                    1,
                    @load_ts,
                    @batch_id
                FROM #src_full AS src;

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
        -- INCREMENTAL LOAD (SCD2)
        ---------------------------------------------------------
        PRINT '>> INCREMENTAL load (SCD2) started';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Build DELTA source set (#src_delta)
            ---------------------------------------------------------
            SELECT
                b.ProductID AS product_id,
                CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                     THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END AS product_name,
                b.ProductNumber AS product_number,
                b.MakeFlag AS make_flag,
                CASE WHEN b.MakeFlag = 1 THEN 'Manufactured'
                     WHEN b.MakeFlag = 0 THEN 'Purchased'
                     ELSE 'Unknown' END AS make_flag_desc,
                b.FinishedGoodsFlag AS finished_goods_flag,
                CASE WHEN b.FinishedGoodsFlag = 1 THEN 'Salable'
                     WHEN b.FinishedGoodsFlag = 0 THEN 'Not salable'
                     ELSE 'Unknown' END AS finished_goods_flag_desc,
                CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                     THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END AS color,
                b.SafetyStockLevel AS safety_stock_level,
                b.ReorderPoint AS reorder_point,
                b.StandardCost AS standard_cost,
                b.ListPrice AS list_price,
                CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                     THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END AS size,
                b.SizeUnitMeasureCode AS size_unit_measure_code,
                CASE WHEN b.SizeUnitMeasureCode = 'CM' THEN 'Centimeters'
                     WHEN b.SizeUnitMeasureCode = 'IN' THEN 'Inches'
                     WHEN b.SizeUnitMeasureCode = 'MM' THEN 'Millimeters'
                     ELSE 'Unknown' END AS size_unit_desc,
                b.WeightUnitMeasureCode AS weight_unit_measure_code,
                CASE WHEN b.WeightUnitMeasureCode = 'KG' THEN 'Kilograms'
                     WHEN b.WeightUnitMeasureCode = 'LB' THEN 'Pounds'
                     ELSE 'Unknown' END AS weight_unit_desc,
                b.Weight AS weight,
                b.DaysToManufacture AS days_to_manufacture,
                b.ProductLine AS product_line,
                CASE WHEN b.ProductLine = 'R' THEN 'Road'
                     WHEN b.ProductLine = 'M' THEN 'Mountain'
                     WHEN b.ProductLine = 'T' THEN 'Touring'
                     WHEN b.ProductLine = 'S' THEN 'Standard'
                     ELSE 'Unknown' END AS product_line_desc,
                b.Class AS class,
                CASE WHEN b.Class = 'H' THEN 'High'
                     WHEN b.Class = 'M' THEN 'Medium'
                     WHEN b.Class = 'L' THEN 'Low'
                     ELSE 'Unknown' END AS class_desc,
                b.Style AS style,
                CASE WHEN b.Style = 'W' THEN 'Womens'
                     WHEN b.Style = 'M' THEN 'Mens'
                     WHEN b.Style = 'U' THEN 'Universal'
                     ELSE 'Unknown' END AS style_desc,
                CAST(b.SellStartDate AS DATE) AS sell_start_date,
                CAST(b.SellEndDate   AS DATE) AS sell_end_date,
                sc.subcategory_sk AS product_subcategory_sk,
                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        b.ProductID,
                        CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                             THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END,
                        b.ProductNumber,
                        b.MakeFlag,
                        b.FinishedGoodsFlag,
                        CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                             THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END,
                        CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                             THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END,
                        b.SizeUnitMeasureCode,
                        b.WeightUnitMeasureCode,
                        b.Weight,
                        b.DaysToManufacture,
                        b.ProductLine,
                        b.Class,
                        b.Style,
                        CAST(b.SellStartDate AS DATE),
                        CAST(b.SellEndDate   AS DATE),
                        sc.subcategory_sk
                    )
                ) AS row_hash,
                b.ModifiedDate
            INTO #src_delta
            FROM bronze.erp_product AS b
            LEFT JOIN silver.erp_subcategory AS sc
                ON b.ProductSubcategoryID = sc.subcategory_id
            WHERE b.ModifiedDate > @last_wm
              AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'product'
                      AND r.business_key = CAST(b.ProductID AS NVARCHAR(200))
                  );

            SET @rows_src = @@ROWCOUNT;
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
            -- Close old versions (SCD2)
            ---------------------------------------------------------
            UPDATE tgt
            SET tgt.dwh_end_date   = @load_ts,
                tgt.is_active      = 0,
                tgt.dwh_load_date  = @load_ts,
                tgt.dwh_batch_id   = @batch_id
            FROM silver.erp_product AS tgt
            INNER JOIN #src_delta AS src
                ON src.product_id = tgt.product_id
            WHERE tgt.is_active = 1
              AND tgt.row_hash <> src.row_hash;

            SET @rows_closed = @@ROWCOUNT;

            ---------------------------------------------------------
            -- Insert new versions (SCD2)
            ---------------------------------------------------------
            INSERT INTO silver.erp_product
                (product_sk, product_id, product_name, product_number,
                 make_flag, make_flag_desc,
                 finished_goods_flag, finished_goods_flag_desc,
                 color,
                 safety_stock_level, reorder_point,
                 standard_cost, list_price,
                 size, size_unit_measure_code, size_unit_desc,
                 weight_unit_measure_code, weight_unit_desc,
                 weight,
                 days_to_manufacture,
                 product_line, product_line_desc,
                 class, class_desc,
                 style, style_desc,
                 sell_start_date, sell_end_date,
                 product_subcategory_sk,
                 row_hash,
                 dwh_create_date, dwh_start_date, dwh_end_date,
                 is_active,
                 dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_product OVER (ORDER BY src.product_id),
                src.product_id,
                src.product_name,
                src.product_number,
                src.make_flag,
                src.make_flag_desc,
                src.finished_goods_flag,
                src.finished_goods_flag_desc,
                src.color,
                src.safety_stock_level,
                src.reorder_point,
                src.standard_cost,
                src.list_price,
                src.size,
                src.size_unit_measure_code,
                src.size_unit_desc,
                src.weight_unit_measure_code,
                src.weight_unit_desc,
                src.weight,
                src.days_to_manufacture,
                src.product_line,
                src.product_line_desc,
                src.class,
                src.class_desc,
                src.style,
                src.style_desc,
                src.sell_start_date,
                src.sell_end_date,
                src.product_subcategory_sk,
                src.row_hash,
                @load_ts,
                @load_ts,
                DATEFROMPARTS(9999,12,31),
                1,
                @load_ts,
                @batch_id
            FROM #src_delta AS src;

            SET @rows_inserted = @@ROWCOUNT;

            ---------------------------------------------------------
            -- Update watermark
            ---------------------------------------------------------
            SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

            UPDATE etl.config
            SET last_watermark_value = @new_wm
            WHERE object_name = 'product'
              AND is_active = 1;

        COMMIT TRANSACTION;

        DROP TABLE #src_delta;

        SET @end_time = SYSDATETIME();

        PRINT '>> INCREMENTAL load completed';
        PRINT '>> Rows closed:   ' + CAST(@rows_closed   AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full') IS NOT NULL
            DROP TABLE #src_full;

        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL
            DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: product';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
