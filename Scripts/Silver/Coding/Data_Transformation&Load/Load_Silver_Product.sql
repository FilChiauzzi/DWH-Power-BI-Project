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

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_product]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @start_time DATETIME2(3) = SYSDATETIME(),
        @end_time   DATETIME2(3),
        @load_ts    DATETIME2(3) = SYSDATETIME(),
        @load_type  VARCHAR(20),
        @last_wm    DATETIME,
        @new_wm     DATETIME,
        @rows_src   INT = 0,
        @rows_closed INT = 0,
        @rows_inserted INT = 0;

    BEGIN TRY

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
            THROW 51000, 'No active configuration found for product.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – Product (SCD2)';
        PRINT 'Batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL LOAD STARTED';

        -----------------------------------------------------
        -- Build #src FULL (NO watermark)
        ----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.ProductID AS product_id,

                    CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                         THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END AS product_name,

                    b.ProductNumber,
                    b.MakeFlag,
                    b.FinishedGoodsFlag,

                    CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                         THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END AS color,

                    b.SafetyStockLevel,
                    b.ReorderPoint,
                    b.StandardCost,
                    b.ListPrice,

                    CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                         THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END AS size,

                    CASE WHEN b.SizeUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.SizeUnitMeasureCode)) = '' 
                         THEN 'N/A' ELSE b.SizeUnitMeasureCode END AS size_unit_measure_code,

                    CASE WHEN b.WeightUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.WeightUnitMeasureCode)) = '' 
                         THEN 'N/A' ELSE b.WeightUnitMeasureCode END AS weight_unit_measure_code,

                    b.Weight AS weight,
                    b.DaysToManufacture AS days_to_manufacture,

                    CASE WHEN b.ProductLine IS NULL OR LTRIM(RTRIM(b.ProductLine)) = '' 
                         THEN 'N/A' ELSE b.ProductLine END AS product_line,

                    CASE WHEN b.Class IS NULL OR LTRIM(RTRIM(b.Class)) = '' 
                         THEN 'N/A' ELSE b.Class END AS class,

                    CASE WHEN b.Style IS NULL OR LTRIM(RTRIM(b.Style)) = '' 
                         THEN 'N/A' ELSE b.Style END AS style,

                    CAST(b.SellStartDate AS DATE) AS sell_start_date,
                    CAST(b.SellEndDate   AS DATE) AS sell_end_date,

                    sc.subcategory_sk,

                    -------------------------------------------------
                    -- Descriptive columns
                    -------------------------------------------------
                    CASE WHEN b.MakeFlag = 1 THEN 'Manufactured'
                         WHEN b.MakeFlag = 0 THEN 'Purchased'
                         ELSE 'Unknown' END AS make_flag_desc,

                    CASE WHEN b.FinishedGoodsFlag = 1 THEN 'Salable'
                         WHEN b.FinishedGoodsFlag = 0 THEN 'Not salable'
                         ELSE 'Unknown' END AS finished_goods_flag_desc,

                    CASE WHEN b.ProductLine = 'R' THEN 'Road'
                         WHEN b.ProductLine = 'M' THEN 'Mountain'
                         WHEN b.ProductLine = 'T' THEN 'Touring'
                         WHEN b.ProductLine = 'S' THEN 'Standard'
                         ELSE 'Unknown' END AS product_line_desc,

                    CASE WHEN b.Class = 'H' THEN 'High'
                         WHEN b.Class = 'M' THEN 'Medium'
                         WHEN b.Class = 'L' THEN 'Low'
                         ELSE 'Unknown' END AS class_desc,

                    CASE WHEN b.Style = 'W' THEN 'Womens'
                         WHEN b.Style = 'M' THEN 'Mens'
                         WHEN b.Style = 'U' THEN 'Universal'
                         ELSE 'Unknown' END AS style_desc,

                    CASE WHEN b.SizeUnitMeasureCode = 'CM' THEN 'Centimeters'
                         WHEN b.SizeUnitMeasureCode = 'IN' THEN 'Inches'
                         WHEN b.SizeUnitMeasureCode = 'MM' THEN 'Millimeters'
                         ELSE 'Unknown' END AS size_unit_desc,

                    CASE WHEN b.WeightUnitMeasureCode = 'KG' THEN 'Kilograms'
                         WHEN b.WeightUnitMeasureCode = 'LB' THEN 'Pounds'
                         ELSE 'Unknown' END AS weight_unit_desc,

                    -------------------------------------------------
                    -- Hash SCD2
                    -------------------------------------------------
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
                            CASE WHEN b.SizeUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.SizeUnitMeasureCode)) = '' 
                                 THEN 'N/A' ELSE b.SizeUnitMeasureCode END,
                            CASE WHEN b.WeightUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.WeightUnitMeasureCode)) = '' 
                                 THEN 'N/A' ELSE b.WeightUnitMeasureCode END,
                            b.Weight,
                            b.DaysToManufacture,
                            CASE WHEN b.ProductLine IS NULL OR LTRIM(RTRIM(b.ProductLine)) = '' 
                                 THEN 'N/A' ELSE b.ProductLine END,
                            CASE WHEN b.Class IS NULL OR LTRIM(RTRIM(b.Class)) = '' 
                                 THEN 'N/A' ELSE b.Class END,
                            CASE WHEN b.Style IS NULL OR LTRIM(RTRIM(b.Style)) = '' 
                                 THEN 'N/A' ELSE b.Style END,
                            CAST(b.SellStartDate AS DATE),
                            CAST(b.SellEndDate   AS DATE),
                            sc.subcategory_sk
                        )
                    ) AS row_hash,

                    b.ModifiedDate,
                    b.dwh_source_row_num
                FROM bronze.erp_product b
                LEFT JOIN silver.erp_subcategory sc
                    ON b.ProductSubcategoryID = sc.subcategory_id
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected AS r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'product'
                          AND r.business_key = CAST(b.ProductID AS NVARCHAR(200))
                )
            )
            SELECT *
            INTO #src
            FROM cte;

            SET @rows_src = @@ROWCOUNT;

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_product;

                ALTER SEQUENCE dwh.seq_silver_product RESTART WITH 1;

                INSERT INTO silver.erp_product (
                    product_sk,
                    product_id,
                    product_name,
                    product_number,
                    make_flag,
                    make_flag_desc,
                    finished_goods_flag,
                    finished_goods_flag_desc,
                    color,
                    safety_stock_level,
                    reorder_point,
                    standard_cost,
                    list_price,
                    size,
                    size_unit_measure_code,
                    size_unit_desc,
                    weight_unit_measure_code,
                    weight_unit_desc,
                    weight,
                    days_to_manufacture,
                    product_line,
                    product_line_desc,
                    class,
                    class_desc,
                    style,
                    style_desc,
                    sell_start_date,
                    sell_end_date,
                    product_subcategory_sk,
                    row_hash,
                    dwh_create_date,
                    dwh_start_date,
                    dwh_end_date,
                    is_active,
                    dwh_load_date,
                    dwh_batch_id
                )
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_product OVER (ORDER BY src.product_id),
                    src.product_id,
                    src.product_name,
                    src.ProductNumber,
                    src.MakeFlag,
                    src.make_flag_desc,
                    src.FinishedGoodsFlag,
                    src.finished_goods_flag_desc,
                    src.color,
                    src.SafetyStockLevel,
                    src.ReorderPoint,
                    src.StandardCost,
                    src.ListPrice,
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
                    src.subcategory_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    DATEFROMPARTS(9999,12,31),
                    1,
                    @load_ts,
                    @batch_id
                FROM #src src;

            COMMIT TRANSACTION;

            DROP TABLE #src;

            PRINT '>> FULL load completed';
            RETURN;
        ---------------------------------------------------------
        -- Build #src: DELTA Bronze + exclude rejected
        ---------------------------------------------------------
        IF @load_type = 'incremental'
        BEGIN
        PRINT '>> INCREMENTAL LOAD STARTED';
        ;WITH cte AS (
            SELECT
                b.ProductID AS product_id,

                CASE WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = '' 
                     THEN 'Unknown' ELSE LTRIM(RTRIM(b.Name)) END AS product_name,

                b.ProductNumber,
                b.MakeFlag,
                b.FinishedGoodsFlag,

                CASE WHEN b.Color IS NULL OR LTRIM(RTRIM(b.Color)) = '' 
                     THEN 'Unknown' ELSE LTRIM(RTRIM(b.Color)) END AS color,

                b.SafetyStockLevel,
                b.ReorderPoint,
                b.StandardCost,
                b.ListPrice,

                CASE WHEN b.Size IS NULL OR LTRIM(RTRIM(b.Size)) = '' 
                     THEN 'N/A' ELSE LTRIM(RTRIM(b.Size)) END AS size,

                CASE WHEN b.SizeUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.SizeUnitMeasureCode)) = '' 
                     THEN 'N/A' ELSE b.SizeUnitMeasureCode END AS size_unit_measure_code,

                CASE WHEN b.WeightUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.WeightUnitMeasureCode)) = '' 
                     THEN 'N/A' ELSE b.WeightUnitMeasureCode END AS weight_unit_measure_code,

                b.Weight AS weight,
                b.DaysToManufacture AS days_to_manufacture,

                CASE WHEN b.ProductLine IS NULL OR LTRIM(RTRIM(b.ProductLine)) = '' 
                     THEN 'N/A' ELSE b.ProductLine END AS product_line,

                CASE WHEN b.Class IS NULL OR LTRIM(RTRIM(b.Class)) = '' 
                     THEN 'N/A' ELSE b.Class END AS class,

                CASE WHEN b.Style IS NULL OR LTRIM(RTRIM(b.Style)) = '' 
                     THEN 'N/A' ELSE b.Style END AS style,

                CAST(b.SellStartDate AS DATE) AS sell_start_date,
                CAST(b.SellEndDate   AS DATE) AS sell_end_date,

                sc.subcategory_sk,

                -------------------------------------------------
                -- Descriptive columns
                -------------------------------------------------
                CASE WHEN b.MakeFlag = 1 THEN 'Manufactured'
                     WHEN b.MakeFlag = 0 THEN 'Purchased'
                     ELSE 'Unknown' END AS make_flag_desc,

                CASE WHEN b.FinishedGoodsFlag = 1 THEN 'Salable'
                     WHEN b.FinishedGoodsFlag = 0 THEN 'Not salable'
                     ELSE 'Unknown' END AS finished_goods_flag_desc,

                CASE WHEN b.ProductLine = 'R' THEN 'Road'
                     WHEN b.ProductLine = 'M' THEN 'Mountain'
                     WHEN b.ProductLine = 'T' THEN 'Touring'
                     WHEN b.ProductLine = 'S' THEN 'Standard'
                     ELSE 'Unknown' END AS product_line_desc,

                CASE WHEN b.Class = 'H' THEN 'High'
                     WHEN b.Class = 'M' THEN 'Medium'
                     WHEN b.Class = 'L' THEN 'Low'
                     ELSE 'Unknown' END AS class_desc,

                CASE WHEN b.Style = 'W' THEN 'Womens'
                     WHEN b.Style = 'M' THEN 'Mens'
                     WHEN b.Style = 'U' THEN 'Universal'
                     ELSE 'Unknown' END AS style_desc,

                CASE WHEN b.SizeUnitMeasureCode = 'CM' THEN 'Centimeters'
                     WHEN b.SizeUnitMeasureCode = 'IN' THEN 'Inches'
                     WHEN b.SizeUnitMeasureCode = 'MM' THEN 'Millimeters'
                     ELSE 'Unknown' END AS size_unit_desc,

                CASE WHEN b.WeightUnitMeasureCode = 'KG' THEN 'Kilograms'
                     WHEN b.WeightUnitMeasureCode = 'LB' THEN 'Pounds'
                     ELSE 'Unknown' END AS weight_unit_desc,

                -------------------------------------------------
                -- Hash SCD2
                -------------------------------------------------
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
                        CASE WHEN b.SizeUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.SizeUnitMeasureCode)) = '' 
                             THEN 'N/A' ELSE b.SizeUnitMeasureCode END,
                        CASE WHEN b.WeightUnitMeasureCode IS NULL OR LTRIM(RTRIM(b.WeightUnitMeasureCode)) = '' 
                             THEN 'N/A' ELSE b.WeightUnitMeasureCode END,
                        b.Weight,
                        b.DaysToManufacture,
                        CASE WHEN b.ProductLine IS NULL OR LTRIM(RTRIM(b.ProductLine)) = '' 
                             THEN 'N/A' ELSE b.ProductLine END,
                        CASE WHEN b.Class IS NULL OR LTRIM(RTRIM(b.Class)) = '' 
                             THEN 'N/A' ELSE b.Class END,
                        CASE WHEN b.Style IS NULL OR LTRIM(RTRIM(b.Style)) = '' 
                             THEN 'N/A' ELSE b.Style END,
                        CAST(b.SellStartDate AS DATE),
                        CAST(b.SellEndDate   AS DATE),
                        sc.subcategory_sk
                    )
                ) AS row_hash,

                b.ModifiedDate,
                b.dwh_source_row_num
            FROM bronze.erp_product b
            LEFT JOIN silver.erp_subcategory sc
                ON b.ProductSubcategoryID = sc.subcategory_id
            WHERE b.ModifiedDate > @last_wm
              AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'product'
                      AND r.business_key = CAST(b.ProductID AS NVARCHAR(200))
              )
        )
        SELECT *
        INTO #src
        FROM cte;

        SET @rows_src = @@ROWCOUNT;
        PRINT '>> Delta rows from Bronze (valid): ' + CAST(@rows_src AS NVARCHAR(20));

        IF @rows_src = 0
        BEGIN
            PRINT '>> No delta found. Nothing to do.';
            RETURN;
        END

        ---------------------------------------------------------
        -- Compute new watermark
        ---------------------------------------------------------
        SELECT @new_wm = MAX(ModifiedDate) FROM #src;

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD2)
        ---------------------------------------------------------
        BEGIN TRANSACTION;

            -----------------------------------------------------
            -- Close previous versions
            -----------------------------------------------------
            UPDATE tgt
            SET tgt.dwh_end_date  = @load_ts,
                tgt.is_active     = 0,
                tgt.dwh_load_date = @load_ts,
                tgt.dwh_batch_id  = @batch_id
            FROM silver.erp_product tgt
            JOIN #src src
                ON tgt.product_id = src.product_id
            WHERE tgt.is_active = 1
              AND tgt.row_hash <> src.row_hash;

            SET @rows_closed = @@ROWCOUNT;

            -----------------------------------------------------
            -- Insert new versions
            -----------------------------------------------------
            INSERT INTO silver.erp_product (
                product_sk,
                product_id,
                product_name,
                product_number,
                make_flag,
                make_flag_desc,
                finished_goods_flag,
                finished_goods_flag_desc,
                color,
                safety_stock_level,
                reorder_point,
                standard_cost,
                list_price,
                size,
                size_unit_measure_code,
                size_unit_desc,
                weight_unit_measure_code,
                weight_unit_desc,
                weight,
                days_to_manufacture,
                product_line,
                product_line_desc,
                class,
                class_desc,
                style,
                style_desc,
                sell_start_date,
                sell_end_date,
                product_subcategory_sk,
                row_hash,
                dwh_create_date,
                dwh_start_date,
                dwh_end_date,
                is_active,
                dwh_load_date,
                dwh_batch_id
            )
            SELECT
                NEXT VALUE FOR dwh.seq_silver_product OVER (ORDER BY src.product_id),
                src.product_id,
                src.product_name,
                src.ProductNumber,
                src.MakeFlag,
                src.make_flag_desc,
                src.FinishedGoodsFlag,
                src.finished_goods_flag_desc,
                src.color,
                src.SafetyStockLevel,
                src.ReorderPoint,
                src.StandardCost,
                src.ListPrice,
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
                src.subcategory_sk,
                src.row_hash,
                @load_ts,
                @load_ts,
                DATEFROMPARTS(9999,12,31),
                1,
                @load_ts,
                @batch_id
            FROM #src src;

            SET @rows_inserted = @@ROWCOUNT;
            
            DROP TABLE #src;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Update watermark
        ---------------------------------------------------------
        UPDATE etl.config
        SET last_watermark_value = @new_wm
        WHERE object_name = 'product';

        SET @end_time = SYSDATETIME();

        PRINT '>> Rows closed:   ' + CAST(@rows_closed AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';
        END
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for product';
        PRINT ERROR_MESSAGE();
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
