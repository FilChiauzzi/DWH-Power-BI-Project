/*
=========================================================
Silver Load – Subcategory
=========================================================
Purpose:
    This stored procedure loads the Silver-layer table
    for the Subcategory domain. It applies basic cleaning
    rules, removes duplicates using ModifiedDate ordering,
    resets the surrogate key sequence, and inserts the
    refined data into silver.erp_subcategory.

SSIS Usage:
    SSIS executes this procedure inside the metadata-driven
    Silver Load pipeline. The Bronze layer must be validated
    before this step. The procedure truncates the Silver
    table and reloads it entirely.

Notes:
    - Surrogate keys (subcategory_sk) are generated using
      dwh.seq_silver_subcategory.
    - Only the most recent record per ProductSubcategoryID
      is selected (ROW_NUMBER logic).
    - category_sk is resolved by joining Silver Category.
    - SCD (type 1)
*/

USE [DWH_AdventureWorks2025]
GO
/****** Oggetto: StoredProcedure [etl].[usp_load_silver_subcategory]    Data dello script 15/10/2026 12:19:01 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_subcategory]
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
        WHERE object_name = 'subcategory'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SUBCATEGORY.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SUBCATEGORY (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_subcategory;

                ALTER SEQUENCE dwh.seq_silver_subcategory RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                ;WITH cte AS (
                    SELECT
                        b.ProductSubcategoryID AS subcategory_id,
                        CASE
                            WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                THEN 'Unknown'
                            ELSE LTRIM(RTRIM(b.[Name]))
                        END AS subcategory_name,
                        b.ProductCategoryID     AS product_category_id,
                        b.ModifiedDate,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.ProductSubcategoryID
                            ORDER BY b.ModifiedDate DESC, b.dwh_source_row_num DESC
                        ) AS rn
                    FROM bronze.erp_subcategory b
                    WHERE b.ProductSubcategoryID IS NOT NULL
                      AND NOT EXISTS (
                            SELECT 1
                            FROM etl.rejected r
                            WHERE r.batch_id    = @batch_id
                              AND r.layer        = 'bronze'
                              AND r.object_name  = 'subcategory'
                              AND r.business_key = CAST(b.ProductSubcategoryID AS NVARCHAR(200))
                        )
                )
                SELECT
                    cte.subcategory_id,
                    cte.subcategory_name,
                    cte.product_category_id,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(cte.subcategory_id       AS VARCHAR(20)),
                            cte.subcategory_name,
                            CAST(cte.product_category_id  AS VARCHAR(20))
                        )
                    ) AS row_hash,
                    MAX(cte.ModifiedDate) AS ModifiedDate
                INTO #src_full
                FROM cte
                WHERE cte.rn = 1
                GROUP BY cte.subcategory_id,
                         cte.subcategory_name,
                         cte.product_category_id;

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_subcategory
                    (subcategory_sk, subcategory_id, subcategory_name,
                     category_sk, row_hash, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_subcategory OVER (ORDER BY src.subcategory_id),
                    src.subcategory_id,
                    src.subcategory_name,
                    tc.category_sk,
                    src.row_hash,
                    @load_ts,
                    @batch_id
                FROM #src_full src
                LEFT JOIN silver.erp_category tc
                    ON src.product_category_id = tc.category_id;

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
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                ;WITH cte AS (
                    SELECT
                        b.ProductSubcategoryID AS subcategory_id,
                        CASE
                            WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' 
                                THEN 'Unknown'
                            ELSE LTRIM(RTRIM(b.[Name]))
                        END AS subcategory_name,
                        b.ProductCategoryID     AS product_category_id,
                        b.ModifiedDate,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.ProductSubcategoryID
                            ORDER BY b.ModifiedDate DESC, b.dwh_source_row_num DESC
                        ) AS rn
                    FROM bronze.erp_subcategory b
                    WHERE b.ModifiedDate > @last_wm
                      AND b.ProductSubcategoryID IS NOT NULL
                      AND NOT EXISTS (
                            SELECT 1
                            FROM etl.rejected r
                            WHERE r.batch_id    = @batch_id
                              AND r.layer        = 'bronze'
                              AND r.object_name  = 'subcategory'
                              AND r.business_key = CAST(b.ProductSubcategoryID AS NVARCHAR(200))
                        )
                )
                SELECT
                    cte.subcategory_id,
                    cte.subcategory_name,
                    cte.product_category_id,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            CAST(cte.subcategory_id       AS VARCHAR(20)),
                            cte.subcategory_name,
                            CAST(cte.product_category_id  AS VARCHAR(20))
                        )
                    ) AS row_hash,
                    MAX(cte.ModifiedDate) AS ModifiedDate
                INTO #src_delta
                FROM cte
                WHERE cte.rn = 1
                GROUP BY cte.subcategory_id,
                         cte.subcategory_name,
                         cte.product_category_id;

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
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.subcategory_name = src.subcategory_name,
                    tgt.category_sk      = tc.category_sk,
                    tgt.row_hash         = src.row_hash,
                    tgt.dwh_load_date    = @load_ts,
                    tgt.dwh_batch_id     = @batch_id
                FROM silver.erp_subcategory tgt
                INNER JOIN #src_delta src
                    ON src.subcategory_id = tgt.subcategory_id
                LEFT JOIN silver.erp_category tc
                    ON src.product_category_id = tc.category_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_subcategory
                    (subcategory_sk, subcategory_id, subcategory_name,
                     category_sk, row_hash, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_subcategory OVER (ORDER BY src.subcategory_id),
                    src.subcategory_id,
                    src.subcategory_name,
                    tc.category_sk,
                    src.row_hash,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_category tc
                    ON src.product_category_id = tc.category_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_subcategory tgt
                    WHERE tgt.subcategory_id = src.subcategory_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'subcategory'
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
            THROW 51020, 'Unsupported load type for SUBCATEGORY.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: subcategory';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;

