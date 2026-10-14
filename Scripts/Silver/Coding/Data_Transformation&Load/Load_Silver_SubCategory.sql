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

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_subcategory]
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
            @rows_src         INT = 0,
            @rows_updated     INT = 0,
            @rows_inserted    INT = 0;

    BEGIN TRY
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'subcategory'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for subcategory in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – Subcategory (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Build source set (#src): dedup + standardize + quarantine gate
        ---------------------------------------------------------
        ;WITH cte AS (
            SELECT
                b.ProductSubcategoryID AS subcategory_id,
                CASE
                    WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' THEN 'Unknown'
                    ELSE LTRIM(RTRIM(b.[Name]))
                END AS subcategory_name,
                b.ProductCategoryID,
                ROW_NUMBER() OVER (
                    PARTITION BY b.ProductSubcategoryID
                    ORDER BY b.ModifiedDate DESC, b.dwh_source_row_num DESC
                ) AS rn
            FROM bronze.erp_subcategory AS b
            WHERE b.ProductSubcategoryID IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'subcategory'
                      AND r.business_key = CAST(b.ProductSubcategoryID AS NVARCHAR(200))
                  )
        )
        SELECT
            subcategory_id,
            subcategory_name,
            ProductCategoryID,
            HASHBYTES('SHA2_256',
                      CONCAT_WS('|',
                                CAST(subcategory_id AS VARCHAR(20)),
                                subcategory_name,
                                CAST(ProductCategoryID AS VARCHAR(20))
                      )
                     ) AS row_hash
        INTO #src
        FROM cte
        WHERE rn = 1;

        SET @rows_src = @@ROWCOUNT;
        PRINT '>> Source rows (after dedup + quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_subcategory;

                ALTER SEQUENCE dwh.seq_silver_subcategory RESTART WITH 1;

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
                FROM #src AS src
                LEFT JOIN silver.erp_category AS tc
                    ON src.ProductCategoryID = tc.category_id;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src;

            SET @end_time = SYSDATETIME();

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' +
                  CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD1 reconcile)
        ---------------------------------------------------------
        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- UPDATE changed rows
            ---------------------------------------------------------
            UPDATE tgt
            SET tgt.subcategory_name = src.subcategory_name,
                tgt.category_sk      = tc.category_sk,
                tgt.row_hash         = src.row_hash,
                tgt.dwh_load_date    = @load_ts,
                tgt.dwh_batch_id     = @batch_id
            FROM silver.erp_subcategory AS tgt
            INNER JOIN #src AS src
                ON src.subcategory_id = tgt.subcategory_id
            LEFT JOIN silver.erp_category AS tc
                ON src.ProductCategoryID = tc.category_id
            WHERE tgt.row_hash IS NULL
               OR tgt.row_hash <> src.row_hash;

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
            FROM #src AS src
            LEFT JOIN silver.erp_category AS tc
                ON src.ProductCategoryID = tc.category_id
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_subcategory AS tgt
                    WHERE tgt.subcategory_id = src.subcategory_id
                  );

            SET @rows_inserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE #src;

        SET @end_time = SYSDATETIME();

        PRINT '>> INCREMENTAL load completed';
        PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

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
GO
