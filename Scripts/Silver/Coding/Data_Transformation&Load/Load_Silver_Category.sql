/*
=========================================================
Silver Load – Category (SCD Type 1)
=========================================================
Purpose:
    Loads the Category dimension into Silver with SCD1 logic:
    one row per business key, attributes overwritten in place.

Load types:
    FULL:
        - TRUNCATE Silver
        - RESET surrogate key sequence
        - INSERT the entire deduplicated + quarantined source
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        - UPDATE changed rows (SK preserved)
        - INSERT new business keys
      Idempotent: a second run makes no further changes.

Source rules (same hardening as Bronze DQ):
    - Dedup: latest version per business key
    - Standardize Name (NULL/empty → 'Unknown')
    - Exclude rows rejected for THIS batch

Flow:
    1. Read load_type from etl.config
    2. Build #src (dedup + standardize + quarantine gate)
    3. FULL: TRUNCATE + RESET SEQUENCE + INSERT all rows
       INCREMENTAL: UPDATE changed + INSERT new
    4. Report counters

Deliberately not done:
    - No business logic already owned by the DQ check
    - No delete of missing keys by default (can orphan references)
    - Unknown member (SK = -1) is always preserved

SSIS Usage:
    EXEC etl.usp_load_silver_category ?
        param 0: batch_id (same variable used by Bronze DQ)
*/


USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_category]
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
            @rows_inserted    INT = 0,
            @rows_deleted     INT = 0;

    BEGIN TRY
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Configuration, the same source of truth as Bronze
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'category'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for category in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Loading Silver Category (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Source set, materialized in #src because it feeds both
        -- the UPDATE and the INSERT (a CTE lives for one
        -- statement only).
        ---------------------------------------------------------
        ;WITH cte AS (
            SELECT
                b.ProductCategoryID AS category_id,
                CASE
                    WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = '' THEN 'Unknown'
                    ELSE LTRIM(RTRIM(b.[Name]))
                END AS category_name,
                ROW_NUMBER() OVER (
                    PARTITION BY b.ProductCategoryID
                    ORDER BY b.ModifiedDate DESC, b.dwh_source_row_num DESC
                ) AS rn
            FROM bronze.erp_category AS b
            WHERE b.ProductCategoryID IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected AS r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'category'
                      AND r.business_key = CAST(b.ProductCategoryID AS NVARCHAR(200))
                  )
        )
        SELECT
            category_id,
            category_name,
            HASHBYTES('SHA2_256',
                      CONCAT_WS('|', CAST(category_id AS VARCHAR(20)), category_name)
                     ) AS row_hash
        INTO #src
        FROM cte
        WHERE rn = 1;

        SET @rows_src = @@ROWCOUNT;

        PRINT '>> Source rows (after dedup and quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncating Silver and resetting sequence';

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_category;

                ALTER SEQUENCE dwh.seq_silver_category RESTART WITH 1;

                INSERT INTO silver.erp_category
                    (category_sk, category_id, category_name, row_hash,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_category OVER (ORDER BY src.category_id),
                    src.category_id,
                    src.category_name,
                    src.row_hash,
                    @load_ts,
                    @batch_id
                FROM #src AS src;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src;

            SET @end_time = SYSDATETIME();

            PRINT '>> FULL load completed';
            PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
            PRINT '>> Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
            PRINT '====================================================';
            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (original logic preserved)
        ---------------------------------------------------------
        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- 1) UPDATE the rows that changed (SCD1: overwrite)
            ---------------------------------------------------------
            UPDATE tgt
            SET tgt.category_name = src.category_name,
                tgt.row_hash      = src.row_hash,
                tgt.dwh_load_date = @load_ts,
                tgt.dwh_batch_id  = @batch_id
            FROM silver.erp_category AS tgt
            INNER JOIN #src AS src
                ON src.category_id = tgt.category_id
            WHERE tgt.row_hash IS NULL
               OR tgt.row_hash <> src.row_hash;

            SET @rows_updated = @@ROWCOUNT;

            ---------------------------------------------------------
            -- 2) INSERT the new business keys
            ---------------------------------------------------------
            INSERT INTO silver.erp_category
                (category_sk, category_id, category_name, row_hash,
                 dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_category OVER (ORDER BY src.category_id),
                src.category_id,
                src.category_name,
                src.row_hash,
                @load_ts,
                @batch_id
            FROM #src AS src
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_category AS tgt
                    WHERE tgt.category_id = src.category_id
                  );

            SET @rows_inserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE #src;

        SET @end_time = SYSDATETIME();

        PRINT '>> INCREMENTAL load completed';
        PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src') IS NOT NULL
            DROP TABLE #src;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: category';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO


