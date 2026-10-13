/*
=========================================================
Silver Load - Category  (SCD Type 1)
=========================================================
Purpose:
    Loads the category dimension into Silver with SCD Type 1
    logic: a change overwrites the attribute in place, no
    history is kept, one row per business key.

Design: SCD1 as a RECONCILE, not a reload
    UPDATE the changed rows (surrogate key preserved) and
    INSERT the new keys. Run it twice and the second run
    changes nothing (idempotent).

Why not TRUNCATE + ALTER SEQUENCE RESTART:
    That pattern reassigned the surrogate keys on every run.
    As soon as a business key disappears from the source (or a
    new one is inserted earlier in the ordering), every
    following category_sk shifts, and whatever references it
    (silver.erp_subcategory.category_sk, later the Gold facts)
    silently points to the wrong category.
    It also left the dimension EMPTY for the duration of the
    load: had the INSERT failed, readers would have seen
    nothing. The reconcile never leaves that window.

The flow of this procedure:
    1. read load_type from etl.config
    3. build #src: deduplicated, standardized, and filtered by
       the quarantine written by the DQ check
    4. UPDATE the changed rows, INSERT the new ones, in one
       transaction
    5. report the counters

What is deliberately NOT done here:
    - No TRUNCATE, no sequence restart (see above).
    - No business logic that the DQ check already owns: the
      quarantine comes from there, this procedure only reads it.
    - The optional delete of rows that disappeared from the
      source is included but commented: deleting a dimension
      row can orphan whatever references it. It is a decision,
      not a default, and it carries a guard for the Unknown
      member (SK = -1) which must never be deleted.

Source hardening rules:
    - Dedup: latest version per business key, tiebreaker on
      dwh_source_row_num DESC (with equal ModifiedDate the
      result would otherwise be random).
    - Business key NOT NULL: last line of defence, the DQ check
      already quarantines those rows.
    - Quarantine gate: any row rejected for THIS batch stays out.

SSIS Usage:
    EXEC etl.usp_load_silver_category ?
        param 0: batch_id (Input, the same SSIS variable used by
                 the DQ task, so the gate sees the right rejects).
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
            -- ONE timestamp for the whole batch: every row of this
            -- load carries the same dwh_load_date, so no timeline
            -- gap can appear between the statements.
            @load_ts          DATETIME2(3) = SYSDATETIME(),
            @load_type        VARCHAR(20),
            @rows_src         INT = 0,
            @rows_updated     INT = 0,
            @rows_inserted    INT = 0,
            @rows_deleted     INT = 0;   -- used by the optional step 3

    BEGIN TRY
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Configuration, the same source of truth as Bronze
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'category'
          AND is_active = 1;
        -- NOTE: if etl.config holds one row per layer
        --       (object_type bronze / silver), add here:
        --       AND object_type = 'silver'

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for category in etl.config.', 1;

        PRINT '====================================================';
        PRINT 'Loading Silver Category (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
            -- CONCAT_WS needs at least TWO values to join (separator +
            -- two arguments minimum): with a single value SQL Server
            -- raises error 189. The business key is included as the
            -- first value, which is both valid and useful: it makes
            -- the fingerprint unique per member.
            HASHBYTES('SHA2_256',
                      CONCAT_WS('|', CAST(category_id AS VARCHAR(20)), category_name)
                     ) AS row_hash
        INTO #src
        FROM cte
        WHERE rn = 1;

        SET @rows_src = @@ROWCOUNT;

        PRINT '>> Source rows (after dedup and quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- 1) UPDATE the rows that changed (SCD1: overwrite)
            --    The surrogate key is preserved, so nothing that
            --    references it breaks on a reload.
            --    The Unknown member (category_id = -1) never
            --    matches a Bronze row, so it is untouched.
            ---------------------------------------------------------
            UPDATE tgt
            SET tgt.category_name = src.category_name,
                tgt.row_hash      = src.row_hash,
                tgt.dwh_load_date = @load_ts,
                tgt.dwh_batch_id  = @batch_id
            FROM silver.erp_category AS tgt
            INNER JOIN #src AS src
                ON src.category_id = tgt.category_id
            WHERE tgt.row_hash IS NULL           -- rows loaded before row_hash existed
               OR tgt.row_hash <> src.row_hash;  -- attributes that changed

            SET @rows_updated = @@ROWCOUNT;

            ---------------------------------------------------------
            -- 2) INSERT the new business keys
            --    dwh_create_date takes its DEFAULT here: it is the
            --    creation date and must never be updated later.
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

            ---------------------------------------------------------
            -- 3) OPTIONAL: keys that disappeared from the source.
            --    Only meaningful on a FULL load, where the source
            --    snapshot is authoritative; on an incremental load
            --    the absence of a key says nothing.
            --    Two guards are essential:
            --      - category_sk > 0, so the Unknown member
            --        (SK = -1) is never deleted;
            --      - think twice before enabling it at all: a hard
            --        delete can orphan whatever references the row.
            --        A soft delete (is_deleted flag, filtered out
            --        in the Gold views) is the safer variant.
            ---------------------------------------------------------
            /*
            IF @load_type = 'full'
            BEGIN
                DELETE tgt
                FROM silver.erp_category AS tgt
                WHERE tgt.category_sk > 0
                  AND NOT EXISTS (
                        SELECT 1 FROM #src AS src
                        WHERE src.category_id = tgt.category_id
                      );

                SET @rows_deleted = @@ROWCOUNT;
            END
            */

        COMMIT TRANSACTION;

        DROP TABLE #src;

        SET @end_time = SYSDATETIME();

        PRINT '>> Rows updated:  ' + CAST(@rows_updated  AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Load Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';
        PRINT 'Loading Silver Category completed | batch: ' +
              CAST(@batch_id AS NVARCHAR(20)) + ' | ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, SYSDATETIME()) AS NVARCHAR(20)) + ' ms';
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


