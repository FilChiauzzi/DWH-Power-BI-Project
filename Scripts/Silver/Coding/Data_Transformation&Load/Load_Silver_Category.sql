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


ALTER PROCEDURE [etl].[usp_load_silver_category]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @start_time DATETIME2(3) = SYSDATETIME(),
        @end_time   DATETIME2(3),
        @load_ts    DATETIME2(3) = SYSDATETIME(),
        @rows_src   INT = 0,
        @rows_ins   INT = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Silver Load – CATEGORY (FULL ONLY) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Source set (after quarantine)
        ---------------------------------------------------------
        SELECT
            b.ProductCategoryID AS category_id,

            CASE
                WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(b.[Name]))
            END AS category_name,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(b.ProductCategoryID AS VARCHAR(20)),
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END
                )
            ) AS row_hash
        INTO #src
        FROM bronze.erp_category b
        WHERE NOT EXISTS (
            SELECT 1
            FROM etl.rejected r
            WHERE r.batch_id    = @batch_id
              AND r.layer        = 'bronze'
              AND r.object_name  = 'category'
              AND r.business_key = CAST(b.ProductCategoryID AS NVARCHAR(200))
        );

        SELECT @rows_src = COUNT(*) FROM #src;

        PRINT '>> Source rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD ONLY
        ---------------------------------------------------------
        PRINT '>> FULL load: truncate + reset sequence + reload';

        BEGIN TRANSACTION;

            TRUNCATE TABLE silver.erp_category;

            ALTER SEQUENCE dwh.seq_silver_category RESTART WITH 1;

            INSERT INTO silver.erp_category
                (category_sk, category_id, category_name,
                 row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_category OVER (ORDER BY src.category_id),
                src.category_id,
                src.category_name,
                src.row_hash,
                @load_ts,
                @load_ts,
                @batch_id
            FROM #src AS src;

            SET @rows_ins = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE #src;

        SET @end_time = SYSDATETIME();

        PRINT '>> FULL load completed';
        PRINT '>> Rows inserted: ' + CAST(@rows_ins AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

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


