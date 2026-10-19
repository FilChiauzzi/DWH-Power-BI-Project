/*
=========================================================
Silver Load – Subcategory (SCD1)
=========================================================
Purpose:
    Loads silver.erp_subcategory from bronze.erp_subcategory
    with SCD Type 1 logic. It keeps the latest version per
    ProductSubcategoryID, resolves category_sk from
    silver.erp_category and uses a row_hash to detect
    changes.

    FULL truncates, restarts the surrogate key sequence and
    reloads in one transaction. INCREMENTAL reads only the
    Bronze rows loaded after the last Silver load
    (MAX(dwh_load_date) of the Silver table), then updates
    rows whose row_hash changed and inserts new business
    keys.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    silver.erp_category. SSIS passes the current @batch_id.
    The load type (full or incremental) is read from
    etl.config.

Warning:
    A FULL load truncates silver.erp_subcategory and
    restarts its surrogate key sequence. Tables that store
    its surrogate keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_subcategory]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time        DATETIME2(3),
        @end_time          DATETIME2(3),
        @load_ts           DATETIME2(3) = SYSDATETIME(),
        @load_type         VARCHAR(20),
        @last_silver_load  DATETIME2(3),
        @rows_src          INT = 0,
        @rows_updated      INT = 0,
        @rows_inserted     INT = 0;

    BEGIN TRY
        ---------------------------------------------------------
        -- Read configuration (load_type only: no watermark here)
        ---------------------------------------------------------
        SELECT @load_type = load_type
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

            -----------------------------------------------------
            -- Build FULL source (#src_full): all Bronze, latest
            -- version per id, after quarantine
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.ProductSubcategoryID AS subcategory_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS subcategory_name,
                    b.ProductCategoryID AS product_category_id
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
                        CAST(cte.subcategory_id      AS VARCHAR(20)),
                        cte.subcategory_name,
                        CAST(cte.product_category_id AS VARCHAR(20))
                    )
                ) AS row_hash
            INTO #src_full
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze subcategory is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_subcategory;

                ALTER SEQUENCE dwh.seq_silver_subcategory RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_subcategory WHERE subcategory_sk = -1)
                    INSERT INTO silver.erp_subcategory (
                        subcategory_sk, subcategory_id, subcategory_name, category_sk,
                        row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown', -1,
                        NULL, @load_ts, @load_ts, -1
                    );

                -------------------------------------------------------
                -- INSERT ALL RECORDS + SURROGATE KEY
                -------------------------------------------------------
                INSERT INTO silver.erp_subcategory
                    (subcategory_sk, subcategory_id, subcategory_name,
                     category_sk, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_subcategory OVER (ORDER BY src.subcategory_id),
                    src.subcategory_id,
                    src.subcategory_name,
                    tc.category_sk,
                    src.row_hash,
                    @load_ts,
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

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -- (empty Silver -> 1900, so everything is read)
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_subcategory;

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

            -----------------------------------------------------
            -- Build DELTA (#src_delta): Bronze rows that arrived
            -- after the last Silver load
            -----------------------------------------------------
            ;WITH cte AS (
                SELECT
                    b.ProductSubcategoryID AS subcategory_id,
                    CASE
                        WHEN b.[Name] IS NULL OR LTRIM(RTRIM(b.[Name])) = ''
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.[Name]))
                    END AS subcategory_name,
                    b.ProductCategoryID AS product_category_id
                FROM bronze.erp_subcategory b
                WHERE b.dwh_load_date > @last_silver_load
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
                        CAST(cte.subcategory_id      AS VARCHAR(20)),
                        cte.subcategory_name,
                        CAST(cte.product_category_id AS VARCHAR(20))
                    )
                ) AS row_hash
            INTO #src_delta
            FROM cte;

            SELECT @rows_src = COUNT(*) FROM #src_delta;

            PRINT '>> Delta rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            IF @rows_src = 0
            BEGIN
                PRINT '>> No delta found. Nothing to do.';
                DROP TABLE #src_delta;
                PRINT '====================================================';
                RETURN;
            END

            -----------------------------------------------------
            -- UPDATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                -- UPDATE changed rows (hash or category_sk differs)
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

                -- INSERT new business keys
                INSERT INTO silver.erp_subcategory
                    (subcategory_sk, subcategory_id, subcategory_name,
                     category_sk, row_hash, dwh_created_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_subcategory OVER (ORDER BY src.subcategory_id),
                    src.subcategory_id,
                    src.subcategory_name,
                    tc.category_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_category tc
                    ON src.product_category_id = tc.category_id
                LEFT JOIN silver.erp_subcategory tgt
                    ON tgt.subcategory_id = src.subcategory_id
                WHERE tgt.subcategory_id IS NULL;


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
