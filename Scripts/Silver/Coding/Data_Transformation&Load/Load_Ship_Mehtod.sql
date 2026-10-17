/*
=========================================================
Silver Load – Ship Method (SCD1 – FULL ONLY)
=========================================================
Purpose:
    This stored procedure loads the Silver-layer table for
    the Ship Method domain. Since the source contains only a
    few static rows and does not change over time, the load
    strategy is FULL-only: the Silver table is truncated and
    fully reloaded at each batch execution.

    The procedure applies business rules (Unknown name
    substitution), computes row_hash for change detection,
    and generates surrogate keys using the sequence
    dwh.seq_silver_ship_method.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS reads etl.config to determine the
    load type, but for Ship Method the load is always FULL.
    Rows rejected during Bronze DQ are excluded from the load.

Warning:
    Running this procedure will delete all previous Silver
    data for Ship Method. This is intentional, as the domain
    is static and incremental logic would add unnecessary
    complexity without any benefit.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_ship_method]
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
        PRINT 'Silver Load – SHIP_METHOD (FULL ONLY) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Source set (after quarantine)
        ---------------------------------------------------------
        SELECT
            b.ShipMethodID AS ship_method_id,

            CASE
                WHEN b.Name IS NULL OR TRIM(b.Name) = ''
                    THEN 'Unknown'
                ELSE TRIM(b.Name)
            END AS ship_name,

            b.ShipBase AS ship_base,
            b.ShipRate AS ship_rate,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(b.ShipMethodID AS VARCHAR(20)),
                    CASE WHEN b.Name IS NULL OR TRIM(b.Name) = '' THEN 'Unknown' ELSE TRIM(b.Name) END,
                    CAST(ISNULL(b.ShipBase,0) AS VARCHAR(50)),
                    CAST(ISNULL(b.ShipRate,0) AS VARCHAR(50))
                )
            ) AS row_hash
        INTO #src
        FROM bronze.erp_ship_method b
        WHERE NOT EXISTS (
            SELECT 1
            FROM etl.rejected r
            WHERE r.batch_id    = @batch_id
              AND r.layer        = 'bronze'
              AND r.object_name  = 'ship_method'
              AND r.business_key = CAST(b.ShipMethodID AS NVARCHAR(200))
        );

        SELECT @rows_src = COUNT(*) FROM #src;

        PRINT '>> Source rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD ONLY
        ---------------------------------------------------------
        PRINT '>> FULL load: truncate + reset sequence + reload';

        BEGIN TRANSACTION;

            TRUNCATE TABLE silver.erp_ship_method;

            -------------------------------------------------------
            -- INSERT UNKNOWN MEMBER
            -------------------------------------------------------
            IF NOT EXISTS (SELECT 1 FROM silver.erp_ship_method WHERE ship_method_sk = -1)
                INSERT INTO silver.erp_ship_method (
                    ship_method_sk, ship_method_id, ship_name,
                    ship_base, ship_rate, row_hash,
                    dwh_create_date, dwh_load_date, dwh_batch_id
                )
                VALUES (
                    -1, -1, 'Unknown',
                    0, 0, NULL,
                    @load_ts, @load_ts, -1
                );

            -------------------------------------------------------
            -- INSERT ALL RECORDS + SURROGATE KEY
            -------------------------------------------------------
            ALTER SEQUENCE dwh.seq_silver_ship_method RESTART WITH 1;

            INSERT INTO silver.erp_ship_method
                (ship_method_sk, ship_method_id, ship_name,
                 ship_base, ship_rate, row_hash,
                 dwh_create_date, dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_ship_method OVER (ORDER BY src.ship_method_id),
                src.ship_method_id,
                src.ship_name,
                src.ship_base,
                src.ship_rate,
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
        PRINT 'ERROR during Silver load for: ship_method';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
