/*
=========================================================
Bronze Data Quality Check – Ship Method
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Ship Method domain.
    It checks for null business keys, duplicate keys, and
    incomplete descriptive attributes such as missing names
    or missing shipping cost components (ShipBase, ShipRate).

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure for
    each imported source table. Each procedure logs issues
    into etl.data_quality_log and returns the number of
    detected errors. Blocking errors (NULL or duplicate
    business keys) are also inserted into etl.rejected.

Warning:
    Running this procedure will delete previous quality logs
    for the Ship Method object in the Bronze layer. Only
    blocking errors prevent the Silver load; warnings are
    informational and do not stop the pipeline.
*/

USE [DWH_AdventureWorks2025]
GO
/****** Oggetto: StoredProcedure [etl].[usp_load_silver_ship_method]    Data dello script 16/10/2026 10:30:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [etl].[usp_load_silver_ship_method]
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
                WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(b.Name))
            END AS ship_name,

            b.ShipBase AS ship_base,
            b.ShipRate AS ship_rate,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    CAST(b.ShipMethodID AS VARCHAR(20)),
                    CASE
                        WHEN b.Name IS NULL OR LTRIM(RTRIM(b.Name)) = ''
                            THEN 'Unknown'
                        ELSE LTRIM(RTRIM(b.Name))
                    END,
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
