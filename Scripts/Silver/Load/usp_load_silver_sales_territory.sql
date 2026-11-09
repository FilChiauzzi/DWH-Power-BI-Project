/*
=========================================================
Silver Load – Sales Territory (SCD1, FULL only)
=========================================================
Purpose:
    Loads silver.erp_sales_territory from
    bronze.erp_sales_territory with SCD Type 1 logic. The
    source is a small, stable lookup (about 10 rows), so
    only the FULL load is supported. It trims text, replaces
    an empty Name or Group with 'Unknown' and an empty
    CountryRegionCode with 'N/A', and resolves country_sk
    from silver.erp_country_region. Rows quarantined in
    etl.rejected for the current batch are excluded, and a
    default member (surrogate key -1, 'Unknown') is always
    kept.

    If etl.config asks for an incremental load, the
    procedure throws an error on purpose.

    Every execution is logged in etl.load_audit (layer silver,
    step load): status, duration, row counts and, on
    failure, the error.
SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline, after
    the Bronze quality check of the same object and after
    silver.erp_country_region. SSIS passes the current
    @batch_id. The load type (full or incremental) is read
    from etl.config.

Warning:
    Every run truncates silver.erp_sales_territory and
    restarts its surrogate key sequence. Tables that store
    country or territory keys must be reloaded afterwards.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_sales_territory]
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

    -- Audit (etl.load_audit): declared before TRY, the CATCH uses them
    DECLARE
        @audit_id   BIGINT,
        @proc_name  NVARCHAR(256) =
            QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));

    BEGIN TRY
        ---------------------------------------------------------
        -- Audit: open the step (outside any transaction)
        ---------------------------------------------------------
        EXEC etl.usp_audit_start
            @batch_id       = @batch_id,
            @layer          = 'silver',
            @object_name    = 'sales_territory',
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES TERRITORY (FULL ONLY) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Source set (after quarantine)
        ---------------------------------------------------------
        SELECT
            st.TerritoryID AS territory_id,

            CASE
                WHEN st.Name IS NULL OR TRIM(st.Name) = ''
                    THEN 'Unknown'
                ELSE TRIM(st.Name)
            END AS territory_name,

            CASE
                WHEN st.[Group] IS NULL OR TRIM(st.[Group]) = ''
                    THEN 'Unknown'
                ELSE TRIM(st.[Group])
            END AS continent_name,

            CASE
                WHEN st.CountryRegionCode IS NULL OR TRIM(st.CountryRegionCode) = ''
                    THEN 'N/A'
                ELSE TRIM(st.CountryRegionCode)
            END AS country_code,

            cr.country_sk,

            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    st.TerritoryID,
                    CASE WHEN st.Name IS NULL OR TRIM(st.Name) = '' THEN 'Unknown' ELSE TRIM(st.Name) END,
                    CASE WHEN st.[Group] IS NULL OR TRIM(st.[Group]) = '' THEN 'Unknown' ELSE TRIM(st.[Group]) END,
                    CASE WHEN st.CountryRegionCode IS NULL OR TRIM(st.CountryRegionCode) = '' THEN 'N/A' ELSE TRIM(st.CountryRegionCode) END
                )
            ) AS row_hash

        INTO #src
        FROM bronze.erp_sales_territory st
        LEFT JOIN silver.erp_country_region cr
            ON TRIM(st.CountryRegionCode) = cr.country_code
        WHERE NOT EXISTS (
            SELECT 1
            FROM etl.rejected r
            WHERE r.batch_id    = @batch_id
              AND r.layer        = 'bronze'
              AND r.object_name  = 'sales_territory'
              AND r.business_key = CAST(st.TerritoryID AS NVARCHAR(200))
        );

        SELECT @rows_src = COUNT(*) FROM #src;

        PRINT '>> Source rows (after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

        ---------------------------------------------------------
        -- FULL LOAD ONLY
        ---------------------------------------------------------
        PRINT '>> FULL load: truncate + reset sequence + reload';

        BEGIN TRANSACTION;

            TRUNCATE TABLE silver.erp_sales_territory;

            -------------------------------------------------------
            -- INSERT UNKNOWN MEMBER
            -------------------------------------------------------
            IF NOT EXISTS (SELECT 1 FROM silver.erp_sales_territory WHERE territory_sk = -1)
                INSERT INTO silver.erp_sales_territory (
                    territory_sk, territory_id, territory_name,
                    country_code, continent_name, country_sk,
                    row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                )
                VALUES (
                    -1, -1, 'Unknown',
                    'N/A', 'Unknown', NULL,
                    NULL, @load_ts, @load_ts, -1
                );

            -------------------------------------------------------
            -- INSERT ALL RECORDS + SURROGATE KEY
            -------------------------------------------------------
            ALTER SEQUENCE dwh.seq_silver_sales_territory RESTART WITH 1;

            INSERT INTO silver.erp_sales_territory
                (territory_sk, territory_id, territory_name,
                 country_code, continent_name, country_sk,
                 row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
            SELECT
                NEXT VALUE FOR dwh.seq_silver_sales_territory OVER (ORDER BY src.territory_id),
                src.territory_id,
                src.territory_name,
                src.country_code,
                src.continent_name,
                src.country_sk,
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

        -- Audit: load completed
        EXEC etl.usp_audit_end
            @audit_id      = @audit_id,
            @status        = 'succeeded',
            @load_type     = 'full',
            @rows_read     = @rows_src,
            @rows_inserted = @rows_ins;

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_territory';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id  = @audit_id,
                @status    = 'failed',
                @load_type = 'full';

        THROW;
    END CATCH
END;
GO
