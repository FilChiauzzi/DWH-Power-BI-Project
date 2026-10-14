/*
=========================================================
Silver Load – sales_territory (SCD Type 1, FULL ONLY)
=========================================================
Purpose:
    Loads the SALES TERRITORY dimension into the Silver layer
    using SCD Type 1 logic. Only one version per business key
    (TerritoryID) is kept. Because the source contains only a
    small, stable lookup (≈10 rows), this object supports
    FULL load only.

Load types:
    FULL:
        - TRUNCATE Silver table
        - RESET surrogate key sequence
        - INSERT all valid rows from Bronze (excluding quarantined)
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        Not supported.
        If load_type = 'incremental' is configured in etl.config,
        the procedure throws an exception intentionally. This
        prevents accidental misconfiguration by data engineers.

Source rules (from Bronze DQ):
    Bronze already guarantees:
        * TerritoryID not NULL
        * TerritoryID unique
        * Group not NULL/empty
        * CountryRegionCode not NULL/empty
        * Name may be NULL/empty → warning only
        * Quarantine of invalid rows into etl.rejected

Silver transformations:
    * TRIM normalization of Name, Group, CountryRegionCode
    * Name NULL/empty → substituted with 'Unknown'
    * Group NULL/empty → substituted with 'Unknown'
    * CountryRegionCode NULL/empty → substituted with 'N/A'
    * row_hash (SHA2-256) used for change detection (future-proof)
    * Surrogate key generated via dwh.seq_silver_sales_territory
    * Lookup of country_sk from Silver CountryRegion

Flow:
    1. Read load_type from etl.config
    2. If load_type <> 'full' → THROW (FULL ONLY)
    3. FULL:
         - Build #src_full (clean + quarantine gate)
         - TRUNCATE + RESET SEQUENCE + INSERT all rows
    4. Print counters and duration

Why FULL ONLY:
    SalesTerritory is a small, stable lookup table (≈10 rows).
    Incremental logic adds no value and increases complexity.
    FULL reload is simpler, safer, and guarantees consistency.

Deliberately not done:
    - No incremental logic
    - No delete of missing keys (SCD1 never deletes)
    - No ROW_NUMBER() or dwh_source_row_num
      (Bronze DQ already ensures one valid row per business key)
    - No business logic already owned by Bronze DQ

SSIS Usage:
    EXEC etl.usp_load_silver_sales_territory ?
        param 0: batch_id (same variable used by Bronze DQ)

Recommendation:
    In etl.config, add a "notes" column documenting that
    sales_territory supports FULL load only.

Prerequisite:
    Run 00_etl_ddl.sql first (etl.batch, etl.rejected,
    etl.data_quality_log, sequences, Silver tables).
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
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @load_ts          DATETIME2(3) = SYSDATETIME(),
        @load_type        VARCHAR(20),
        @rows_src         INT = 0,
        @rows_inserted    INT = 0;

    BEGIN TRY
        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT @load_type = load_type
        FROM etl.config
        WHERE object_name = 'sales_territory'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for SALES_TERRITORY.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – SALES TERRITORY (FULL ONLY) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Reject unsupported load types
        ---------------------------------------------------------
        IF @load_type <> 'full'
        BEGIN
            THROW 51021, 'SALES_TERRITORY supports FULL load only. Check etl.config.', 1;
        END

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        SET @start_time = SYSDATETIME();

        BEGIN TRANSACTION;

            PRINT '>> FULL LOAD: Truncating silver.erp_sales_territory';
            TRUNCATE TABLE silver.erp_sales_territory;

            PRINT '>> FULL LOAD: Resetting sequence dwh.seq_silver_sales_territory';
            ALTER SEQUENCE dwh.seq_silver_sales_territory RESTART WITH 1;

            ---------------------------------------------------------
            -- Build FULL source set (#src_full)
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

            INTO #src_full
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

            SELECT @rows_src = COUNT(*) FROM #src_full;

            PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            ---------------------------------------------------------
            -- Insert FULL (SCD1)
            ---------------------------------------------------------
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
            FROM #src_full src;

            SET @rows_inserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE #src_full;

        SET @end_time = SYSDATETIME();

        PRINT '>> FULL load completed';
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' ms';
        PRINT '====================================================';

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full') IS NOT NULL DROP TABLE #src_full;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: sales_territory';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
