/*
=========================================================
Silver Load – customer (SCD Type 2)
=========================================================
Purpose:
    Loads the CUSTOMER dimension into the Silver layer using
    Slowly Changing Dimension Type 2 logic. Each change to a
    customer creates a new version of the record, preserving
    historical accuracy for analytical reporting.

Load types:
    FULL:
        - TRUNCATE Silver table
        - RESET surrogate key sequence
        - INSERT all valid rows from Bronze (excluding quarantined)
      The dimension is rebuilt from scratch and aligned to Bronze.

    INCREMENTAL:
        - SELECT only rows with ModifiedDate > last_watermark_value
        - CLOSE previous active versions when row_hash changes
        - INSERT new versions with updated attributes
        - UPDATE watermark
      Idempotent: re-running the same batch produces no changes.

Source rules (from Bronze DQ):
    Bronze already guarantees:
        * CustomerID not NULL
        * CustomerID unique
        * TerritoryID not NULL
        * Quarantine of invalid rows into etl.rejected
    PersonID and StoreID may be NULL (optional foreign keys).

Silver transformations:
    * Derivation of customer_type:
        - store            → PersonID NULL, StoreID NOT NULL
        - store contact    → PersonID NOT NULL, StoreID NOT NULL
        - individual       → PersonID NOT NULL, StoreID NULL
    * Lookup of surrogate keys:
        - person_sk from Silver Person
        - store_sk from Silver Store
        - territory_sk from Silver SalesTerritory
    * row_hash (SHA2-256) used for change detection
    * SCD2 versioning:
        - dwh_start_date
        - dwh_end_date (default 9999-12-31)
        - is_active flag
    * Exclusion of quarantined rows

Flow:
    1. Read load_type and last_watermark_value from etl.config
    2. FULL:
         - Build #src_full (clean + quarantine gate)
         - TRUNCATE + RESET SEQUENCE + INSERT all rows
    3. INCREMENTAL:
         - Build #src_delta (ModifiedDate > watermark)
         - Close active versions where row_hash changed
         - Insert new versions
         - Update watermark
    4. Print counters and duration

SCD2 logic:
    A new version is created when:
        - PersonID changes
        - StoreID changes
        - TerritoryID changes
    These attributes define the customer’s business identity.
    row_hash ensures accurate and efficient change detection.

Deliberately not done:
    - No delete of missing keys (SCD2 never deletes)
    - No ROW_NUMBER() or dwh_source_row_num
      (Bronze DQ already ensures one valid row per business key)
    - No business logic already owned by Bronze DQ

SSIS Usage:
    EXEC etl.usp_load_silver_customer ?
        param 0: batch_id (same variable used by Bronze DQ)

Prerequisite:
    Run 00_etl_ddl.sql first (etl.batch, etl.rejected,
    etl.data_quality_log, sequences, Silver tables).
*/

USE [DWH_AdventureWorks2025]
GO
/****** Oggetto: StoredProcedure [etl].[usp_load_silver_customer]    Data dello script 17/10/2026 18:33:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_load_silver_customer]
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @start_time        DATETIME2(3),
            @end_time          DATETIME2(3),
            @load_ts           DATETIME2(3) = SYSDATETIME(),
            @load_type         VARCHAR(20),
            @last_silver_load  DATETIME2(3),
            @rows_src          INT = 0,
            @rows_closed       INT = 0,
            @rows_inserted     INT = 0;

    BEGIN TRY
    SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type
        FROM etl.config
        WHERE object_name = 'customer'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for CUSTOMER.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – CUSTOMER (SCD2) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            PRINT '>> FULL load: truncate + reset sequence + reload';

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    c.CustomerID AS customer_id,
                    c.PersonID   AS person_id,
                    c.StoreID    AS store_id,
                    c.TerritoryID AS territory_id,
                    CASE
                        WHEN c.PersonID IS NULL AND c.StoreID IS NOT NULL THEN 'store'
                        WHEN c.PersonID IS NOT NULL AND c.StoreID IS NOT NULL THEN 'store contact'
                        ELSE 'individual customer'
                    END AS customer_type,
                    p.person_sk,
                    s.store_sk,
                    t.territory_sk,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            c.PersonID,
                            c.StoreID,
                            c.TerritoryID
                        )
                    ) AS row_hash,
                    c.ModifiedDate
                INTO #src_full
                FROM bronze.erp_customer c
                LEFT JOIN silver.erp_person p
                    ON c.PersonID = p.person_id
                LEFT JOIN silver.erp_store s
                    ON c.StoreID = s.store_id
                LEFT JOIN silver.erp_sales_territory t
                    ON c.TerritoryID = t.territory_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'customer'
                      AND r.business_key = CAST(c.CustomerID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze person is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_customer;

                ALTER SEQUENCE dwh.seq_silver_customer RESTART WITH 1;

                ---------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN
                ---------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_customer WHERE customer_sk = -1)
                    INSERT INTO silver.erp_customer (
                        customer_sk, customer_id, customer_type, person_sk, store_sk, territory_sk,
                        row_hash, dwh_create_date, dwh_start_date, dwh_end_date, is_active, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown', -1, -1, -1,
                        NULL, @load_ts, @load_ts, DATEFROMPARTS(9999,12,31), 1, @load_ts, -1
                    );

                ---------------------------------------------------------
                -- Insert FULL (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_customer
                    (customer_sk, customer_id, person_id, store_id, territory_id,
                     customer_type, person_sk, store_sk, territory_sk,
                     row_hash, dwh_start_date, dwh_end_date, is_active,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_customer OVER (ORDER BY src.customer_id),
                    src.customer_id,
                    src.person_id,
                    src.store_id,
                    src.territory_id,
                    src.customer_type,
                    src.person_sk,
                    src.store_sk,
                    src.territory_sk,
                    src.row_hash,
                    @load_ts,
                    DATEFROMPARTS(9999,12,31),
                    1,
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

            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD2)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD2) started';

        ---------------------------------------------------------
        -- Bookmark: last time Silver wrote something
        -- (empty Silver -> 1900, so everything is read)
        ---------------------------------------------------------
        SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
        FROM silver.erp_customer;

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    c.CustomerID AS customer_id,
                    c.PersonID   AS person_id,
                    c.StoreID    AS store_id,
                    c.TerritoryID AS territory_id,
                    CASE
                        WHEN c.PersonID IS NULL AND c.StoreID IS NOT NULL THEN 'store'
                        WHEN c.PersonID IS NOT NULL AND c.StoreID IS NOT NULL THEN 'store contact'
                        ELSE 'individual customer'
                    END AS customer_type,
                    p.person_sk,
                    s.store_sk,
                    t.territory_sk,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            c.PersonID,
                            c.StoreID,
                            c.TerritoryID
                        )
                    ) AS row_hash,
                    c.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_customer c
                LEFT JOIN silver.erp_person p
                    ON c.PersonID = p.person_id
                LEFT JOIN silver.erp_store s
                    ON c.StoreID = s.store_id
                LEFT JOIN silver.erp_sales_territory t
                    ON c.TerritoryID = t.territory_id
                WHERE c.dwh_load_date > @last_silver_load
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'customer'
                          AND r.business_key = CAST(c.CustomerID AS NVARCHAR(200))
                    );

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

                BEGIN TRANSACTION;
                ---------------------------------------------------------
                -- Close previous versions (SCD2)
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.dwh_end_date = @load_ts,
                    tgt.is_active     = 0,
                    tgt.dwh_load_date = @load_ts,
                    tgt.dwh_batch_id  = @batch_id
                FROM silver.erp_customer tgt
                INNER JOIN #src_delta src
                    ON src.customer_id = tgt.customer_id
                WHERE tgt.is_active = 1
                  AND tgt.row_hash <> src.row_hash;

                SET @rows_closed = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Insert new versions (SCD2)
                ---------------------------------------------------------
                INSERT INTO silver.erp_customer
                    (customer_sk, customer_id, person_id, store_id, territory_id,
                     customer_type, person_sk, store_sk, territory_sk,
                     row_hash, dwh_start_date, dwh_end_date, is_active,
                     dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_customer OVER (ORDER BY src.customer_id),
                    src.customer_id,
                    src.person_id,
                    src.store_id,
                    src.territory_id,
                    src.customer_type,
                    src.person_sk,
                    src.store_sk,
                    src.territory_sk,
                    src.row_hash,
                    @load_ts,
                     DATEFROMPARTS(9999,12,31),
                    1,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN silver.erp_customer tgt
                    ON tgt.customer_id = src.customer_id
                      AND tgt.is_active = 1
                WHERE tgt.customer_id IS NULL;

                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            DROP TABLE #src_delta;

            SET @end_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load completed';
            PRINT '>> Rows closed:   ' + CAST(@rows_closed   AS NVARCHAR(20));
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
            THROW 51020, 'Unsupported load type for CUSTOMER.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: customer';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
