USE [DWH_AdventureWorks2025]
GO
/****** Oggetto: StoredProcedure [etl].[usp_load_silver_person]    Data dello script 17/10/2026 19:14:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_load_silver_person]
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
        SET @start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type
        FROM etl.config
        WHERE object_name = 'person'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for PERSON.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – PERSON (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
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
                    b.BusinessEntityID AS person_id,
                    CASE
                        WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                            THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                        ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                    END AS person_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                                    THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                                ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_person b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'person'
                      AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

            -- Safety: never empty Silver because the source is empty
            IF @rows_src = 0
                THROW 51030, 'Bronze person is empty: FULL load stopped to avoid emptying the Silver table.', 1;

            -----------------------------------------------------
            -- TRUNCATE + INSERT in one transaction
            -----------------------------------------------------
            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_person;

                ALTER SEQUENCE dwh.seq_silver_person RESTART WITH 1;

                -------------------------------------------------------
                -- INSERT UNKNOW MEMBER - OPTIMIZATION PERFORMANCE JOIN 
                -------------------------------------------------------
                IF NOT EXISTS (SELECT 1 FROM silver.erp_person WHERE person_sk = -1)
                    INSERT INTO silver.erp_person (
                        person_sk, person_id, person_name, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id
                    )
                    VALUES (
                        -1, -1, 'Unknown', NULL, SYSDATETIME(), SYSDATETIME(), -1 
                        );

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_person
                    (person_sk, person_id, person_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.person_name,
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

            RETURN;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD (SCD1)
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            PRINT '>> INCREMENTAL load (SCD1) started';

            -----------------------------------------------------
            -- Bookmark: last time Silver wrote something
            -- (empty Silver -> 1900, so everything is read)
            -----------------------------------------------------
            SELECT @last_silver_load = ISNULL(MAX(dwh_load_date), '1900-01-01')
            FROM silver.erp_person;

            PRINT '>> Last Silver load: ' + CONVERT(NVARCHAR(30), @last_silver_load, 121);

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS person_id,
                    CASE
                        WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                            THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                        ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                    END AS person_name,
                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            CASE
                                WHEN TRIM(b.MiddleName) = '' OR b.MiddleName IS NULL
                                    THEN TRIM(b.FirstName) + ' ' + TRIM(b.LastName)
                                ELSE TRIM(b.FirstName) + ' ' + TRIM(b.MiddleName) + ' ' + TRIM(b.LastName)
                            END
                        )
                    ) AS row_hash,
                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_person b
                WHERE b.dwh_load_date > @last_silver_load
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'person'
                          AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
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
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.person_name    = src.person_name,
                    tgt.row_hash       = src.row_hash,
                    tgt.dwh_load_date  = @load_ts,
                    tgt.dwh_batch_id   = @batch_id
                FROM silver.erp_person tgt
                INNER JOIN #src_delta src
                    ON src.person_id = tgt.person_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_person
                    (person_sk, person_id, person_name,
                     row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_person OVER (ORDER BY src.person_id),
                    src.person_id,
                    src.person_name,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                LEFT JOIN  silver.erp_person tgt
                    ON tgt.person_id = src.person_id
                WHERE tgt.person_id IS NULL;

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
            THROW 51020, 'Unsupported load type for PERSON.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: person';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
