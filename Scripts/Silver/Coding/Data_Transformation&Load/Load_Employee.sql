/*
=========================================================
Silver Load – Employee (SCD1)
=========================================================
Purpose:
    Loads the Employee dimension into the Silver layer.
    Applies SCD Type 1 logic: only the latest version of
    each employee is kept. The procedure performs FULL or
    INCREMENTAL loads based on the configuration stored in
    etl.config. It also resolves person_sk from the Silver
    Person dimension.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Silver Load procedure for each
    domain. The procedure inserts or updates rows in the
    Silver layer and updates the watermark for incremental
    loads.

Warning:
    Running this procedure will delete previous Silver data
    for Employee when load_type = 'full'.
*/

USE [DWH_AdventureWorks2025]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_employee]
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
        @last_wm          DATETIME,
        @new_wm           DATETIME,
        @rows_src         INT = 0,
        @rows_updated     INT = 0,
        @rows_inserted    INT = 0;

    BEGIN TRY
        ---------------------------------------------------------
        -- Read configuration
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @last_wm   = last_watermark_value
        FROM etl.config
        WHERE object_name = 'employee'
          AND is_active = 1;

        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for EMPLOYEE.', 1;

        PRINT '====================================================';
        PRINT 'Silver Load – EMPLOYEE (SCD1) | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT 'Load type: ' + @load_type;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @start_time = SYSDATETIME();

            BEGIN TRANSACTION;

                TRUNCATE TABLE silver.erp_employee;

                ALTER SEQUENCE dwh.seq_silver_employee RESTART WITH 1;

                ---------------------------------------------------------
                -- Build FULL source set (#src_full)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS business_entity_id,
                    b.JobTitle,
                    b.BirthDate,
                    b.MaritalStatus,
                    b.Gender,
                    b.HireDate,
                    b.SalariedFlag,
                    b.VacationHours,
                    b.SickLeaveHours,
                    b.CurrentFlag,

                    p.person_sk,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            b.JobTitle,
                            b.BirthDate,
                            b.MaritalStatus,
                            b.Gender,
                            b.HireDate,
                            b.SalariedFlag,
                            b.VacationHours,
                            b.SickLeaveHours,
                            b.CurrentFlag
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_full
                FROM bronze.erp_employee b
                LEFT JOIN silver.erp_person p
                    ON p.person_id = b.BusinessEntityID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM etl.rejected r
                    WHERE r.batch_id    = @batch_id
                      AND r.layer        = 'bronze'
                      AND r.object_name  = 'employee'
                      AND r.business_key = CAST(b.BusinessEntityID AS NVARCHAR(200))
                );

                SELECT @rows_src = COUNT(*) FROM #src_full;

                PRINT '>> Source rows (FULL, after quarantine): ' + CAST(@rows_src AS NVARCHAR(20));

                ---------------------------------------------------------
                -- Insert FULL (SCD1)
                ---------------------------------------------------------
                INSERT INTO silver.erp_employee
                    (employee_sk, business_entity_id, job_title, birth_date,
                     marital_status, gender, hire_date, salaried_flag,
                     vacation_hours, sick_leave_hours, current_flag,
                     person_sk, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_employee OVER (ORDER BY src.business_entity_id),
                    src.business_entity_id,
                    src.JobTitle,
                    src.BirthDate,
                    src.MaritalStatus,
                    src.Gender,
                    src.HireDate,
                    src.SalariedFlag,
                    src.VacationHours,
                    src.SickLeaveHours,
                    src.CurrentFlag,
                    src.person_sk,
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
            SET @start_time = SYSDATETIME();

            PRINT '>> INCREMENTAL load (SCD1) started';

            BEGIN TRANSACTION;

                ---------------------------------------------------------
                -- Build DELTA source set (#src_delta)
                ---------------------------------------------------------
                SELECT
                    b.BusinessEntityID AS business_entity_id,
                    b.JobTitle,
                    b.BirthDate,
                    b.MaritalStatus,
                    b.Gender,
                    b.HireDate,
                    b.SalariedFlag,
                    b.VacationHours,
                    b.SickLeaveHours,
                    b.CurrentFlag,

                    p.person_sk,

                    HASHBYTES('SHA2_256',
                        CONCAT_WS('|',
                            b.BusinessEntityID,
                            b.JobTitle,
                            b.BirthDate,
                            b.MaritalStatus,
                            b.Gender,
                            b.HireDate,
                            b.SalariedFlag,
                            b.VacationHours,
                            b.SickLeaveHours,
                            b.CurrentFlag
                        )
                    ) AS row_hash,

                    b.ModifiedDate
                INTO #src_delta
                FROM bronze.erp_employee b
                LEFT JOIN silver.erp_person p
                    ON p.person_id = b.BusinessEntityID
                WHERE b.ModifiedDate > @last_wm
                  AND NOT EXISTS (
                        SELECT 1
                        FROM etl.rejected r
                        WHERE r.batch_id    = @batch_id
                          AND r.layer        = 'bronze'
                          AND r.object_name  = 'employee'
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

                ---------------------------------------------------------
                -- UPDATE changed rows
                ---------------------------------------------------------
                UPDATE tgt
                SET tgt.job_title        = src.JobTitle,
                    tgt.birth_date       = src.BirthDate,
                    tgt.marital_status   = src.MaritalStatus,
                    tgt.gender           = src.Gender,
                    tgt.hire_date        = src.HireDate,
                    tgt.salaried_flag    = src.SalariedFlag,
                    tgt.vacation_hours   = src.VacationHours,
                    tgt.sick_leave_hours = src.SickLeaveHours,
                    tgt.current_flag     = src.CurrentFlag,
                    tgt.person_sk        = src.person_sk,
                    tgt.row_hash         = src.row_hash,
                    tgt.dwh_load_date    = @load_ts,
                    tgt.dwh_batch_id     = @batch_id
                FROM silver.erp_employee tgt
                INNER JOIN #src_delta src
                    ON src.business_entity_id = tgt.business_entity_id
                WHERE tgt.row_hash <> src.row_hash;

                SET @rows_updated = @@ROWCOUNT;

                ---------------------------------------------------------
                -- INSERT new business keys
                ---------------------------------------------------------
                INSERT INTO silver.erp_employee
                    (employee_sk, business_entity_id, job_title, birth_date,
                     marital_status, gender, hire_date, salaried_flag,
                     vacation_hours, sick_leave_hours, current_flag,
                     person_sk, row_hash, dwh_create_date, dwh_load_date, dwh_batch_id)
                SELECT
                    NEXT VALUE FOR dwh.seq_silver_employee OVER (ORDER BY src.business_entity_id),
                    src.business_entity_id,
                    src.JobTitle,
                    src.BirthDate,
                    src.MaritalStatus,
                    src.Gender,
                    src.HireDate,
                    src.SalariedFlag,
                    src.VacationHours,
                    src.SickLeaveHours,
                    src.CurrentFlag,
                    src.person_sk,
                    src.row_hash,
                    @load_ts,
                    @load_ts,
                    @batch_id
                FROM #src_delta src
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM silver.erp_employee tgt
                    WHERE tgt.business_entity_id = src.business_entity_id
                );

                SET @rows_inserted = @@ROWCOUNT;

                ---------------------------------------------------------
                -- Update watermark
                ---------------------------------------------------------
                SELECT @new_wm = MAX(ModifiedDate) FROM #src_delta;

                UPDATE etl.config
                SET last_watermark_value = @new_wm
                WHERE object_name = 'employee'
                  AND is_active = 1;

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
            THROW 51020, 'Unsupported load type for EMPLOYEE.', 1;
        END

    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF OBJECT_ID('tempdb..#src_full')  IS NOT NULL DROP TABLE #src_full;
        IF OBJECT_ID('tempdb..#src_delta') IS NOT NULL DROP TABLE #src_delta;

        PRINT '====================================================';
        PRINT 'ERROR during Silver load for: employee';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
