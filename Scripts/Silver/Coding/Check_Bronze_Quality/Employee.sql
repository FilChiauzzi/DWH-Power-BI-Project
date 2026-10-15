USE [DWH_AdventureWorks2025]
GO
/****** Oggetto: StoredProcedure [etl].[usp_check_bronze_quality_load_employee]    Data dello script 15/10/2026 11:29:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [etl].[usp_check_bronze_quality_load_employee]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start_time DATETIME2(3) = SYSDATETIME(),
            @rows_checked     INT           = 0,
            @rows_rejected    INT           = 0,
            @errors           INT           = 0,
            @reject_rate      DECIMAL(9,4)  = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - employee | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

        ---------------------------------------------------------
        -- Count source rows
        ---------------------------------------------------------
        SELECT @rows_checked = COUNT(*)
        FROM bronze.erp_employee;

        ---------------------------------------------------------
        -- RULE 0: source empty -> warning
        ---------------------------------------------------------
        IF @rows_checked = 0
        BEGIN
            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            VALUES
                (@batch_id, 'bronze', 'employee', 'Source empty', NULL,
                 'bronze.erp_employee contains no rows', 'warning');
        END

        ---------------------------------------------------------
        -- RULE 1: BusinessEntityID NULL -> error
        ---------------------------------------------------------
        PRINT '-- Rule 1: BusinessEntityID NULL';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'BusinessEntityID NULL',
               NULL,
               'BusinessEntityID is NULL',
               'error'
        FROM bronze.erp_employee
        WHERE BusinessEntityID IS NULL;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'employee',
               NULL,
               'NULL_BK',
               'BusinessEntityID is NULL',
               'error',
               JSON_OBJECT(
                    'BusinessEntityID': BusinessEntityID,
                    'JobTitle': JobTitle,
                    'Gender': Gender,
                    'HireDate': HireDate,
                    'CurrentFlag': CurrentFlag,
                    'ModifiedDate': ModifiedDate
               NULL ON NULL)
        FROM bronze.erp_employee
        WHERE BusinessEntityID IS NULL;

        ---------------------------------------------------------
        -- RULE 2: Duplicate BusinessEntityID -> error
        ---------------------------------------------------------
        PRINT '-- Rule 2: Duplicate BusinessEntityID';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'Duplicate BusinessEntityID',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Duplicate BusinessEntityID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
               'error'
        FROM bronze.erp_employee AS b
        GROUP BY b.BusinessEntityID
        HAVING COUNT(*) > 1;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'employee',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'DUP_BK',
               'Duplicate BusinessEntityID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
               'error',
               JSON_OBJECT(
                    'BusinessEntityID': b.BusinessEntityID,
                    'JobTitle': b.JobTitle,
                    'Gender': b.Gender,
                    'HireDate': b.HireDate,
                    'CurrentFlag': b.CurrentFlag,
                    'ModifiedDate': b.ModifiedDate
               NULL ON NULL)
        FROM bronze.erp_employee AS b
        INNER JOIN (
            SELECT BusinessEntityID, COUNT(*) AS cnt
            FROM bronze.erp_employee
            GROUP BY BusinessEntityID
            HAVING COUNT(*) > 1
        ) AS d
            ON d.BusinessEntityID = b.BusinessEntityID;

        ---------------------------------------------------------
        -- RULE 3: JobTitle NULL -> warning
        ---------------------------------------------------------
        PRINT '-- Rule 3: JobTitle NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'JobTitle NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'JobTitle is NULL',
               'warning'
        FROM bronze.erp_employee AS b
        WHERE b.JobTitle IS NULL
           OR LTRIM(RTRIM(b.JobTitle)) = '';

        ---------------------------------------------------------
        -- RULE 4: Gender NULL -> warning
        ---------------------------------------------------------
        PRINT '-- Rule 4: Gender NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'Gender NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'Gender is NULL',
               'warning'
        FROM bronze.erp_employee AS b
        WHERE b.Gender IS NULL;

        ---------------------------------------------------------
        -- RULE 5: HireDate NULL -> warning
        ---------------------------------------------------------
        PRINT '-- Rule 5: HireDate NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'HireDate NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'HireDate is NULL',
               'warning'
        FROM bronze.erp_employee AS b
        WHERE b.HireDate IS NULL;

        ---------------------------------------------------------
        -- RULE 6: CurrentFlag NULL -> warning
        ---------------------------------------------------------
        PRINT '-- Rule 6: CurrentFlag NULL (warning)';

        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'employee',
               'CurrentFlag NULL',
               CAST(b.BusinessEntityID AS NVARCHAR(200)),
               'CurrentFlag is NULL',
               'warning'
        FROM bronze.erp_employee AS b
        WHERE b.CurrentFlag IS NULL;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'employee'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'employee';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, SYSDATETIME()) AS NVARCHAR(20)) + ' ms';

        ---------------------------------------------------------
        -- Threshold guard
        ---------------------------------------------------------
        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - employee completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: employee';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
