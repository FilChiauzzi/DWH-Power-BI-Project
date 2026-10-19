/*
=========================================================
Bronze Data Quality Check – Person Address
=========================================================
Purpose:
    Validates bronze.erp_person_address before the Silver
    load. AddressID NULL or duplicated and StateProvinceID
    NULL are blocking errors; City or PostalCode NULL is a
    warning (Silver uses 'Unknown').

    Blocking errors quarantine the row; non-blocking
    warnings are only logged.

SSIS Usage:
    Executed inside the metadata-driven SSIS pipeline,
    before the Silver load of the same object. SSIS passes
    @batch_id and @max_reject_rate (default 20). Every
    finding is logged in etl.data_quality_log; blocking
    errors are also quarantined in etl.rejected with a JSON
    copy of the raw row.

Warning:
    The procedure throws an error and stops the pipeline
    when the rejection rate exceeds @max_reject_rate.
    Previous log entries are kept: the log is append-only.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_person_address]
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
        PRINT 'Bronze Data Quality - person_address | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_person_address;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'person_address', 'Source empty', NULL,
                     'bronze.erp_person_address contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: AddressID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 1: AddressID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person_address',
                   'AddressID NULL',
                   NULL,
                   'AddressID is NULL',
                   'error'
            FROM bronze.erp_person_address
            WHERE AddressID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person_address',
                   NULL,
                   'NULL_BK',
                   'Business key AddressID is NULL',
                   'error',
                   JSON_OBJECT('AddressID': b.AddressID,
                               'AddressLine1': b.AddressLine1,
                               'City': b.City,
                               'StateProvinceID': b.StateProvinceID,
                               'PostalCode': b.PostalCode
                               NULL ON NULL)
            FROM bronze.erp_person_address AS b
            WHERE b.AddressID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate AddressID -> error
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate AddressID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person_address',
                   'Duplicate AddressID',
                   CAST(AddressID AS NVARCHAR(200)),
                   'Duplicate AddressID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_person_address
            GROUP BY AddressID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person_address',
                   CAST(b.AddressID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate AddressID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT('AddressID': b.AddressID,
                               'AddressLine1': b.AddressLine1,
                               'City': b.City,
                               'StateProvinceID': b.StateProvinceID,
                               'PostalCode': b.PostalCode
                               NULL ON NULL)
            FROM bronze.erp_person_address AS b
            INNER JOIN (
                SELECT AddressID, COUNT(*) AS cnt
                FROM bronze.erp_person_address
                GROUP BY AddressID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.AddressID = b.AddressID;

            ---------------------------------------------------------
            -- RULE 3: City NULL -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 3: City NULL (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person_address',
                   'City NULL',
                   CAST(AddressID AS NVARCHAR(200)),
                   'City is NULL - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_person_address
            WHERE City IS NULL;

            ---------------------------------------------------------
            -- RULE 4: StateProvinceID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 4: StateProvinceID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person_address',
                   'StateProvinceID NULL',
                   CAST(AddressID AS NVARCHAR(200)),
                   'StateProvinceID is NULL',
                   'error'
            FROM bronze.erp_person_address
            WHERE StateProvinceID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person_address',
                   CAST(b.AddressID AS NVARCHAR(200)),
                   'NULL_FK',
                   'Foreign key StateProvinceID is NULL',
                   'error',
                   JSON_OBJECT('AddressID': b.AddressID,
                               'AddressLine1': b.AddressLine1,
                               'City': b.City,
                               'StateProvinceID': b.StateProvinceID,
                               'PostalCode': b.PostalCode
                               NULL ON NULL)
            FROM bronze.erp_person_address AS b
            WHERE b.StateProvinceID IS NULL;

            ---------------------------------------------------------
            -- RULE 5: PostalCode NULL -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 5: PostalCode NULL (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person_address',
                   'PostalCode NULL',
                   CAST(AddressID AS NVARCHAR(200)),
                   'PostalCode is NULL - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_person_address
            WHERE PostalCode IS NULL;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person_address'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person_address';

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
        PRINT 'Bronze Data Quality - person_address completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: person_address';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
