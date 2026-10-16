/*
=========================================================
Bronze Data Quality Check – Ship Method
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Ship Method domain.
    It checks for null business keys, duplicate keys, and
    incomplete descriptive attributes such as missing names
    or missing shipping cost components.

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

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_ship_method]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @rows_checked     INT = 0,
        @rows_rejected    INT = 0,
        @errors           INT = 0,
        @reject_rate      DECIMAL(9,4) = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - ship_method | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_ship_method;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'ship_method', 'Source empty', NULL,
                     'bronze.erp_ship_method contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: ShipMethodID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 1: ShipMethodID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'ship_method',
                   'ShipMethodID NULL',
                   NULL,
                   'Business key ShipMethodID is NULL',
                   'error'
            FROM bronze.erp_ship_method
            WHERE ShipMethodID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'ship_method',
                   NULL,
                   'NULL_BK',
                   'Business key ShipMethodID is NULL',
                   'error',
                   JSON_OBJECT(
                        'ShipMethodID': b.ShipMethodID,
                        'Name': b.Name,
                        'ShipBase': b.ShipBase,
                        'ShipRate': b.ShipRate,
                        'ModifiedDate': b.ModifiedDate
                        NULL ON NULL
                   )
            FROM bronze.erp_ship_method b
            WHERE b.ShipMethodID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate ShipMethodID -> error
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate ShipMethodID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'ship_method',
                   'Duplicate ShipMethodID',
                   CAST(ShipMethodID AS NVARCHAR(200)),
                   'Duplicate ShipMethodID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_ship_method
            GROUP BY ShipMethodID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'ship_method',
                   CAST(b.ShipMethodID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate ShipMethodID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT(
                        'ShipMethodID': b.ShipMethodID,
                        'Name': b.Name,
                        'ShipBase': b.ShipBase,
                        'ShipRate': b.ShipRate,
                        'ModifiedDate': b.ModifiedDate
                        NULL ON NULL
                   )
            FROM bronze.erp_ship_method b
            INNER JOIN (
                SELECT ShipMethodID, COUNT(*) AS cnt
                FROM bronze.erp_ship_method
                GROUP BY ShipMethodID
                HAVING COUNT(*) > 1
            ) d
                ON d.ShipMethodID = b.ShipMethodID;

            ---------------------------------------------------------
            -- RULE 3: Name NULL or empty -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 3: Name NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'ship_method',
                   'Name NULL or empty',
                   CAST(ShipMethodID AS NVARCHAR(200)),
                   'Name is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_ship_method
            WHERE Name IS NULL
               OR LTRIM(RTRIM(Name)) = '';

            ---------------------------------------------------------
            -- RULE 4: ShipBase NULL -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 4: ShipBase NULL (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'ship_method',
                   'ShipBase NULL',
                   CAST(ShipMethodID AS NVARCHAR(200)),
                   'ShipBase is NULL - default may be applied in Silver',
                   'warning'
            FROM bronze.erp_ship_method
            WHERE ShipBase IS NULL;

            ---------------------------------------------------------
            -- RULE 5: ShipRate NULL -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 5: ShipRate NULL (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'ship_method',
                   'ShipRate NULL',
                   CAST(ShipMethodID AS NVARCHAR(200)),
                   'ShipRate is NULL - default may be applied in Silver',
                   'warning'
            FROM bronze.erp_ship_method
            WHERE ShipRate IS NULL;

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'ship_method'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'ship_method';

        IF @rows_checked > 0
            SET @reject_rate = CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

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
        PRINT 'Bronze Data Quality - ship_method completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: ship_method';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
