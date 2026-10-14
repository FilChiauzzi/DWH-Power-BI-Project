/*
=========================================================
Bronze Data Quality – sales_territory
=========================================================
Purpose:
    Validates the raw SALES TERRITORY data before loading it
    into the Silver layer. Ensures structural integrity of the
    business key (TerritoryID) and mandatory attributes required
    for dimensional modeling.

Checks implemented:
    RULE 0 – Source empty (warning)
        If the Bronze table contains zero rows, a warning is logged.
        The pipeline continues, but Silver will load nothing.

    RULE 1 – TerritoryID NULL (error, blocking)
        TerritoryID is the business key and cannot be NULL.
        All rows with NULL TerritoryID are logged as errors and
        moved to etl.rejected.

    RULE 2 – Duplicate TerritoryID (error, blocking)
        TerritoryID must be unique.
        Any TerritoryID appearing more than once is logged as an
        error and all duplicate rows are moved to etl.rejected.

    RULE 3 – Name NULL or empty (warning)
        Name is required for reporting but not for key integrity.
        Rows with NULL or empty Name generate a warning only.
        Silver will substitute “Unknown”.

    RULE 4 – Group NULL or empty (error, blocking)
        Group is a mandatory attribute describing the geographic
        macro-area. Rows with NULL or empty Group are rejected.

    RULE 5 – CountryRegionCode NULL or empty (error, blocking)
        CountryRegionCode is a foreign key to CountryRegion.
        It must be present and non-empty. Rows violating this rule
        are rejected.

Not checked:
    ModifiedDate is NOT validated here.
    It is not required for business key integrity or Silver SCD1 logic.

Rejected rows:
    All blocking errors (NULL BK, duplicate BK, NULL Group,
    NULL CountryRegionCode) are inserted into etl.rejected with:
        - batch_id
        - layer = 'bronze'
        - object_name = 'sales_territory'
        - business_key
        - rule_code
        - rule_description
        - severity
        - raw_row (JSON_OBJECT)

Severity model:
    error     → row is rejected, pipeline may stop
    warning   → row is accepted but logged

Threshold guard:
    If the percentage of rejected rows exceeds @max_reject_rate
    (default 20%), the stored procedure throws an exception and
    stops the pipeline intentionally. This prevents loading
    corrupted or incomplete source files.

Summary:
    After all checks:
        - rows_checked
        - rows_rejected
        - errors
        - reject_rate (%)
    are printed for monitoring and auditing.

SSIS usage:
    EXEC etl.usp_check_bronze_quality_load_sales_territory ?
        param 0: batch_id
        param 1: max_reject_rate (optional)

Notes:
    This procedure is fully aligned with the Bronze DQ framework
    used for person, store, product, and country_region.
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_sales_territory]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @rows_checked     INT           = 0,
        @rows_rejected    INT           = 0,
        @errors           INT           = 0,
        @reject_rate      DECIMAL(9,4)  = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - sales_territory | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_sales_territory;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'sales_territory', 'Source empty', NULL,
                     'bronze.erp_sales_territory contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: TerritoryID NULL -> error
            ---------------------------------------------------------
            PRINT '-- Rule 1: TerritoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   'TerritoryID NULL',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'TerritoryID is NULL',
                   'error'
            FROM bronze.erp_sales_territory b
            WHERE b.TerritoryID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'NULL_BK',
                   'TerritoryID is NULL',
                   'error',
                   JSON_OBJECT(
                       'TerritoryID':       b.TerritoryID,
                       'Name':              b.Name,
                       'CountryRegionCode': b.CountryRegionCode,
                       'Group':             b.[Group],
                       'ModifiedDate':      b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_sales_territory b
            WHERE b.TerritoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate TerritoryID -> error
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate TerritoryID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   'Duplicate TerritoryID',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'Duplicate TerritoryID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_sales_territory b
            GROUP BY b.TerritoryID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate TerritoryID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT(
                       'TerritoryID':       b.TerritoryID,
                       'Name':              b.Name,
                       'CountryRegionCode': b.CountryRegionCode,
                       'Group':             b.[Group],
                       'ModifiedDate':      b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_sales_territory b
            INNER JOIN (
                SELECT TerritoryID, COUNT(*) AS cnt
                FROM bronze.erp_sales_territory
                GROUP BY TerritoryID
                HAVING COUNT(*) > 1
            ) d
                ON d.TerritoryID = b.TerritoryID;

            ---------------------------------------------------------
            -- RULE 3: Name NULL or empty -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 3: Name NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   'Name NULL or empty',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'Name is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_sales_territory b
            WHERE b.Name IS NULL
               OR TRIM(b.Name) = '';

            ---------------------------------------------------------
            -- RULE 4: Group NULL or empty -> error
            ---------------------------------------------------------
            PRINT '-- Rule 4: Group NULL or empty';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   'Group NULL or empty',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'Group is NULL or empty',
                   'error'
            FROM bronze.erp_sales_territory b
            WHERE b.[Group] IS NULL
               OR TRIM(b.[Group]) = '';

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'NULL_GROUP',
                   'Group is NULL or empty',
                   'error',
                   JSON_OBJECT(
                       'TerritoryID':       b.TerritoryID,
                       'Name':              b.Name,
                       'CountryRegionCode': b.CountryRegionCode,
                       'Group':             b.[Group],
                       'ModifiedDate':      b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_sales_territory b
            WHERE b.[Group] IS NULL
               OR TRIM(b.[Group]) = '';

            ---------------------------------------------------------
            -- RULE 5: CountryRegionCode NULL or empty -> error
            ---------------------------------------------------------
            PRINT '-- Rule 5: CountryRegionCode NULL or empty';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   'CountryRegionCode NULL or empty',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'CountryRegionCode is NULL or empty',
                   'error'
            FROM bronze.erp_sales_territory b
            WHERE b.CountryRegionCode IS NULL
               OR TRIM(b.CountryRegionCode) = '';

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'sales_territory',
                   CAST(b.TerritoryID AS NVARCHAR(200)),
                   'NULL_COUNTRY',
                   'CountryRegionCode is NULL or empty',
                   'error',
                   JSON_OBJECT(
                       'TerritoryID':       b.TerritoryID,
                       'Name':              b.Name,
                       'CountryRegionCode': b.CountryRegionCode,
                       'Group':             b.[Group],
                       'ModifiedDate':      b.ModifiedDate
                       NULL ON NULL
                   )
            FROM bronze.erp_sales_territory b
            WHERE b.CountryRegionCode IS NULL
               OR TRIM(b.CountryRegionCode) = '';

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_territory'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'sales_territory';

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
        PRINT 'Bronze Data Quality - sales_territory completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: sales_territory';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
