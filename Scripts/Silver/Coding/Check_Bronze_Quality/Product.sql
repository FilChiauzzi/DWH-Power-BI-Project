/*
=========================================================
Bronze Data Quality Check – Product
=========================================================
Purpose:
    This stored procedure performs data quality validation
    on the Bronze-layer table for the Product domain.
    It checks for null keys, duplicate keys, missing foreign
    keys, invalid descriptive fields, and negative numeric
    values.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    SSIS pipeline. SSIS loops through all active objects and
    calls the corresponding Bronze Data Quality procedure for
    each imported source table. Each procedure logs issues
    into etl.data_quality_log and returns the number of
    detected errors.

Warning:
    Running this procedure will delete previous quality logs
    for the Product object in the Bronze layer.
*/

USE [DWH_AdventureWorks2025]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE [etl].[usp_check_bronze_quality_load_product]
    @batch_id BIGINT,
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
        PRINT 'Bronze Data Quality – product | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        ---------------------------------------------------------
        -- Count source rows
        ---------------------------------------------------------
        SELECT @rows_checked = COUNT(*)
        FROM bronze.erp_product;

        IF @rows_checked = 0
        BEGIN
            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            VALUES
                (@batch_id, 'bronze', 'product', 'Source empty', NULL,
                 'bronze.erp_product contains no rows', 'warning');
        END

        ---------------------------------------------------------
        -- RULE 1: ProductID NULL (critical)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'ProductID NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductID is NULL',
               'critical'
        FROM bronze.erp_product AS p
        WHERE p.ProductID IS NULL;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'product',
               CAST(p.ProductID AS NVARCHAR(200)),
               'NULL_BK',
               'Business key ProductID is NULL',
               'critical',
               JSON_OBJECT('ProductID': p.ProductID,
                           'Name': p.Name,
                           'ProductNumber': p.ProductNumber,
                           'ProductSubcategoryID': p.ProductSubcategoryID
                           NULL ON NULL)
        FROM bronze.erp_product AS p
        WHERE p.ProductID IS NULL;

        ---------------------------------------------------------
        -- RULE 2: Duplicate ProductID (critical)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Duplicate ProductID',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Duplicate ProductID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
               'critical'
        FROM bronze.erp_product AS p
        GROUP BY p.ProductID
        HAVING COUNT(*) > 1;

        INSERT INTO etl.rejected
            (batch_id, layer, object_name, business_key, rule_code,
             rule_description, severity, raw_row)
        SELECT @batch_id, 'bronze', 'product',
               CAST(p.ProductID AS NVARCHAR(200)),
               'DUP_BK',
               'Duplicate ProductID detected',
               'critical',
               JSON_OBJECT('ProductID': p.ProductID,
                           'Name': p.Name,
                           'ProductNumber': p.ProductNumber,
                           'ProductSubcategoryID': p.ProductSubcategoryID
                           NULL ON NULL)
        FROM bronze.erp_product AS p
        INNER JOIN (
            SELECT ProductID
            FROM bronze.erp_product
            GROUP BY ProductID
            HAVING COUNT(*) > 1
        ) AS d
            ON d.ProductID = p.ProductID;

        ---------------------------------------------------------
        -- RULE 3: Name NULL or empty (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Name NULL or empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'Name is NULL or empty',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.Name IS NULL
           OR LTRIM(RTRIM(p.Name)) = '';

        ---------------------------------------------------------
        -- RULE 4: ProductNumber NULL or empty (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'ProductNumber NULL or empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductNumber is NULL or empty',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.ProductNumber IS NULL
           OR LTRIM(RTRIM(p.ProductNumber)) = '';

        ---------------------------------------------------------
        -- RULE 5: SellStartDate NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'SellStartDate NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'SellStartDate is NULL',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.SellStartDate IS NULL;

        ---------------------------------------------------------
        -- RULE 6: ProductSubcategoryID NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'SubCategoryID NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'ProductSubcategoryID is NULL',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.ProductSubcategoryID IS NULL;

        ---------------------------------------------------------
        -- RULE 7: Numeric fields NULL or negative (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Negative or NULL numeric value',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more numeric fields are negative or NULL',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.StandardCost < 0 OR p.StandardCost IS NULL
           OR p.ListPrice < 0 OR p.ListPrice IS NULL
           OR p.SafetyStockLevel < 0 OR p.SafetyStockLevel IS NULL
           OR p.ReorderPoint < 0 OR p.ReorderPoint IS NULL
           OR p.Weight < 0 OR p.Weight IS NULL
           OR p.DaysToManufacture < 0 OR p.DaysToManufacture IS NULL;

        ---------------------------------------------------------
        -- RULE 8: UnitMeasureCode NULL (error)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'UnitMeasureCode NULL',
               CAST(p.ProductID AS NVARCHAR(200)),
               'SizeUnitMeasureCode or WeightUnitMeasureCode is NULL',
               'error'
        FROM bronze.erp_product AS p
        WHERE p.SizeUnitMeasureCode IS NULL
           OR p.WeightUnitMeasureCode IS NULL;

        ---------------------------------------------------------
        -- RULE 9: Descriptive fields empty (warning)
        ---------------------------------------------------------
        INSERT INTO etl.data_quality_log
            (batch_id, layer, object_name, check_name, record_id, description, severity)
        SELECT @batch_id, 'bronze', 'product',
               'Descriptive field empty',
               CAST(p.ProductID AS NVARCHAR(200)),
               'One or more descriptive fields are empty',
               'warning'
        FROM bronze.erp_product AS p
        WHERE LTRIM(RTRIM(p.Color)) = ''
           OR LTRIM(RTRIM(p.Size)) = ''
           OR LTRIM(RTRIM(p.ProductLine)) = ''
           OR LTRIM(RTRIM(p.Class)) = ''
           OR LTRIM(RTRIM(p.Style)) = '';

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'product'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'product';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';

        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality – product completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: product';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;

