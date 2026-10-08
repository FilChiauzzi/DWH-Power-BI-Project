/*
=========================================================
Silver Load – Person (SCD1)
=========================================================
Purpose:
    This stored procedure loads the Person domain into the
    Silver layer using SCD Type 1 logic. It supports both
    FULL and INCREMENTAL load types based on metadata stored
    in etl.config.

SSIS Usage:
    This procedure is executed inside the metadata-driven
    pipeline. SSIS reads the load_type from etl.config and
    triggers either a full reload or an incremental update.
    The procedure inserts cleaned Person records into
    silver.erp_person and maintains the surrogate key sequence.

Warning:
    Running this procedure in FULL mode will truncate the
    Silver table and reset the surrogate key sequence.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_silver_person]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_start_time DATETIME,
            @batch_end_time   DATETIME,
            @start_time       DATETIME,
            @end_time         DATETIME,
            @object_name      NVARCHAR(50),
            @load_type        NVARCHAR(20);

    SET @batch_start_time = GETDATE();
    SET @object_name = 'person';

    PRINT '====================================================';
    PRINT 'Retrieving configuration for object: ' + @object_name;
    PRINT '====================================================';

    ---------------------------------------------------------
    -- Read load type from config
    ---------------------------------------------------------
    SELECT @load_type = load_type
    FROM etl.config
    WHERE object_name = @object_name
      AND is_active = 1;

    PRINT '====================================================';
    PRINT 'Loading Silver Person';
    PRINT '====================================================';

    ---------------------------------------------------------
    -- FULL LOAD (SCD1)
    ---------------------------------------------------------
    IF @load_type = 'full'
    BEGIN
        SET @start_time = GETDATE();

        PRINT '>> FULL LOAD: Truncating silver.erp_person';
        TRUNCATE TABLE silver.erp_person;

        PRINT '>> FULL LOAD: Resetting sequence dwh.seq_silver_person';
        ALTER SEQUENCE dwh.seq_silver_person RESTART WITH 1;

        PRINT '>> FULL LOAD: Inserting all person records';

        WITH clean_person AS (
            SELECT 
                BusinessEntityID AS person_id,
                ROW_NUMBER() OVER (
                    PARTITION BY BusinessEntityID 
                    ORDER BY ModifiedDate DESC
                ) AS flag_unique,
                CASE
                    WHEN MiddleName IS NULL 
                        THEN TRIM(FirstName) + ' ' + TRIM(LastName)
                    ELSE TRIM(FirstName) + ' ' + TRIM(MiddleName) + ' ' + TRIM(LastName)
                END AS person_name
            FROM bronze.erp_person
        )
        INSERT INTO silver.erp_person (
            person_sk,
            person_id,
            person_name,
            dwh_create_date
        )
        SELECT 
            NEXT VALUE FOR dwh.seq_silver_person OVER (ORDER BY person_id),
            person_id,
            person_name,
            GETDATE()
        FROM clean_person
        WHERE flag_unique = 1;

        PRINT '>> FULL LOAD completed';
    END

    ---------------------------------------------------------
    -- INCREMENTAL LOAD (SCD1)
    ---------------------------------------------------------
    ELSE IF @load_type = 'incremental'
    BEGIN
        SET @start_time = GETDATE();
        PRINT '>> INCREMENTAL LOAD (SCD1)';

        ---------------------------------------------------------
        -- Source data (ERP)
        ---------------------------------------------------------
        WITH src AS (
            SELECT 
                BusinessEntityID AS person_id,
                CASE 
                    WHEN MiddleName IS NULL 
                        THEN TRIM(FirstName) + ' ' + TRIM(LastName)
                    ELSE TRIM(FirstName) + ' ' + TRIM(MiddleName) + ' ' + TRIM(LastName)
                END AS person_name,
                ModifiedDate
            FROM bronze.erp_person
        )

        ---------------------------------------------------------
        -- 1️ UPDATE existing records that changed
        ---------------------------------------------------------
        UPDATE tgt
        SET 
            tgt.person_name     = src.person_name,
            tgt.dwh_create_date = GETDATE()
        FROM src
        INNER JOIN silver.erp_person tgt
            ON src.person_id = tgt.person_id
        WHERE src.ModifiedDate > tgt.dwh_create_date;

        ---------------------------------------------------------
        -- 2️ INSERT new records
        ---------------------------------------------------------
        INSERT INTO silver.erp_person (
            person_sk,
            person_id,
            person_name,
            dwh_create_date
        )
        SELECT 
            NEXT VALUE FOR dwh.seq_silver_person,
            src.person_id,
            src.person_name,
            GETDATE()
        FROM src
        LEFT JOIN silver.erp_person tgt
            ON src.person_id = tgt.person_id
        WHERE tgt.person_id IS NULL;

        PRINT '>> INCREMENTAL LOAD completed';
    END

    ---------------------------------------------------------
    -- End of batch
    ---------------------------------------------------------
    SET @batch_end_time = GETDATE();

    PRINT '====================================================';
    PRINT 'Silver Person Load Completed in ' 
          + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
          + ' seconds';
    PRINT '====================================================';
END;
GO
