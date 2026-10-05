/*
=========================================================
Bronze Load Procedure – SSIS Metadata-Driven Pipeline
=========================================================
Purpose:
    This stored procedure loads Bronze-layer tables based on
    metadata stored in etl.config. It supports both FULL and
    INCREMENTAL loads using a watermark column.

SSIS Usage:
    - The Foreach Loop iterates over an Object variable
      "object_list" containing active object names.
    - Each iteration assigns the current object name to
      "current_object_name".
    - This procedure is executed via an Execute SQL Task:
          EXEC etl.usp_load_bronze ?
    - No result set is returned; SSIS only passes the parameter.

Warning:
    This procedure performs TRUNCATE TABLE during FULL loads.
    Ensure downstream dependencies are aware of this behavior.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_bronze]
    @object_name VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Retrieve configuration for object: ' + @object_name;
        PRINT '====================================================';

        DECLARE 
            @load_type VARCHAR(20),
            @source_table VARCHAR(200),
            @bronze_table VARCHAR(200),
            @watermark VARCHAR(100),
            @last_watermark_value DATETIME,
            @sql NVARCHAR(MAX),
            @new_watermark_value DATETIME;

        ---------------------------------------------------------
        -- Read metadata from config table
        ---------------------------------------------------------
        SELECT 
            @load_type = load_type,
            @source_table = source_table,
            @bronze_table = bronze_table,
            @watermark = watermark,
            @last_watermark_value = last_watermark_value
        FROM etl.config
        WHERE object_name = @object_name
          AND is_active = 1;

        SET @start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Loading Bronze for object: ' + @object_name;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            SET @sql = '
                TRUNCATE TABLE ' + @bronze_table + ';
                INSERT INTO ' + @bronze_table + '
                SELECT *
                FROM ' + @source_table + ';
            ';
            EXEC(@sql);

            -- Compute new watermark
            SET @sql = '
                SELECT @new_water_mark = MAX(' + @watermark + ')
                FROM ' + @source_table + ';
            ';
            EXEC sp_executesql @sql,
                N'@new_water_mark DATETIME OUTPUT',
                @new_water_mark = @new_watermark_value OUTPUT;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE IF @load_type = 'incremental'
        BEGIN
            SET @sql = '
                INSERT INTO ' + @bronze_table + '
                SELECT *
                FROM ' + @source_table + '
                WHERE ' + @watermark + ' > ''' +
                CONVERT(VARCHAR(30), @last_watermark_value, 29) + ''';';
            EXEC(@sql);

            -- Compute new watermark from newly inserted rows
            SET @sql = '
                SELECT @new_water_mark = MAX(' + @watermark + ')
                FROM ' + @bronze_table + '
                WHERE ' + @watermark + ' > ''' +
                CONVERT(VARCHAR(30), @last_watermark_value, 29) + ''';';
            EXEC sp_executesql @sql,
                N'@new_water_mark DATETIME OUTPUT',
                @new_water_mark = @new_watermark_value OUTPUT;
        END

        ---------------------------------------------------------
        -- Update watermark in config table
        ---------------------------------------------------------
        UPDATE etl.config
        SET last_watermark_value = @new_watermark_value
        WHERE object_name = @object_name;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) +
              ' ms';

        SET @batch_end_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Bronze load completed for: ' + @object_name +
              ' | Total Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
              ' ms';
        PRINT '====================================================';
    END TRY

    BEGIN CATCH
        PRINT '====================================================';
        PRINT 'ERROR during Bronze load for: ' + @object_name;
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '====================================================';
    END CATCH
END;
GO
