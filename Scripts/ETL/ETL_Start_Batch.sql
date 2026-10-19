/*
=========================================================
ETL Procedure – Start Batch
=========================================================
Purpose:
    Creates a new run in etl.batch and returns its number in
    the output parameter @batch_id. The start time is filled
    in automatically and the login that started the run is
    stored in triggered_by.

SSIS Usage:
    Executed by the first task of the pipeline, Start Batch:
    EXEC etl.usp_start_batch ? OUTPUT. The parameter is
    bound to the SSIS variable batch_id, which is then
    passed to every load and quality check procedure of the
    run.

Warning:
    Each execution adds one row to etl.batch. Nothing closes
    the run yet: ended_at stays empty.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_start_batch]
    @batch_id BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        ---------------------------------------------------------
        -- New run: started_at comes from the column default
        ---------------------------------------------------------
        INSERT INTO etl.batch (triggered_by)
        VALUES (SUSER_SNAME());

        SET @batch_id = SCOPE_IDENTITY();

        PRINT '====================================================';
        PRINT 'Batch started | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';
    END TRY

    BEGIN CATCH
        PRINT '====================================================';
        PRINT 'ERROR while starting the batch';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
