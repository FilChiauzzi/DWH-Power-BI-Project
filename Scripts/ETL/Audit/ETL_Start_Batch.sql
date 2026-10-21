/*
=========================================================
ETL Procedure – Start Batch
=========================================================
Purpose:
    Creates a new run in etl.batch and returns its number in
    the output parameter @batch_id. The start time and the
    status 'running' are filled in by the column defaults;
    the login that started the run is stored in
    triggered_by and a short description in notes.

    Before creating the new run, it closes the previous runs
    that are still 'running' (the package was stopped or
    crashed and etl.usp_end_batch was never called): they
    become 'aborted', and their steps still 'running' in
    etl.load_audit become 'failed'.

SSIS Usage:
    Executed by the first task of the pipeline, Start Batch:
    EXEC etl.usp_start_batch ? OUTPUT. The parameter is
    bound to the SSIS variable batch_id, which is then
    passed to every load and quality check procedure of the
    run. @notes is optional.

Warning:
    Run ETL_Load_Audit.sql first: this procedure needs the
    column etl.batch.status and the table etl.load_audit.
    The pipeline must never run twice at the same time: a
    run still in progress would be marked 'aborted' by the
    new one.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_start_batch]
    @batch_id BIGINT OUTPUT,
    @notes    NVARCHAR(500) = N'SSIS pipeline run (Bronze, Silver, Gold)'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @aborted_runs INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Close the previous runs left open (crash, kill...)
            ---------------------------------------------------------
            UPDATE la
            SET la.status        = 'failed',
                la.ended_at      = SYSDATETIME(),
                la.error_message = 'Step interrupted: the run was never closed.'
            FROM etl.load_audit AS la
            INNER JOIN etl.batch AS b
                ON b.batch_id = la.batch_id
            WHERE b.[status]  = 'running'
              AND la.status   = 'running';

            UPDATE etl.batch
            SET [status] = 'aborted',
                ended_at = SYSDATETIME()
            WHERE [status] = 'running';

            SET @aborted_runs = @@ROWCOUNT;

            ---------------------------------------------------------
            -- New run: started_at and status come from the defaults
            ---------------------------------------------------------
            INSERT INTO etl.batch (triggered_by, notes)
            VALUES (SUSER_SNAME(), @notes);

            SET @batch_id = SCOPE_IDENTITY();

        COMMIT TRANSACTION;

        PRINT '====================================================';
        PRINT 'Batch started | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        IF @aborted_runs > 0
            PRINT 'Previous runs closed as aborted: ' + CAST(@aborted_runs AS NVARCHAR(10));
        PRINT '====================================================';
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

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
