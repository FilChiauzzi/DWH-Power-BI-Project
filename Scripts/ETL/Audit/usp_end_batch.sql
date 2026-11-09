/*
=========================================================
ETL Procedure – etl.usp_end_batch
=========================================================
Purpose:
    Closes the run in etl.batch (ended_at, status). Steps
    still 'running' are closed as failed (the run was
    interrupted), and the run is marked failed if at least
    one step failed.

SSIS Usage:
    Called by SSIS:
        - last task of the pipeline (on success):
              EXEC etl.usp_end_batch ?, 'succeeded'
        - OnError event handler at package level:
              EXEC etl.usp_end_batch ?, 'failed'
    The parameter is bound to the SSIS variable batch_id.

Warning:
    None.
*/


USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_end_batch]
    @batch_id BIGINT,
    @status   VARCHAR(20) = 'succeeded'     -- succeeded / failed
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @final_status  VARCHAR(20),
        @failed_steps  INT,
        @orphan_steps  INT;

    BEGIN TRY
        IF @status NOT IN ('succeeded', 'failed')
            THROW 51110, 'Invalid status for etl.usp_end_batch. Allowed values: succeeded, failed.', 1;

        IF NOT EXISTS (SELECT 1 FROM etl.batch WHERE batch_id = @batch_id)
            THROW 51111, 'Unknown batch_id.', 1;

        BEGIN TRANSACTION;

            -----------------------------------------------------
            -- Steps still 'running' were interrupted (package
            -- killed, timeout...): close them as failed
            -----------------------------------------------------
            UPDATE etl.load_audit
            SET status        = 'failed',
                ended_at      = SYSDATETIME(),
                error_message = 'Step interrupted: still running when the batch was closed.'
            WHERE batch_id = @batch_id
              AND status   = 'running';

            SET @orphan_steps = @@ROWCOUNT;

            SELECT @failed_steps = COUNT(*)
            FROM etl.load_audit
            WHERE batch_id = @batch_id
              AND status   = 'failed';

            -- The run succeeds only if SSIS says so AND no step failed
            SET @final_status =
                CASE WHEN @status = 'succeeded' AND @failed_steps = 0
                     THEN 'succeeded'
                     ELSE 'failed'
                END;

            UPDATE etl.batch
            SET ended_at = SYSDATETIME(),
                [status] = @final_status
            WHERE batch_id = @batch_id;

        COMMIT TRANSACTION;

        PRINT '====================================================';
        PRINT 'Batch closed | batch: ' + CAST(@batch_id AS NVARCHAR(20))
            + ' | status: '        + @final_status
            + ' | failed steps: '  + CAST(@failed_steps AS NVARCHAR(10))
            + ' | interrupted: '   + CAST(@orphan_steps AS NVARCHAR(10));
        PRINT '====================================================';
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR while closing the batch';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
