/*
=========================================================
ETL Procedures – Load Audit and End Batch
=========================================================
Purpose:
    Three procedures that write the run log:

    etl.usp_audit_start
        Opens one step in etl.load_audit with status
        'running' and returns its audit_id.

    etl.usp_audit_end
        Closes the step: status (succeeded, failed, skipped),
        end time, row counts, load type and watermarks. When
        it is called from a CATCH block with status 'failed'
        and no error parameters, it reads the error itself
        with ERROR_NUMBER(), ERROR_MESSAGE()...

    etl.usp_end_batch
        Closes the run in etl.batch (ended_at, status). Steps
        still 'running' are closed as failed (the run was
        interrupted), and the run is marked failed if at
        least one step failed.

SSIS Usage:
    usp_audit_start and usp_audit_end are NOT called by
    SSIS: every load and quality check procedure calls them
    at its start and at its end (see usp_load_bronze).

    usp_end_batch IS called by SSIS:
        - last task of the pipeline (on success):
              EXEC etl.usp_end_batch ?, 'succeeded'
        - OnError event handler at package level:
              EXEC etl.usp_end_batch ?, 'failed'
    The parameter is bound to the SSIS variable batch_id.

Warning:
    The audit must live OUTSIDE the business transaction.
    usp_audit_start refuses to run inside an open
    transaction. In the CATCH block, ROLLBACK first and only
    then call usp_audit_end: otherwise the audit row is
    rolled back together with the failed load and the error
    is lost.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- etl.usp_audit_start
---------------------------------------------------------
CREATE OR ALTER PROCEDURE [etl].[usp_audit_start]
    @batch_id        BIGINT,
    @layer           VARCHAR(10),
    @object_name     VARCHAR(50),
    @step_name       VARCHAR(30),
    @procedure_name  NVARCHAR(256) = NULL,
    @audit_id        BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- The audit row must survive a rollback of the load
    IF @@TRANCOUNT > 0
        THROW 51100, 'etl.usp_audit_start must be called outside an open transaction.', 1;

    IF NOT EXISTS (SELECT 1 FROM etl.batch WHERE batch_id = @batch_id)
        THROW 51101, 'Unknown batch_id: create the run with etl.usp_start_batch first.', 1;

    INSERT INTO etl.load_audit
        (batch_id, layer, object_name, step_name, procedure_name, status)
    VALUES
        (@batch_id, @layer, @object_name, @step_name, @procedure_name, 'running');

    SET @audit_id = SCOPE_IDENTITY();
END;
GO

---------------------------------------------------------
-- etl.usp_audit_end
---------------------------------------------------------
CREATE OR ALTER PROCEDURE [etl].[usp_audit_end]
    @audit_id        BIGINT,
    @status          VARCHAR(20),               -- succeeded / failed / skipped
    @load_type       VARCHAR(20)    = NULL,
    @rows_read       INT            = NULL,
    @rows_inserted   INT            = NULL,
    @rows_updated    INT            = NULL,
    @rows_deleted    INT            = NULL,
    @rows_rejected   INT            = NULL,
    @watermark_from  DATETIME2(3)   = NULL,
    @watermark_to    DATETIME2(3)   = NULL,
    @error_number    INT            = NULL,
    @error_line      INT            = NULL,
    @error_procedure NVARCHAR(128)  = NULL,
    @error_message   NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- No THROW here on purpose: this procedure is also called
    -- inside CATCH blocks, and a new error would hide the
    -- original one. A wrong status is stored as 'failed'.
    IF @status NOT IN ('succeeded', 'failed', 'skipped')
    BEGIN
        SET @error_message = CONCAT('Invalid audit status: ', @status, '. ', @error_message);
        SET @status        = 'failed';
    END

    -- Called from a CATCH block: read the error by itself
    IF @status = 'failed' AND @error_message IS NULL AND ERROR_NUMBER() IS NOT NULL
    BEGIN
        SET @error_number    = ERROR_NUMBER();
        SET @error_line      = ERROR_LINE();
        SET @error_procedure = ERROR_PROCEDURE();
        SET @error_message   = ERROR_MESSAGE();
    END

    UPDATE etl.load_audit
    SET status          = @status,
        ended_at        = SYSDATETIME(),
        load_type       = COALESCE(@load_type, load_type),
        rows_read       = @rows_read,
        rows_inserted   = @rows_inserted,
        rows_updated    = @rows_updated,
        rows_deleted    = @rows_deleted,
        rows_rejected   = @rows_rejected,
        watermark_from  = @watermark_from,
        watermark_to    = @watermark_to,
        error_number    = @error_number,
        error_line      = @error_line,
        error_procedure = @error_procedure,
        error_message   = @error_message
    WHERE audit_id = @audit_id
      AND status   = 'running';             -- a closed step is never rewritten
END;
GO

---------------------------------------------------------
-- etl.usp_end_batch
---------------------------------------------------------
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
