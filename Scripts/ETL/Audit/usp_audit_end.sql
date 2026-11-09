/*
=========================================================
ETL Procedure – etl.usp_audit_end
=========================================================
Purpose:
    Closes the step: status (succeeded, failed, skipped),
    end time, row counts and load type. When it is called
    from a CATCH block with status 'failed' and no error
    parameters, it reads the error itself with
    ERROR_NUMBER(), ERROR_MESSAGE()...

SSIS Usage:
    Not called by SSIS: every load and quality check
    procedure calls it at its end and in its CATCH block.

Warning:
    In the CATCH block, ROLLBACK first and only then call
    usp_audit_end: otherwise the audit row is rolled back
    together with the failed load and the error is lost.
    It never THROWs: a new error would hide the original.
*/


USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

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
