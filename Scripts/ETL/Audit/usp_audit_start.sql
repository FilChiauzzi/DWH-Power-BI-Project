/*
=========================================================
ETL Procedure – etl.usp_audit_start
=========================================================
Purpose:
    Opens one step in etl.load_audit with status 'running'
    and returns its audit_id.

SSIS Usage:
    Not called by SSIS: every load and quality check
    procedure calls it at its start (see usp_load_bronze).

Warning:
    The audit must live OUTSIDE the business transaction:
    the procedure refuses to run inside an open transaction.
*/


USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

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
