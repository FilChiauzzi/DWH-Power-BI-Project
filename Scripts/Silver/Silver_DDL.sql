/*
=========================================================
Silver DDL – Tables
=========================================================
Purpose:
    Creates the refined tables of the Silver layer and the
    dwh sequences used for surrogate keys. Column names are
    snake_case; business and surrogate keys are NOT NULL
    with a primary key; there are no foreign keys (the load
    procedures enforce them). The Unknown member (key -1) is
    created by the load procedures.

    Every table has the same technical columns: row_hash,
    dwh_create_date, dwh_load_date, dwh_batch_id. SCD Type 2
    tables add dwh_start_date, dwh_end_date and is_active.

SSIS Usage:
    Not executed by SSIS. Run once at deployment, after the
    ETL framework tables and before the Silver load
    procedures.

Warning:
    Running this script DROPS and recreates every Silver
    table. All data will be lost: Silver is rebuilt from
    Bronze by the load procedures.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Silver Category  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_category', 'U') IS NOT NULL
    DROP TABLE silver.erp_category;

CREATE TABLE silver.erp_category (
    category_sk       INT            NOT NULL,
    category_id       INT            NOT NULL,   -- business key
    category_name     NVARCHAR(50)   NULL,

    -- technical columns
    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_category_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_category_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_category
    ADD CONSTRAINT PK_silver_erp_category PRIMARY KEY (category_sk);

CREATE UNIQUE INDEX UX_silver_erp_category_id
    ON silver.erp_category (category_id);
GO

---------------------------------------------------------
-- Silver Subcategory  (SCD Type 1)
---------------------------------------------------------
-- category_sk is the resolved FK to silver.erp_category; it is
-- loaded after the category dimension (see load_order in
-- etl.config). -1 is the conventional "Unknown member" used
-- when the parent is missing: it must exist as a real row in
-- silver.erp_category (see the seed at the end of this script).

IF OBJECT_ID('silver.erp_subcategory', 'U') IS NOT NULL
    DROP TABLE silver.erp_subcategory;

CREATE TABLE silver.erp_subcategory (
    subcategory_sk    INT            NOT NULL,
    subcategory_id    INT            NOT NULL,   -- business key
    subcategory_name  NVARCHAR(50)   NULL,
    category_sk       INT            NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_category_sk DEFAULT -1,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_subcategory
    ADD CONSTRAINT PK_silver_erp_subcategory PRIMARY KEY (subcategory_sk);

CREATE UNIQUE INDEX UX_silver_erp_subcategory_id
    ON silver.erp_subcategory (subcategory_id);

CREATE INDEX IX_silver_erp_subcategory_category_sk
    ON silver.erp_subcategory (category_sk);
GO

---------------------------------------------------------
-- Silver Product  (SCD Type 2)
---------------------------------------------------------
IF OBJECT_ID('silver.erp_product', 'U') IS NOT NULL
    DROP TABLE silver.erp_product;

CREATE TABLE silver.erp_product (
    product_sk                 INT            NOT NULL,
    product_id                 INT            NOT NULL,   -- business key
    product_name               NVARCHAR(50)   NULL,
    product_number             NVARCHAR(25)   NULL,
    make_flag                  BIT            NULL,
    make_flag_desc             NVARCHAR(20)   NULL,       

    finished_goods_flag        BIT            NULL,
    finished_goods_flag_desc   NVARCHAR(20)   NULL,       

    color                      NVARCHAR(15)   NULL,
    safety_stock_level         SMALLINT       NULL,
    reorder_point              SMALLINT       NULL,
    standard_cost              DECIMAL(19,4)  NULL,
    list_price                 DECIMAL(19,4)  NULL,

    [size]                     NVARCHAR(5)    NULL,
    size_unit_measure_code     NCHAR(3)       NULL,
    size_unit_desc             NVARCHAR(20)   NULL,       

    weight_unit_measure_code   NCHAR(3)       NULL,
    weight_unit_desc           NVARCHAR(20)   NULL,       

    [weight]                   DECIMAL(8,2)   NULL,
    days_to_manufacture        INT            NULL,

    product_line               NCHAR(2)       NULL,
    product_line_desc          NVARCHAR(20)   NULL,       

    [class]                    NCHAR(2)       NULL,
    class_desc                 NVARCHAR(20)   NULL,       

    style                      NCHAR(2)       NULL,
    style_desc                 NVARCHAR(20)   NULL,       

    sell_start_date            DATE           NULL,
    sell_end_date              DATE           NULL,

    product_subcategory_sk     INT            NULL,

    -- technical columns
    row_hash                   VARBINARY(32)  NULL,
    dwh_create_date            DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_create DEFAULT SYSDATETIME(),
    dwh_start_date             DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_start  DEFAULT SYSDATETIME(),
    dwh_end_date               DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_end    DEFAULT DATEFROMPARTS(9999,12,31),
    is_active                  BIT            NOT NULL
        CONSTRAINT DF_silver_erp_product_active DEFAULT 1,
    dwh_load_date              DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_load   DEFAULT SYSDATETIME(),
    dwh_batch_id               BIGINT         NULL
);

ALTER TABLE silver.erp_product
    ADD CONSTRAINT PK_silver_erp_product PRIMARY KEY (product_sk);

-- SCD2: the business key is unique only among the CURRENT rows.
-- A filtered unique index allows any number of historical
-- versions and enforces EXACTLY ONE active row per product.
CREATE UNIQUE INDEX UX_silver_erp_product_active_id
    ON silver.erp_product (product_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_product_subcategory_sk
    ON silver.erp_product (product_subcategory_sk);
GO
----------------------------------------------------------
/*
	Silver Customer  (SCD Type 2)
*/
---------------------------------------------------------
IF OBJECT_ID('silver.erp_customer', 'U') IS NOT NULL
    DROP TABLE silver.erp_customer;

CREATE TABLE silver.erp_customer (
    customer_sk       INT            NOT NULL,
    customer_id       INT            NOT NULL,   -- business key
    person_id         INT            NULL,
    store_id          INT            NULL,
    territory_id      INT            NULL,
    customer_type     NVARCHAR(50)   NULL,
    person_sk         INT            NULL,
    store_sk          INT            NULL,
    territory_sk      INT            NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_create DEFAULT SYSDATETIME(),
    dwh_start_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_start  DEFAULT SYSDATETIME(),
    dwh_end_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_end    DEFAULT DATEFROMPARTS(9999,12,31),
    is_active         BIT            NOT NULL
        CONSTRAINT DF_silver_erp_customer_active DEFAULT 1,
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_customer
    ADD CONSTRAINT PK_silver_erp_customer PRIMARY KEY (customer_sk);

CREATE UNIQUE INDEX UX_silver_erp_customer_active_id
    ON silver.erp_customer (customer_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_customer_person_sk
    ON silver.erp_customer (person_sk);
GO

----------------------------------------------------------
/*
	Silver Store  (SCD Type 1)
*/
---------------------------------------------------------
IF OBJECT_ID('silver.erp_store', 'U') IS NOT NULL
    DROP TABLE silver.erp_store;

CREATE TABLE silver.erp_store (
    store_sk          INT            NOT NULL,
    store_id          INT            NOT NULL,   -- business key
    store_name        NVARCHAR(50)   NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_store_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_store_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_store
    ADD CONSTRAINT PK_silver_erp_store PRIMARY KEY (store_sk);

CREATE UNIQUE INDEX UX_silver_erp_store_id
    ON silver.erp_store (store_id);
GO

----------------------------------------------------------
/*
	Silver Sales Territory  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_sales_territory', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_territory;

CREATE TABLE silver.erp_sales_territory (
    territory_sk      INT            NOT NULL,
    territory_id      INT            NOT NULL,   -- business key
    territory_name    NVARCHAR(50)     NULL,
    country_code      NVARCHAR(3)    NULL,
    continent_name    NVARCHAR(50)   NULL,
    country_sk        INT            NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_territory_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_territory_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_sales_territory
    ADD CONSTRAINT PK_silver_erp_sales_territory PRIMARY KEY (territory_sk);

CREATE UNIQUE INDEX UX_silver_erp_sales_territory_id
    ON silver.erp_sales_territory (territory_id);
GO

----------------------------------------------------------
/*
	Silver Person  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_person', 'U') IS NOT NULL
    DROP TABLE silver.erp_person;

CREATE TABLE silver.erp_person (
    person_sk         INT            NOT NULL,
    person_id         INT            NOT NULL,   -- business key
    person_name       NVARCHAR(160)    NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_person_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_person_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_person
    ADD CONSTRAINT PK_silver_erp_person PRIMARY KEY (person_sk);

CREATE UNIQUE INDEX UX_silver_erp_person_id
    ON silver.erp_person (person_id);
GO

----------------------------------------------------------
/*
	Silver Country Region  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_country_region', 'U') IS NOT NULL
    DROP TABLE silver.erp_country_region;

CREATE TABLE silver.erp_country_region (
    country_sk        INT            NOT NULL,
    country_code      NVARCHAR(3)    NOT NULL,   -- business key
    country_name      NVARCHAR(50)   NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_country_region_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_country_region_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_country_region
    ADD CONSTRAINT PK_silver_erp_country_region PRIMARY KEY (country_sk);

CREATE UNIQUE INDEX UX_silver_erp_country_region_code
    ON silver.erp_country_region (country_code);
GO

----------------------------------------------------------
/*
	Silver Sales Person  (SCD Type 2)
*/
----------------------------------------------------------
-- Naming: the surrogate key is sales_person_sk, not person_sk
-- (that would be ambiguous with silver.erp_person.person_sk).
-- person_id stays as the reference to the person.

IF OBJECT_ID('silver.erp_sales_person', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_person;

CREATE TABLE silver.erp_sales_person (
    sales_person_sk   INT            NOT NULL,      -- surrogate key SCD2
    person_id         INT            NOT NULL,      -- business key (BusinessEntityID)
    territory_id      INT            NULL,
    sales_quota       DECIMAL(19,4)  NULL,
    bonus             DECIMAL(19,4)  NULL,
    commission_pct    DECIMAL(5,4)   NULL,
    sales_ytd         DECIMAL(19,4)  NULL,
    sales_last_year   DECIMAL(19,4)  NULL,

    territory_sk      INT            NULL,          -- FK to silver.erp_sales_territory
    employee_sk       INT            NULL,          -- FK to silver.erp_employee

    row_hash          VARBINARY(32)  NULL,

    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_create DEFAULT SYSDATETIME(),

    dwh_start_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_start  DEFAULT SYSDATETIME(),

    dwh_end_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_end    DEFAULT DATEFROMPARTS(9999,12,31),

    is_active         BIT            NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_active DEFAULT 1,

    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_load   DEFAULT SYSDATETIME(),

    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_sales_person
    ADD CONSTRAINT PK_silver_erp_sales_person PRIMARY KEY (sales_person_sk);

CREATE UNIQUE INDEX UX_silver_erp_sales_person_active_id
    ON silver.erp_sales_person (person_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_sales_person_employee_sk
    ON silver.erp_sales_person (employee_sk);

CREATE INDEX IX_silver_erp_sales_person_sales_territory_sk
    ON silver.erp_sales_person (territory_sk);
GO

----------------------------------------------------------
/*
    Silver Employee  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_employee', 'U') IS NOT NULL
    DROP TABLE silver.erp_employee;

CREATE TABLE silver.erp_employee (
    employee_sk        INT            NOT NULL,      -- surrogate key
    business_entity_id INT            NOT NULL,      -- business key (PK in HR.Employee)

    job_title          NVARCHAR(50)   NULL,
    birth_date         DATE           NULL,
    marital_status     NCHAR(1)       NULL,
    gender             NCHAR(1)       NULL,
    hire_date          DATE           NULL,
    salaried_flag      BIT            NULL,
    vacation_hours     SMALLINT       NULL,
    sick_leave_hours   SMALLINT       NULL,
    current_flag       BIT            NULL,

   person_sk INT NULL,
    row_hash           VARBINARY(32)  NULL,
    dwh_create_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_employee_create DEFAULT SYSDATETIME(),
    dwh_load_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_employee_load   DEFAULT SYSDATETIME(),
    dwh_batch_id       BIGINT         NULL
);

ALTER TABLE silver.erp_employee
    ADD CONSTRAINT PK_silver_erp_employee PRIMARY KEY (employee_sk);

CREATE UNIQUE INDEX UX_silver_erp_employee_business_entity_id
    ON silver.erp_employee (business_entity_id);

CREATE INDEX IX_silver_erp_employee_person_sk
    ON silver.erp_employee (person_sk);

GO
	
---------------------------------------------------------
-- Silver Person Address  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_person_address', 'U') IS NOT NULL
    DROP TABLE silver.erp_person_address;

CREATE TABLE silver.erp_person_address (
    address_sk          INT             NOT NULL,       -- surrogate key
    address_id          INT             NOT NULL,       -- business key
    address_line1       NVARCHAR(60)    NULL,
    address_line2       NVARCHAR(60)    NULL,
    city                NVARCHAR(30)    NULL,
    state_province_id   INT             NULL,
	state_province_sk   INT             NULL,
    postal_code         NVARCHAR(15)    NULL,

    -- coordinate estratte dal tipo geography del Bronze
    latitude            DECIMAL(9,6)    NULL,
    longitude           DECIMAL(9,6)    NULL,

    -- technical columns
    row_hash            VARBINARY(32)   NULL,
    dwh_create_date     DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_person_address_create DEFAULT SYSDATETIME(),
    dwh_load_date       DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_person_address_load   DEFAULT SYSDATETIME(),
    dwh_batch_id        BIGINT          NULL
);

ALTER TABLE silver.erp_person_address
    ADD CONSTRAINT PK_silver_person_address PRIMARY KEY (address_sk);

CREATE INDEX IX_silver_erp_person_addres_sk
    ON silver.erp_person_address (address_sk);

CREATE UNIQUE INDEX UX_silver_person_address_id
    ON silver.erp_person_address (address_id);

CREATE INDEX IX_silver_erp_person_state_province_sk
    ON silver.erp_person_address (state_province_sk);
GO
	
---------------------------------------------------------
-- Silver State Province  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_state_province', 'U') IS NOT NULL
    DROP TABLE silver.erp_state_province;

CREATE TABLE silver.erp_state_province (
    state_province_sk          INT             NOT NULL,       -- surrogate key
    state_province_id          INT             NOT NULL,       -- business key
    state_province_code        NCHAR(3) 	   NULL,
    country_region_code       NVARCHAR(3)      NULL,
    country_region_sk         INT              NULL,
    [name]                    NVARCHAR(50)     NULL,
    territory_id              INT              NULL,
    territory_sk              INT              NULL,
    -- technical columns
    row_hash            VARBINARY(32)   NULL,
    dwh_create_date     DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_state_province_create DEFAULT SYSDATETIME(),
    dwh_load_date       DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_state_province_load   DEFAULT SYSDATETIME(),
    dwh_batch_id        BIGINT          NULL
);

ALTER TABLE silver.erp_state_province
    ADD CONSTRAINT PK_silver_state_province_sk PRIMARY KEY (state_province_sk);

CREATE INDEX IX_silver_erp_state_province_sk
    ON silver.erp_state_province (state_province_sk);

CREATE UNIQUE INDEX UX_silver_state_province_id
    ON silver.erp_state_province (state_province_id);

---------------------------------------------------------
-- Silver Ship Method  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_ship_method', 'U') IS NOT NULL
    DROP TABLE silver.erp_ship_method;

CREATE TABLE silver.erp_ship_method (
    [ship_method_sk] [int] NOT NULL,
	[ship_method_id] [int]  NOT NULL,
	[ship_name] [nvarchar](50) NULL,
	[ship_base] DECIMAL(19,4) NULL,
	[ship_rate] DECIMAL(19,4) NULL,

    -- technical columns
    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_ship_method_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_ship_method_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_ship_method
    ADD CONSTRAINT PK_silver_erp_ship_method PRIMARY KEY (ship_method_sk);

CREATE UNIQUE INDEX UX_silver_erp_ship_method_id
    ON silver.erp_ship_method (ship_method_id);
GO
----------------------------------------------------------
/*
	Silver Sales Order Header
*/
----------------------------------------------------------
-- NOTE: modified_date stays DATETIME, NOT DATE. It is the
--       watermark column of the incremental load: casting it to
--       DATE would truncate the time and lose every change made
--       during the same day.
-- NOTE: the header carries no order-line metrics (quantity,
--       unit price, discount live in sales_order_detail, which
--       is the fact grain we will build in Gold).
-- NOTE: sales_order_id is NOT NULL because it is the PRIMARY
--       KEY: a PK cannot live on a nullable column (a NULL is
--       "unknown", and "unknown" is not an identity).

IF OBJECT_ID('silver.erp_sales_order_header', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_order_header;

CREATE TABLE silver.erp_sales_order_header (
    sales_order_id         INT            NOT NULL,   -- business key
    order_date             DATE           NULL,   -- business date: no time part
    due_date               DATE           NULL,
    ship_date              DATE           NULL,
    status                 TINYINT        NULL,
    online_order_flag      BIT            NULL,
    sales_order_number     NVARCHAR(25)   NULL,
    purchase_order_number  NVARCHAR(25)   NULL,
    account_number         NVARCHAR(15)   NULL,
    customer_id            INT            NULL,
    sales_person_id        INT            NULL,
    territory_id           INT            NULL,
    currency_rate_id       INT            NULL,
    sub_total              DECIMAL(19,4)  NULL,
    tax_amount             DECIMAL(19,4)  NULL,
    freight                DECIMAL(19,4)  NULL,
    total_due              DECIMAL(19,4)  NULL,
    modified_date          DATETIME       NULL,   -- watermark: never DATE
    customer_sk            INT            NULL,
    person_sk              INT            NULL,
    territory_sk           INT            NULL,

    -- technical columns
    row_hash               VARBINARY(32)  NULL,
    dwh_create_date        DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_header_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_header_load   DEFAULT SYSDATETIME(),
    dwh_batch_id           BIGINT         NULL
);

ALTER TABLE silver.erp_sales_order_header
    ADD CONSTRAINT PK_silver_erp_sales_order_header PRIMARY KEY (sales_order_id);

CREATE INDEX IX_silver_erp_sales_order_header_customer_sk
    ON silver.erp_sales_order_header (customer_sk);

CREATE INDEX IX_silver_erp_sales_order_header_modified_date
    ON silver.erp_sales_order_header (modified_date);
GO

----------------------------------------------------------
/*
    Silver Sales Order Detail
*/
----------------------------------------------------------
-- NOTE: modified_date stays DATETIME, NOT DATE. It is the
--       watermark column of the incremental load: casting it
--       to DATE would truncate the time and lose changes made
--       during the same day.
-- NOTE: This table represents the FACT grain at the order-line
--       level: quantity, unit price, discount and line total
--       live here, not in the header.
-- NOTE: SalesOrderID + SalesOrderDetailID form the natural PK.
--       Both columns must be NOT NULL to guarantee referential
--       integrity and avoid “unknown” business keys.

IF OBJECT_ID('silver.erp_sales_order_detail', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_order_detail;
GO

CREATE TABLE silver.erp_sales_order_detail (
    sales_order_id         INT             NOT NULL,   -- business key (FK to header)
    sales_order_detail_id  INT             NOT NULL,   -- business key (line number)

    carrier_tracking_number NVARCHAR(25)   NULL,
    order_qty              SMALLINT        NULL,
    product_id             INT             NULL,
    special_offer_id       INT             NULL,
    unit_price             DECIMAL(19,4)   NULL,
    unit_price_discount    DECIMAL(5,4)    NULL,
    line_total             DECIMAL(19,4)     NULL,
    modified_date          DATETIME        NULL,       -- watermark: never DATE

    -- surrogate keys resolved in Silver
    product_sk             INT             NULL,
    special_offer_sk       INT             NULL,

    -- technical columns
    row_hash               VARBINARY(32)   NULL,
    dwh_create_date        DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_detail_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_detail_load   DEFAULT SYSDATETIME(),
    dwh_batch_id           BIGINT          NULL
);

----------------------------------------------------------
-- Primary Key
----------------------------------------------------------
ALTER TABLE silver.erp_sales_order_detail
    ADD CONSTRAINT PK_silver_erp_sales_order_detail
        PRIMARY KEY (sales_order_id, sales_order_detail_id);

----------------------------------------------------------
-- Useful Indexes
----------------------------------------------------------
CREATE INDEX IX_silver_erp_sales_order_detail_product_sk
    ON silver.erp_sales_order_detail (product_sk);

CREATE INDEX IX_silver_erp_sales_order_detail_special_offer_sk
    ON silver.erp_sales_order_detail (special_offer_sk);

CREATE INDEX IX_silver_erp_sales_order_detail_modified_date
    ON silver.erp_sales_order_detail (modified_date);
GO

/*
=========================================================
Sequences (created only if missing)
=========================================================
Surrogate keys come from sequence objects. NEXT VALUE FOR
with OVER (ORDER BY ...) is supported from SQL Server 2012.
CREATE SEQUENCE is wrapped in EXEC(): like CREATE SCHEMA, it
must not be parsed inside a conditional block.
*/
IF SCHEMA_ID('dwh') IS NULL
    EXEC('CREATE SCHEMA dwh;');
GO

IF OBJECT_ID('dwh.seq_silver_category', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_category START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_subcategory', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_subcategory START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_product', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_product START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_customer', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_customer START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_store', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_store START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_sales_territory', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_sales_territory START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_person', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_person START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_country_region', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_country_region START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_sales_person', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_sales_person START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_employee', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_employee START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_person_address', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_person_address START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_state_province', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_state_province START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_ship_method', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_ship_method START WITH 1 INCREMENT BY 1;');
GO


