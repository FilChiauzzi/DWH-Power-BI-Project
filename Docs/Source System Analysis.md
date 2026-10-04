# Source System Analysis - AdventureWorks2025 OLTP

---
## 1 Business Context & Ownership
### 1.1 Who own is the data?

AdventureWorks2025 OLTP is owned and maintained by the IT Operations Department.
Functional ownership is distributed across business domains:
* Sales & Marketing – customers, orders, territories
* Manufacturing – products, BOM, work orders
* Finance – pricing, costs, transactions
* Human Resources – employees, departments

### 1.2 Supported Business Processes
The source system supports the following core business processes:

* Order Management
* Customer Relationship Management
* Product & Variant Management
* Production Planning (MRP)
* Inventory & Warehouse Management
* Employee Management
* Pricing & Promotions

### 1.3 System & Data Documentation
Official Microsoft documentation is available and referenced for schema, metadata, and table definitions:

AdventureWorks installation & schema
https://learn.microsoft.com/sql/samples/adventureworks-install-configure (learn.microsoft.com in Bing)

AdventureWorks2022/2025 OLTP documentation
https://learn.microsoft.com/sql/samples/adventureworks2022 (learn.microsoft.com in Bing)

OLTP + DW conceptual diagrams
https://learn.microsoft.com/sql/samples/adventureworks2022-oltp-data-warehouse (learn.microsoft.com in Bing)

### 1.4 Data Model & Data Catalog
AdventureWorks is a relational SQL Server database organized into functional schemas:

* Sales
* Production
* Purchasing
* HumanResources
* Person
* Warehouse

A reduced ER model is used for DWH purposes, focusing only on the tables included in the analytical scope (Sales, Product, Customer, Territory, Store, etc.).

A **Data Catalog** is maintained for each table used in the DWH, including:

* Table description
* Primary keys
* Foreign keys
* Business‑relevant columns
* Audit columns (ModifiedDate, rowguid)
* Update frequency
* Notes for ingestion

## 2. Architecture & Technology Stack
### 2.1 Data Storage
AdventureWorks2025 OLTP is stored in Microsoft SQL Server, using:

* Relational tables
* Primary & foreign keys
* Clustered and non‑clustered indexes
* Stored procedures
* Scalar and table‑valued functions

### 2.2 Integration Capabilities
SQL Server supports multiple integration methods:

* Direct DB access (preferred for DWH ingestion)
* Linked Servers
* SSIS connectors
* File extracts (CSV, JSON, Parquet via BCP/SSIS)
* CDC (Change Data Capture)
* Temporal tables (if enabled)
* REST APIs (only if exposed externally, not native to AdventureWorks)

### 2.3 Technology Considerations
The ingestion strategy leverages:

* SQL Server authentication
* Dedicated ETL service accounts
* Network security (VPN, IP whitelisting, firewall rules)
* Least‑privilege access (SELECT only)

## 3. Extract & Load Strategy
### 3.1 Incremental vs Full Loads
Both approaches are supported:

* Full Loads: used for small lookup tables (ProductCategory, Employee, Territory).
* Incremental Loads: used for large transactional tables (SalesOrderHeader, SalesOrderDetail).
  * Incremental logic based on:
    * ModifiedDate
    * CDC (if enabled)
    * Hash comparison (if implemented in DWH)

### 3.2 Data Scope & Historical Requirements
AdventureWorks contains multi‑year historical data:

* Sales: 3 years
* Production: historical BOM and work orders
* HR: employee history
* Inventory: stock movements
* The DWH ingests full historical data for analytical completeness.

### 3.3 Expected Extract Size
Approximate record counts:
* SalesOrderHeader: ~31,000
* SalesOrderDetail: ~121,000
* Product: ~500
* Customer: ~20,000

These volumes are manageable for daily ingestion.

### 3.4 Data Volume Limitations
No structural limitations exist, but considerations include:

* Growth of Sales tables
* Heavy joins impacting OLTP performance
* Avoiding full daily extracts for large tables

### 3.5 Source System Performance Protection
To avoid impacting the OLTP system:

* Run extracts outside business hours
* Use selective column retrieval
* Filter using indexed columns (ModifiedDate)
* Avoid complex joins in source queries
* Use CDC to minimize load
* Apply NOLOCK only if approved by IT

### 3.6 Authentication & Authorization
Supported methods:

* SQL Authentication (ETL user)
* Windows Authentication (AD service account)
* VPN access
* IP whitelisting
* Firewall rules
* Role‑based access control
