# Source System Analysis - AdventureWorks2025 OLTP

---
## 1 Business Context & Ownership
### 1.1 Who own is the data?

AdventureWorks2025 OLTP is owned and maintained by the **IT Operations Department**.
Functional ownership is distributed across business domains:
* **Sales & Marketing** – customers, orders, territories
* **Manufacturing** – products, BOM, work orders
* **Finance** – pricing, costs, transactions
* **Human Resources** – employees, departments

### 1.2 Supported Business Processes
The source system supports the following core business processes:

* **Order** Management
* **Customer** Relationship Management
* **Product** & Variant Management
* **Production** Planning (MRP)
* **Inventory** & **Warehouse** Management
* **Employee** Management
* **Pricing** & **Promotions**

### 1.3 System & Data Documentation
This section summarizes all **technical** and **functional** documentation provided directly by the **AdventureWorks IT** and **Business departments**.
The documentation describes the source system **architecture, database structure, business processes, data definitions, and integration mechanisms** used by the organization.

#### 1.3.1 Technical Documentation Provided by IT Operations
The IT Operations team supplied a complete set of internal technical documents describing how the AdventureWorks2025 OLTP system is **built** and **maintained**.

##### 1.3.1.1 System Architecture Documentation
The following internal documents were provided:

* High‑level architecture **diagrams** of the OLTP system
* Server and infrastructure topology (**SQL Server instances, VM layout, storage configuration**)
* Network access requirements (**VPN**, firewall rules, IP whitelisting)
* **Authentication** and **authorization** model
* Backup, recovery, and maintenance procedures
* Performance guidelines and operational constraints

##### 1.3.1.2 Database Schema & Metadata
IT shared detailed documentation describing the database structure:

* Full **ER diagrams**of the OLTP database
* Table‑level **schema documentation**
* Column‑level **metadata** (datatype, nullability, constraints)
* Primary and foreign **key** relationships
* Indexing strategy and performance notes
* **Stored procedure**

##### 1.3.1.3 Integration Interfaces
The integration team provided documentation covering:

* Existing data extraction processes
* File exchange formats (CSV, XML, JSON)
* **API specifications** (if applicable)
* **Incremental load logic** (ModifiedDate, CDC, triggers)
* Security protocols (service accounts, tokens, SSH keys)
* Scheduling and operational SLAs

#### 1.3.2 Functional Documentation Provided by Business Departments
Business stakeholders supplied **functional documentation** describing how data is used across the organization.

##### 1.3.2.1 Business Glossary
A complete **glossary** defining key business concepts, including:

* Customer, Store, Territory
* Sales Order, Quote, Invoice
* Product, Variant, BOM
* Work Order, Production Cycle
* Employee, Department, Role
* Financial metrics (Revenue, Margin, Cost of Goods Sold)

##### 1.3.2.2 Business Process Documentation
Detailed descriptions of the processes supported by the OLTP system:

* Order‑to‑Cash
* Procure‑to‑Pay
* Production Planning (MRP)
* Inventory Management
* Customer Management
* Employee Lifecycle Management
* Pricing & Promotions

##### 1.3.2.3 Data Ownership & Stewardship
Each department identified data owners and stewards:

* Sales → Customer, Order, Territory
* Manufacturing → Product, BOM, Work Orders
* Finance → Pricing, Costs, Transactions
* HR → Employee, Department

These stakeholders are responsible for business rules, data quality, and approval of changes.

##### 1.3.2.4 Data Quality Rules
Business teams provided:

* Validation rules
* Known data issues
* Exception handling procedures
* KPI definitions and calculation logic

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
* SalesOrderDetail: ~121,000
* Product: ~504
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
