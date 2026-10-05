# 📊 Data Warehouse and Power BI Project
![SQL](https://img.shields.io/badge/SQL-Data%20Modeling-red)
![Power BI](https://img.shields.io/badge/Power%20BI-Data%20Analytics-yellow)
![DAX](https://img.shields.io/badge/DAX-Measures-blue)
![Finance](https://img.shields.io/badge/Finance-KPIs-green)
![Sales](https://img.shields.io/badge/Sales-Analytics-orange)

Hi everyone! Welcome to the **Data Warehouse and Power BI Project** repository. 
The goal of this project is to demostrate a comprehenisive data warehousing and business analitical solution, form building a data warehouse with **Microsoft SQL Server**, orchestrate by SQL Server Integration Services **SSIS** to generating insights with **Power BI**.

---
# 🚀Project Requirements
**Building the Data Warehouse (Data Engineering)**

**Objective**

Develop a modern Data warehouse using SQL Server and SISS for the etl orchestration, to consolidate sales and orders data, enalbling analytical reporting and informed decision-making.

**Specifications**

  * **Data Sources:** Import data from SQL Server OLTP Database AdventureWorks2025.
  * **Data Quality:** Clean and resolve data quality issues prior to analysis.
  * **Integration:** Data enrichment (config tables to data-driven, derived columns) and denormalizaiton to provide an efficiently Star Schema model to BI Systems and analytical queries.
  * **Scope:** Focus on historization data SCD type 2 and 1, anomaly detection tables.
  * **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics team.
---
**BI: Analytics & Reporting (Data Analysis)**

**Objective**

Develop SQL-based analytics to deliver detail insight into:

  * **Customers Behavior**
  * **Product Performance**
  * **Sales Trends**

These insights empower stakeholders with key business metrics, enbaling strategig decision-making

For more details, refert to 

## 📐✏️👷‍♀️ Architecture Proposal
![Data Architectures Approach](Images/Data_Architectures_Approach.png)

In this project I decided to build a **Data Warehouse** because the data I work with is **highly structured** and comes from well‑defined operational systems. A DWH is the most suitable environment for organizing this type of information, applying governance, and ensuring consistency across analytical processes.

Another practical reason behind this choice is the **technology available to the customer**: SQL Server and SSIS. These tools are reliable, widely adopted, and perfectly aligned with a classical DWH approach.

To design the **data pipeline**, I adopted the Medallion Architecture because is simple to understand, easy to maintain, and scales well as new domains or data sources are added.

This model provides a clean separation of responsibilities and simplifies the entire ETL lifecycle:

* **Bronze** stores raw data exactly as received from the source, ensuring traceability and reproducibility.
* **Silver** applies cleaning, normalization, and business rules, producing refined datasets ready for downstream consumption.
* **Gold** exposes curated, analytics‑ready tables optimized for reporting, dashboards, and business insights.

---
# ✍🏻💡 Logical Steps to follow it
![DWH_Medallion_Steps.png](Images/DWH_Medallion_Steps.png)

---
# Building the Data Warehouse (Data Engineering)
![DWH_Architetcture_High_Level](Images/DWH_Architetcture_High_Level.jpg)

The DWH it is **orchestrate** by Server Integration Services **SSIS** and **refreshed** scheduled by **SQL Agent**.

---
## 🔥 Workflow
![DWH_Architetcture_DataFlow.drawio](Images/DWH_Architetcture_DataFlow.drawio.png)

---

## 📂 Projects
### 1. AdventureWorks – Executive Sales Dashboard
- **Dataset OLTP:** AdventureWorks2025
- **Focus:** Sales performance, profitability, customer insights  
- **Tech:** Power BI, SQL Server, DAX  
