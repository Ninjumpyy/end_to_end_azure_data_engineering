# End-to-end Azure Data Engineering - Banking Transaction Pipeline

## Introduction

This end-to-end data engineering project simulates a multi-bank environment and implements a complete Medallion Architecture (Bronze → Silver → Gold) on Azure.

The platform ingests data from:

- Two simulated core banking systems (Bank A & Bank B – Azure SQL Databases)
- Flat files (settlements, disputes, MCC codes)
- An external FX Rates API

It orchestrates ingestion using Azure Data Factory, stores data in Azure Data Lake Storage Gen2 (hierarchical namespace enabled), transforms data with Azure Databricks (PySpark), and implements governance, auditing, and SCD2 modeling in the Gold layer.

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)
place photo of the architecture here 

## Technology Used

### Cloud Platform
- Microsoft Azure

### Services Used
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (Hierarchical Namespace enabled)
- Azure Databricks
- Azure SQL Database
- Azure Key Vault

### Languages & Frameworks
- Python (data generators, API extract)
- PySpark (transformations)
- SQL (DDL, transformations)
- Delta Lake

## Data Used

blablabla
Core Banking Databases
Flat Files
API Source - FX Rates (EUR base)


## Data Model

![Data Model](docs/data_model.png)


## What This Project Demonstrates

This project demonstrates:

- Designing a scalable Azure data platform
- Implementing enterprise ingestion patterns
- Handling multi-source harmonization
- Building a full Medallion Architecture
- Applying SCD2 modeling
- Implementing auditing and incremental processing
- Managing secrets securely
- Orchestrating complex pipelines in ADF

## Future Improvements

- Unity Catalog integration
- CI/CD deployment for ADF & Databricks
- Delta optimizations (Z-Ordering)
- Monitoring dashboard (Power BI)
- Data quality metrics framework

## Scripts for Project

link to files 
Extract
bronze to silver
silver to gold







---

## Security & Governance

- Secrets stored in Azure Key Vault
- Databricks authenticated via Service Principal
- No hardcoded credentials
- Secure Linked Services
- Controlled ADLS access

---

## Advanced Concepts Implemented

- Medallion Architecture
- SCD Type 2
- Incremental Load (Watermark-based)
- Audit-driven ingestion
- Parquet optimization
- CDM-style harmonization
- Parallel ADF execution
- Quarantine data flow
- is_active semantic modeling
- Retry & dependency conditions
- Dimensions (SCD2)
- Fact tables
- Surrogate keys
- Temporal validity columns