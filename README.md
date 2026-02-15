# End-to-end Azure Data Engineering - Banking Transaction Pipeline

## Introduction

This end-to-end data engineering project simulates a multi-bank environment and implements a complete Medallion Architecture (Bronze → Silver → Gold) on Azure.

The platform ingests data from:

- Two simulated core banking systems (Bank A & Bank B – Azure SQL Databases)
- Flat files (settlements, disputes, MCC codes)
- An external FX Rates API

It orchestrates ingestion using Azure Data Factory, stores data in Azure Data Lake Storage Gen2 (hierarchical namespace enabled), transforms data with Azure Databricks (PySpark), and implements governance, auditing, and SCD2 modeling in the Gold layer.

---

## Architecture

![Architecture Diagram](docs/architecture.png)

### Flow Overview

1. Data Generation (Local Python)
2. Bootstrap Pipeline → Load CSV into Azure SQL
3. Bronze Ingestion (ADF)
   - SQL → Parquet (ADLS Bronze)
   - Flat files → Parquet
   - API Extract → Parquet
4. Silver Transformation (Databricks)
5. Gold Modeling (Databricks, SCD2, dimensional model)
6. Master Pipeline orchestrates Bronze → Silver → Gold

---

## Technology Stack

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

---

## Data Sources

### 1. Core Banking Databases (Bank A & Bank B)

Simulated using local Python generators:

- Customers
- Accounts
- Products
- Branches
- Merchants
- Counterparties
- Transactions

Orchestrated via:
- `orchestrator.py`
- `core_generators.py`
- `reference_generators.py`
- `fact_generators.py`

---

### 2. Flat Files (Landing → Bronze)

- MCC Codes
- Settlements (Bank A & B)
- Disputes (Bank A & B)

Stored in ADLS and ingested via generic ADF datasets.

---

### 3. API Source

- FX Rates (EUR base)
- Extracted using Databricks notebook
- Written directly to Bronze in Parquet format

---

## Storage Architecture (ADLS Gen2)

Containers:

- landing
- bronze
- silver
- gold
- configs
- bootstrap
- audit

Hierarchical namespace enabled to support:

- Folder-level organization
- Efficient partitioning
- Structured Medallion design

---

## Medallion Architecture Implementation

### Bronze Layer

Raw ingestion in Parquet format.

Characteristics:
- Append-only
- Schema preservation
- Source-system traceability
- Retry logic in ADF
- Parallel pipeline execution

Sources:
- SQL → Parquet
- Flat files → Parquet
- API → Parquet

---

### Silver Layer

Cleansed, standardized, enriched data.

Key Features:
- Data type normalization
- Deduplication
- Business rule enforcement
- `is_active` flag implementation
- Quarantine flow for invalid records
- CDM-style harmonization between Bank A & B

---

### Gold Layer

Business-ready dimensional model.

Key Implementations:

- Star schema
- SCD Type 2
- Surrogate keys
- Current/History separation
- Audit traceability

Example Gold objects:
- `dim_account`
- `dim_customer`
- Fact tables (transactions, settlements)

---

## Audit Framework

Custom-built audit system in ADLS (Delta format).

Audit components:

- `audit_start`
- `audit_end`
- `fetch_logs`
- `catch_new_watermark_value`

Tracks:
- Pipeline name
- Run ID
- Source
- Load mode (Full / Incremental)
- Watermark value
- Status (STARTED / SUCCESS / FAILED)
- Row counts

Enables:
- Reprocessing capability
- Incremental logic
- Monitoring of ingestion behavior

---

## Pipeline Design

### Bootstrap Pipeline
- Reads `bootstrap_load_config.csv`
- Loads generated CSV into Azure SQL
- Lookup + ForEach pattern

### Bronze Ingestion
- Switch logic (SQL / Flat file / API)
- Parallel execution
- GetMetadata existence checks
- Retry policies implemented

### Silver Pipeline
- Databricks notebooks per domain
- Modular transformations

### Gold Pipeline
- SCD2 implementation
- Surrogate key generation

### Master Pipeline
Executes:

Bronze → Silver → Gold

With dependency chaining and failure control.

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

---

## Data Model (Gold)

![Data Model](docs/data_model.png)

Implements:

- Dimensions (SCD2)
- Fact tables
- Surrogate keys
- Temporal validity columns

---

## Project Structure

