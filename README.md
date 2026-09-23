# Apex Financial — Financial Transactions Intelligence

A production-style Spark data engineering project that demonstrates how large-scale financial transaction data can be ingested, cleaned, enriched, transformed, validated, and analyzed using Apache Spark.

The project uses statistically informed synthetic financial transaction data and a layered Bronze → Silver → Gold architecture to simulate a transaction intelligence platform for a digital financial-services company.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Context](#business-context)
- [Project Objectives](#project-objectives)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data](#data)
- [Project Structure](#project-structure)
- [Spark Pipeline](#spark-pipeline)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Data Quality and Integrity](#data-quality-and-integrity)
- [Spark Concepts Demonstrated](#spark-concepts-demonstrated)
- [Performance and Optimization](#performance-and-optimization)
- [Scale Testing](#scale-testing)
- [Running the Project](#running-the-project)
- [Future Improvements](#future-improvements)
- [Key Takeaways](#key-takeaways)

---

## Project Overview

Apex Financial is a hands-on data engineering project built to explore Apache Spark through a realistic financial transaction processing workload.

The project simulates a digital financial-services company processing transactions across customers, cards, devices, and merchants. The platform is designed to transform raw transaction data into trusted analytical datasets that can support customer insights, merchant analysis, and transaction-risk monitoring.

The primary goal of the project is to demonstrate **practical Spark engineering**, rather than to build a production fraud-detection model or reproduce a real financial institution's infrastructure.

The project focuses on:

- Distributed data processing with PySpark
- Explicit data schemas
- Parquet-based data storage
- Layered data architecture
- Data cleaning and validation
- Referential-integrity validation
- Multi-entity joins and enrichment
- Aggregations and analytical transformations
- Window functions
- Transaction-risk feature engineering
- Spark execution plans
- Shuffle and partition behavior
- Broadcast joins
- Caching and persistence
- Data skew
- Performance experimentation
- Scaling workloads from 100K to 10M transactions

---

## Business Context

Apex Financial is a fictional digital financial-services company processing a growing volume of transactions across customers, cards, devices, and merchants.

As transaction volumes increase, the organization needs a scalable data processing platform capable of turning operational transaction data into reliable analytical datasets.

The platform needs to support questions such as:

- How are customers transacting over time?
- What are the transaction volumes and values associated with customers and merchants?
- Where are unusual transaction patterns occurring?
- How frequently are rapid transactions occurring?
- Which transactions involve high-value amounts?
- How often do customers transact across regions?
- How does transaction-processing performance change as data volume increases?

The project addresses these requirements through a Spark-based processing pipeline that transforms transaction data into structured Bronze, Silver, and Gold layers.

The resulting Gold datasets provide a foundation for downstream analytics and visualization.

---

## Project Objectives

The project has two complementary objectives.

### 1. Build a realistic Spark data pipeline

Implement an end-to-end transaction processing workflow that demonstrates:

- Data ingestion
- Schema enforcement
- Cleaning
- Deduplication
- Validation
- Entity enrichment
- Aggregation
- Window-based analysis
- Analytical feature engineering
- Layered Parquet outputs

### 2. Understand Spark performance

Use controlled experiments to understand how Spark behaves as workloads and processing patterns change.

The optimization work specifically explores:

- Execution plans
- Shuffle operations
- Shuffle partition configuration
- `repartition()` versus `coalesce()`
- Broadcast joins
- Caching and persistence
- Data skew
- Increasing data volume
- Spark execution behavior at larger workloads

The project deliberately keeps these experiments separate from the core ingestion pipeline so that the production-style pipeline remains focused on business processing while the optimization notebook acts as a performance laboratory.

---

## Architecture

The implemented pipeline follows a layered architecture:

```text
                         ┌─────────────────────┐
                         │   IEEE-CIS Data     │
                         │  Reference Dataset  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Synthetic Generator │
                         │   Python / NumPy    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Synthetic Data    │
                         │      Parquet        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       Bronze        │
                         │ Raw Spark-readable  │
                         │      Parquet        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       Silver        │
                         │ Cleaned + Validated │
                         │      Parquet        │
                         └──────────┬──────────┘
                                    │
                          ┌─────────┴─────────┐
                          │                   │
                          ▼                   ▼
                   Entity Enrichment    Data Validation
                          │                   │
                          └─────────┬─────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │        Gold         │
                         │ Analytics-ready     │
                         │      datasets       │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Downstream Analytics│
                         │    / Visualization  │
                         │    Future Scope     │
                         └─────────────────────┘
```

### Implemented Scope

The current implementation covers the pipeline through the Gold layer:                         

``` text
Synthetic Data
      ↓
   Bronze
      ↓
   Silver
      ↓
   Enrichment
      ↓
    Gold
```
PostgreSQL serving and Power BI integration are intentionally outside the current implementation scope and are documented as future improvements.

---
### Technology Stack


<table style="width: 100%; border-collapse: collapse; font-family: sans-serif; margin: 20px 0;">
  <thead>
    <tr style="background-color: #f2f2f2; border-bottom: 2px solid #dddddd;">
      <th style="padding: 12px; text-align: left; font-weight: bold;">Technology</th>
      <th style="padding: 12px; text-align: left; font-weight: bold;">Purpose</th>
    </tr>
  </thead>
  <tbody>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Python</td>
      <td style="padding: 12px; color: #555;">Data generation and Spark application development</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">PySpark</td>
      <td style="padding: 12px; color: #555;">Distributed data processing</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Apache Spark</td>
      <td style="padding: 12px; color: #555;">Execution engine</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Pandas</td>
      <td style="padding: 12px; color: #555;">Supporting data analysis and profiling</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">NumPy</td>
      <td style="padding: 12px; color: #555;">Synthetic data generation</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">PyArrow</td>
      <td style="padding: 12px; color: #555;">Parquet/data interoperability</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Parquet</td>
      <td style="padding: 12px; color: #555;">Columnar data storage</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Jupyter</td>
      <td style="padding: 12px; color: #555;">Data profiling, validation, and Spark experiments</td>
    </tr>
    <tr style="border-bottom: 1px solid #dddddd;">
      <td style="padding: 12px; font-weight: bold; color: #333;">Git</td>
      <td style="padding: 12px; color: #555;">Version control</td>
    </tr>
  </tbody>
</table>



The project is developed and executed locally using Spark with local[*].



