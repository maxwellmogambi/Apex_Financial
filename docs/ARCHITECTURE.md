# Apex Financial — Architecture

## Overview

Apex Financial uses a layered **Bronze → Silver → Gold** architecture implemented with PySpark and Parquet.

The design separates raw ingestion from data cleaning and analytical outputs, making each stage easier to understand, validate, and reuse.

## Data Flow

```text
Synthetic Data
      │
      ▼
   Bronze
      │
      ▼
   Silver
      │
      ▼
 Enrichment
      │
      ▼
    Gold
```

## Layers

### Bronze

The Bronze layer contains the generated source datasets with their original structure preserved.

Datasets:

* `customers`
* `cards`
* `devices`
* `merchants`
* `transactions`

Spark uses explicit schemas when reading the source Parquet files.

### Silver

Silver contains cleaned and validated versions of the Bronze datasets.

Processing includes:

* Type normalization
* Trimming string fields
* Null filtering
* Deduplication
* Transaction validation
* Referential-integrity checks

The five entity datasets remain separate in Silver rather than storing a duplicated enriched transaction dataset.

### Enrichment

Transaction data is enriched by joining it with customer, card, device, and merchant dimensions.

The enriched transaction DataFrame provides the foundation for the Gold layer.

### Gold

Gold contains analytics-ready datasets:

| Dataset                        | Purpose                                         |
| ------------------------------ | ----------------------------------------------- |
| `fact_transactions`            | Enriched transaction-level data                 |
| `customer_transaction_summary` | Customer transaction activity and fraud metrics |
| `merchant_transaction_summary` | Merchant transaction activity and fraud metrics |
| `transaction_risk_features`    | Analytical transaction-risk signals             |

The risk features include transaction timing, transaction amount, and customer/merchant regional differences. They are analytical signals rather than an ML fraud model.

## Design Decisions

### Parquet

Parquet was used as the storage format because it is a columnar format well suited to Spark-based analytical workloads.

### Explicit Schemas

Schemas are defined explicitly rather than relying on schema inference. This provides a clear data contract at ingestion and prevents incorrect type inference from propagating through the pipeline.

### Separate Enrichment from Silver Storage

The enriched transaction DataFrame is used to create Gold datasets but is not persisted as an additional Silver dataset. This avoids unnecessarily duplicating the transaction-level data.

## Scope

The implemented architecture currently ends at the Gold layer.

PostgreSQL serving, Power BI integration, orchestration, incremental processing, automated monitoring, and streaming are considered future extensions rather than part of the current core implementation.
