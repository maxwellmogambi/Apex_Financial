# Apex Financial — Data and Quality

## Data Sources

The project uses two types of data:

* **IEEE-CIS Fraud Detection dataset** — used as a statistical reference for understanding real transaction characteristics.
* **Synthetic financial data** — generated specifically for the Apex Financial pipeline.

The IEEE-CIS dataset is not used as the production input to the Spark pipeline. Instead, its characteristics informed the synthetic generator.

This approach provides a realistic workload while keeping the project self-contained.

## Synthetic Data

The generator produces five related datasets:

| Dataset        | Description                                 |
| -------------- | ------------------------------------------- |
| `customers`    | Customer identifiers and regions            |
| `cards`        | Cards associated with customers             |
| `devices`      | Devices associated with customers           |
| `merchants`    | Merchant identifiers and regions            |
| `transactions` | Financial transactions linking the entities |

The generator uses a fixed seed (`42`) to produce reproducible data.

The initial dataset contains:

* 3,000 customers
* 3,981 cards
* 3,812 devices
* 1,500 merchants
* 10,000 transactions

The transaction data was calibrated against the reference dataset for characteristics such as:

* Fraud rate
* Product distribution
* Fraud distribution by product
* Payment-type fraud patterns
* Transaction amount distribution
* Identity availability and its relationship with fraud

The synthetic dataset is designed to be **statistically realistic rather than an exact reproduction** of the IEEE-CIS dataset.

## Data Quality

Quality checks are applied during the Silver processing stage.

### Transaction Validation

The pipeline validates:

* Missing transaction IDs
* Duplicate transaction IDs
* Invalid fraud flags
* Expected data types

### Referential Integrity

Transaction records are checked against their related entities using Spark joins.

The pipeline verifies that transactions do not contain orphaned:

* Customer IDs
* Card IDs
* Device IDs
* Merchant IDs

The current dataset passes these checks with zero orphan records.

## Cleaning

Silver transformations include:

* Explicit type casting
* String trimming
* Null filtering for required identifiers
* Duplicate removal

The cleaned datasets are written as Parquet and provide the trusted inputs for enrichment and Gold transformations.

## Data Contract

Explicit Spark schemas define the expected structure and data types of the source datasets.

This ensures that the Bronze layer has a predictable schema and prevents incorrect type inference from propagating into downstream processing.

Semantic transformations, such as converting the transaction fraud flag to an integer, are applied during Silver processing rather than altering the Bronze source contract.

## Current Quality Status

The current synthetic dataset and Spark pipeline successfully pass the implemented structural, validation, and referential-integrity checks.

The project does not attempt to provide production-grade automated data-quality monitoring. That is considered future scope.
