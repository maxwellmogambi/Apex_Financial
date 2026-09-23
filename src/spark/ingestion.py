from pyspark.sql import SparkSession
from src.spark.schemas import TRANSACTIONS_SCHEMA, CARDS_SCHEMA, CUSTOMERS_SCHEMA, DEVICES_SCHEMA, MERCHANTS_SCHEMA
from pathlib import Path
from src.spark.cleaning import clean_transactions, clean_customers, clean_cards, clean_devices, clean_merchants, validate_transactions
from src.spark.enrichment import enrich_transactions, validate_referential_integrity
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# Initialise a spark session
spark = SparkSession.builder \
    .appName("Apex Financial - Transaction Ingestion") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Print the Spark UI web address
print("🚀Spark UI Web URL:")
print(spark.sparkContext.uiWebUrl)

# Read the parquet files from the data directory
data_path = Path("data/synthetic")

transactions_df = spark.read.schema(TRANSACTIONS_SCHEMA).parquet(str(data_path / "transactions.parquet"))

# Schema Mapping for other dataframes
schemas = {
    "transactions": TRANSACTIONS_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "cards": CARDS_SCHEMA,
    "devices": DEVICES_SCHEMA,
    "merchants": MERCHANTS_SCHEMA
}

dfs = {}

for file in data_path.glob("*.parquet"):
    if file.stem in schemas:
        dfs[file.stem] = spark.read.schema(schemas[file.stem]).parquet(str(file))
    else:
        print(f"⚠️ No schema defined for {file.stem}. Skipping this file.")    


# Write back the dataframes to parquet files in bronze layer    
bronze_path = Path("data/bronze")

for name, df in dfs.items():
    df.write.mode("overwrite").parquet(str(bronze_path / f"{name}.parquet"))
    print(f"✅ Written {name} to {bronze_path / f'{name}.parquet'}")


# Clean and standardize the dataframes to create silver layer
cleaners = {
    "transactions": clean_transactions,
    "customers": clean_customers,
    "cards": clean_cards,
    "devices": clean_devices,
    "merchants": clean_merchants,
}

silver_dfs = {}

for name, clean_func in cleaners.items():
    silver_dfs[name] = clean_func(dfs[name])

# Write the cleaned silver dataframes to parquet files in silver layer
silver_path = Path("data/silver")

for name, df in silver_dfs.items():
    output_path = silver_path / f"{name}.parquet"

    df.write \
        .mode("overwrite") \
        .parquet(str(output_path))

    print(f"✅ Written {name} to {output_path}")    


# Read the silver transactions parquet file to verify the write operation
silver_transactions_check = spark.read.parquet(
    str(silver_path / "transactions.parquet")
)


# Data validation and integrity checks

silver_transactions = silver_dfs["transactions"]
validation_results = validate_transactions(silver_transactions)

print("\n=== Silver Validation ===")

for check, result in validation_results.items():
    print(f"{check}: {result}")


################################## ENRICHED TRANSACTIONS ###################################

enriched_transactions = enrich_transactions(
    transactions=silver_dfs["transactions"],
    customers=silver_dfs["customers"],
    cards=silver_dfs["cards"],
    devices=silver_dfs["devices"],
    merchants=silver_dfs["merchants"],
)    

# inspect the enriched transactions DataFrame
enriched_transactions.printSchema()

print("🚀 Enriched Row Count:", enriched_transactions.count())

# Validate referential integrity between transactions and other entities
integrity_results = validate_referential_integrity(
    silver_dfs["transactions"],
    silver_dfs["customers"],
    silver_dfs["cards"],
    silver_dfs["devices"],
    silver_dfs["merchants"],
)


# Create Gold fact table from enriched transactions
fact_transactions = enriched_transactions

gold_path = Path("data/gold")
gold_path.mkdir(parents=True, exist_ok=True)

fact_transactions.write \
    .mode("overwrite") \
    .parquet(str(gold_path / "fact_transactions.parquet"))


########################################### CUSTOMER TRANSACTION SUMMARY ###########################################

customer_transaction_summary = (
    fact_transactions
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
        F.min("timestamp").alias("first_transaction_at"),
        F.max("timestamp").alias("last_transaction_at"),
    )
    .withColumn(
        "fraud_rate",
        F.col("fraud_count") / F.col("transaction_count")
    )
)

# Write the customer transaction summary to Gold layer
customer_transaction_summary.write \
    .mode("overwrite") \
    .parquet(str(gold_path / "customer_transaction_summary.parquet"))

print(
    f"✅ Written customer_transaction_summary to "
    f"{gold_path / 'customer_transaction_summary.parquet'}"
)


########################################### MERCHANT TRANSACTION SUMMARY ###########################################

merchant_transaction_summary = (
    fact_transactions
    .groupBy("merchant_id")
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.sum(
            F.when(F.col("is_fraud") == 1, 1).otherwise(0)
        ).alias("fraud_count"),
        F.min("timestamp").alias("first_transaction_at"),
        F.max("timestamp").alias("last_transaction_at"),
    )
    .withColumn(
        "fraud_rate",
        F.col("fraud_count") / F.col("transaction_count")
    )
)

merchant_transaction_summary.write \
    .mode("overwrite") \
    .parquet(str(gold_path / "merchant_transaction_summary.parquet"))

print(
    f"✅ Written merchant_transaction_summary to "
    f"{gold_path / 'merchant_transaction_summary.parquet'}"
)


########################################### CUSTOMER TRANSACTION WINDOW ###########################################

# Customer transaction window
customer_window = (
    Window
    .partitionBy("customer_id")
    .orderBy("timestamp")
)

fact_transactions_ranked = (
    fact_transactions
    .withColumn(
        "transaction_sequence",
        F.row_number().over(customer_window)
    )
)

fact_transactions_ranked.select(
    "customer_id",
    "transaction_id",
    "timestamp",
    "amount",
    "transaction_sequence"
).orderBy(
    "customer_id",
    "timestamp"
)

# Calculate the time difference between consecutive transactions for each customer
customer_window = (
    Window
    .partitionBy("customer_id")
    .orderBy("timestamp")
)

fact_transactions_windowed = (
    fact_transactions
    .withColumn(
        "transaction_sequence",
        F.row_number().over(customer_window)
    )
    .withColumn(
        "previous_transaction_at",
        F.lag("timestamp").over(customer_window)
    )
    .withColumn(
        "minutes_since_previous_transaction",
        (
            F.col("timestamp").cast("long")
            - F.col("previous_transaction_at").cast("long")
        ) / 60
    )
)

fact_transactions_windowed.select(
    "customer_id",
    "transaction_id",
    "timestamp",
    "previous_transaction_at",
    "minutes_since_previous_transaction",
    "transaction_sequence"
).orderBy(
    "customer_id",
    "timestamp"
)

# Create risk features based on transaction patterns
transaction_risk_features = (
    fact_transactions_windowed
    .withColumn(
        "is_rapid_transaction",
        F.when(
            F.col("minutes_since_previous_transaction") <= 10,
            1
        ).otherwise(0)
    )
    .withColumn(
        "is_high_value_transaction",
        F.when(
            F.col("amount") >= 500,
            1
        ).otherwise(0)
    )
    .withColumn(
        "is_cross_region",
        F.when(
            F.col("customer_region") != F.col("merchant_region"),
            1
        ).otherwise(0)
    )
)

transaction_risk_features.select(
    "transaction_id",
    "customer_id",
    "amount",
    "minutes_since_previous_transaction",
    "is_rapid_transaction",
    "is_high_value_transaction",
    "customer_region",
    "merchant_region",
    "is_cross_region",
    "is_fraud"
).show(20, truncate=False)

# Write the transaction risk features to Gold layer

transaction_risk_features.write \
    .mode("overwrite") \
    .parquet(
        str(gold_path / "transaction_risk_features.parquet")
    )

print(
    f"✅ Written transaction_risk_features to "
    f"{gold_path / 'transaction_risk_features.parquet'}"
)


input("Press Enter to stop Spark...")
spark.stop()
