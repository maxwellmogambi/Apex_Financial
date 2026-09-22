from pyspark.sql import SparkSession
from src.spark.schemas import TRANSACTIONS_SCHEMA, CARDS_SCHEMA, CUSTOMERS_SCHEMA, DEVICES_SCHEMA, MERCHANTS_SCHEMA
from pathlib import Path
from src.spark.cleaning import clean_transactions, clean_customers, clean_cards, clean_devices, clean_merchants, validate_transactions

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
silver_transactions_check.printSchema()

print("Silver Row Count:", silver_transactions_check.count())

silver_transactions_check.show(5, truncate=False)


# Data validation and integrity checks
silver_transactions = silver_dfs["transactions"]
validation_results = validate_transactions(silver_transactions)

print("\n=== Silver Validation ===")

for check, result in validation_results.items():
    print(f"{check}: {result}")