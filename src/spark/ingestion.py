from pyspark.sql import SparkSession
from src.spark.schemas import TRANSACTIONS_SCHEMA
from pathlib import Path

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

dfs = {}

transactions_df = spark.read.schema(TRANSACTIONS_SCHEMA).parquet(str(data_path / "transactions.parquet"))

transactions_df.printSchema()

print("Row Count:", transactions_df.count())

transactions_df.show(5, truncate=False)

# for file in data_path.glob("*.parquet"):
#     name = file.stem
#     dfs[name] = spark.read.parquet(str(file))


# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     StringType,
#     TimestampType,
#     DoubleType,
#     BooleanType,
#     IntegerType,
# )


# # Write back the dataframes to parquet files in bronze layer    
# bronze_path = Path("data/bronze")

# for name, df in dfs.items():
#     df.write.mode("overwrite").parquet(str(bronze_path / f"{name}.parquet"))
#     print(f"✅ Written {name} to {bronze_path / f'{name}.parquet'}")

    