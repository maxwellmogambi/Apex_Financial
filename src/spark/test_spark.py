from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ApexFinancial")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (1, "customer_001", 100.50),
    (2, "customer_002", 250.00),
    (3, "customer_001", 75.25),
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "customer_id", "amount"]
)

df.show()

print(f"Row count: {df.count()}")

spark.stop()