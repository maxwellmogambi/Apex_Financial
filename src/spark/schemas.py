from pyspark.sql.types import(
    LongType,
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    BooleanType
)

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("amount", DoubleType(), False),
    StructField("product_type", StringType(), False),
    StructField("card_type", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("customer_region", StringType(), False),
    StructField("merchant_region", StringType(), False),
    StructField("has_identity", BooleanType(), False),
    StructField("is_fraud", LongType(), False),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_region", StringType(), False),
])

CARDS_SCHEMA = StructType([
    StructField("card_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_type", StringType(), False),
])

DEVICES_SCHEMA = StructType([
    StructField("device_id", StringType(), False),
    StructField("customer_id", StringType(), False),
])

MERCHANTS_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_region", StringType(), False),
])