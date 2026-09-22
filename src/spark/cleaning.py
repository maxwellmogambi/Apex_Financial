from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_transactions(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Bronze transactions DataFrame.
    """

    cleaned_df = (
        df
        # Normalize the fraud flag to an analytical integer type
        .withColumn("is_fraud", F.col("is_fraud").cast("int"))

        # Remove records without a transaction identifier
        .filter(F.col("transaction_id").isNotNull())

        # Standardize string fields
        .withColumn("product_type", F.trim(F.col("product_type")))
        .withColumn("card_type", F.trim(F.col("card_type")))
        .withColumn("payment_type", F.trim(F.col("payment_type")))
        .withColumn("customer_region", F.trim(F.col("customer_region")))
        .withColumn("merchant_region", F.trim(F.col("merchant_region")))

        # Remove duplicate transaction IDs
        .dropDuplicates(["transaction_id"])
    )

    return cleaned_df

def clean_customers(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Bronze customers DataFrame."""
    return (
        df
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_region", F.trim(F.col("customer_region")))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def clean_cards(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Bronze cards DataFrame."""
    return (
        df
        .withColumn("card_id", F.trim(F.col("card_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("card_type", F.trim(F.col("card_type")))
        .filter(F.col("card_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["card_id"])
    )


def clean_devices(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Bronze devices DataFrame.
    """
    return (
        df
        .withColumn("device_id", F.trim(F.col("device_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .filter(F.col("device_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["device_id"])
    )


def clean_merchants(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Bronze merchants DataFrame.
    """
    return (
        df
        .withColumn("merchant_id", F.trim(F.col("merchant_id")))
        .withColumn("merchant_region", F.trim(F.col("merchant_region")))
        .filter(F.col("merchant_id").isNotNull())
        .dropDuplicates(["merchant_id"])
    )

def validate_transactions(df: DataFrame) -> dict:
    """
    Validate core integrity rules for Silver transactions.
    """

    total_rows = df.count()

    null_transaction_ids = (
        df.filter(F.col("transaction_id").isNull()).count()
    )

    duplicate_transaction_ids = (
        df.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    invalid_fraud_flags = (
        df.filter(~F.col("is_fraud").isin(0, 1)).count()
    )

    return {
        "total_rows": total_rows,
        "null_transaction_ids": null_transaction_ids,
        "duplicate_transaction_ids": duplicate_transaction_ids,
        "invalid_fraud_flags": invalid_fraud_flags,
    }
