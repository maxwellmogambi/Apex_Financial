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
