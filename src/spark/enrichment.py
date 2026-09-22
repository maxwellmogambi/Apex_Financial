from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_transactions(
    transactions: DataFrame,
    customers: DataFrame,
    cards: DataFrame,
    devices: DataFrame,
    merchants: DataFrame,
) -> DataFrame:

    enriched = (
        transactions.alias("t")

        .join(
            customers.alias("c"),
            F.col("t.customer_id") == F.col("c.customer_id"),
            "left",
        )

        .join(
            cards.alias("ca"),
            F.col("t.card_id") == F.col("ca.card_id"),
            "left",
        )

        .join(
            devices.alias("d"),
            F.col("t.device_id") == F.col("d.device_id"),
            "left",
        )

        .join(
            merchants.alias("m"),
            F.col("t.merchant_id") == F.col("m.merchant_id"),
            "left",
        )

        .select(
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            F.col("t.card_id"),
            F.col("t.merchant_id"),
            F.col("t.device_id"),
            F.col("t.timestamp"),
            F.col("t.amount"),
            F.col("t.product_type"),
            F.col("ca.card_type"),
            F.col("t.payment_type"),
            F.col("c.customer_region"),
            F.col("m.merchant_region"),
            F.col("t.has_identity"),
            F.col("t.is_fraud"),
        )
    )

    return enriched


def validate_referential_integrity(
    transactions: DataFrame,
    customers: DataFrame,
    cards: DataFrame,
    devices: DataFrame,
    merchants: DataFrame,
) -> dict:

    customer_orphans = (
        transactions
        .join(customers.select("customer_id"), "customer_id", "left_anti")
        .count()
    )

    card_orphans = (
        transactions
        .join(cards.select("card_id"), "card_id", "left_anti")
        .count()
    )

    device_orphans = (
        transactions
        .join(devices.select("device_id"), "device_id", "left_anti")
        .count()
    )

    merchant_orphans = (
        transactions
        .join(merchants.select("merchant_id"), "merchant_id", "left_anti")
        .count()
    )

    return {
        "customer_orphans": customer_orphans,
        "card_orphans": card_orphans,
        "device_orphans": device_orphans,
        "merchant_orphans": merchant_orphans,
    }