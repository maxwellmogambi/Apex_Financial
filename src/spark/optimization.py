import time

from pyspark.sql import functions as F


def benchmark_customer_aggregation(df, shuffle_partitions):
    spark = df.sparkSession

    spark.conf.set(
        "spark.sql.shuffle.partitions",
        shuffle_partitions
    )

    start = time.time()

    (
        df
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_transaction_amount"),
        )
        .count()
    )

    elapsed = time.time() - start

    return elapsed