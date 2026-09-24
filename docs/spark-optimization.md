# Apex Financial — Spark Optimization

## Purpose

The optimization work explores how Spark execution changes with different processing patterns and increasing data volumes.

The experiments were performed in `notebooks/03_spark_optimization.ipynb` and were kept separate from the core ingestion pipeline.

---

## Execution Plans and Shuffle

Spark execution plans were inspected using:

```python
df.explain("formatted")
```

A `groupBy()` aggregation showed Spark introducing an `Exchange` stage, demonstrating that the aggregation required a **shuffle**.

The key takeaway is that operations involving data redistribution can become expensive as data volume increases.

---

## Shuffle Partitions

The project tested different values of:

```text
spark.sql.shuffle.partitions
```

At smaller workloads, changing the partition count produced inconsistent local execution times.

This demonstrated that:

> More shuffle partitions do not automatically mean better performance.

The appropriate number depends on factors such as data volume, workload, and available compute resources.

---

## `repartition()` vs `coalesce()`

The experiments demonstrated the difference between the two operations.

### `repartition()`

* Can increase or decrease the number of partitions.
* Performs a shuffle.
* Useful when data needs to be redistributed.

### `coalesce()`

* Primarily used to reduce partitions.
* Avoids a full shuffle.
* Generally cheaper when reducing partition count.

---

## Broadcast Joins

A normal join was compared with a broadcast join.

```python
F.broadcast(customers)
```

The normal join can require data redistribution, while the broadcast join distributes the smaller dataset to the workers.

The physical plans demonstrated the difference between:

```text
SortMergeJoin
```

and:

```text
BroadcastHashJoin
```

Broadcast joins are useful when one side of a join is sufficiently small to distribute to workers.

---

## Caching and Persistence

Caching was explored as a mechanism for reusing expensive intermediate DataFrames.

```python
df.cache()
```

Caching is useful when an expensive DataFrame is reused by multiple downstream operations.

It is not automatically beneficial because cached data consumes memory. For data that is used once, caching can provide little or no benefit.

---

## Data Skew

A deliberately skewed dataset was created where approximately half of the 100K transactions belonged to a single customer.

```text
SKEWED_CUSTOMER → ~50,000 transactions
Other customers  → small numbers of transactions
```

This demonstrated how an uneven distribution of keys can cause disproportionate processing on individual partitions.

At larger production-scale workloads, severe skew can create task-level bottlenecks.

Techniques such as salting can mitigate extreme skew, but were not introduced into the core pipeline because the project does not require them at its current scale.

---

## Scale Testing

The final experiment increased the workload while running the same customer-level aggregation.

| Dataset |       Rows | Partitions | Execution Time |
| ------- | ---------: | ---------: | -------------: |
| 100K    |    100,000 |         10 |    ~1.05–1.35s |
| 1M      |  1,000,000 |        100 |  ~13.19–16.40s |
| 10M     | 10,000,000 |      1,000 |       ~186.67s |

The results demonstrate that execution time increases substantially as the workload grows.

The experiments were performed locally, so the timings are **observational rather than production performance benchmarks**. They are intended to demonstrate Spark behaviour and provide practical experience with distributed processing concepts.

---

## Key Takeaways

The optimization work demonstrated that Spark performance depends on more than simply increasing compute or partition counts.

Key factors include:

* Shuffle and data movement
* Partition sizin
