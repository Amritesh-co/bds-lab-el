"""Medium job: Join + Aggregation using DataFrames."""
from pyspark.sql import functions as F


def run_join_aggregation(spark, input_path_a: str, input_path_b: str, num_partitions: int) -> None:
    """
    Read two CSVs, join on 'id' key, groupBy category and aggregate.
    Materialized with .count() to force actual Spark execution.
    """
    df_a = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path_a)
        .repartition(num_partitions)
    )
    df_b = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path_b)
    )

    joined = df_a.join(df_b, on="id", how="inner")

    result = (
        joined
        .groupBy("category")
        .agg(
            F.sum("value").alias("total_value"),
            F.avg("value").alias("avg_value"),
            F.count("*").alias("record_count")
        )
        .count()
    )
    return result
