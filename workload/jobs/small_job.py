"""Small job: Word Count using RDD operations."""
import random
import string


def run_word_count(spark, input_path: str, num_partitions: int) -> None:
    """
    Run a word count job on the given text file using RDD operations.
    Materialized with .count() to force actual Spark execution.
    """
    sc = spark.sparkContext
    rdd = sc.textFile(input_path, minPartitions=num_partitions)
    result = (
        rdd
        .flatMap(lambda line: line.split())
        .map(lambda word: (word.lower().strip(string.punctuation), 1))
        .reduceByKey(lambda a, b: a + b)
        .count()
    )
    return result
