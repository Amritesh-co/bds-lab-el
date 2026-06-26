"""Large job: Iterative PageRank using RDD operations."""

# Each iteration chains another join+flatMap+reduceByKey onto the RDD lineage.
# Past ~50-100 iterations the unbroken lineage graph is deep enough that Spark's
# recursive DAG traversal (e.g. doCheckpoint) blows the JVM stack. Truncating
# the lineage with an actual checkpoint every CHECKPOINT_INTERVAL iterations
# keeps the chain short regardless of how many iterations are requested.
CHECKPOINT_INTERVAL = 20


def run_iterative_pagerank(spark, input_path: str, num_partitions: int, iterations: int = 10) -> None:
    """
    Run iterative PageRank on an adjacency list.
    Each line in input: 'source_node neighbor_node'
    Materialized with .collect() after final iteration.
    """
    sc = spark.sparkContext
    if sc.getCheckpointDir() is None:
        sc.setCheckpointDir("/tmp/spark-checkpoints")

    # Parse adjacency list: (source, neighbor)
    lines = sc.textFile(input_path, minPartitions=num_partitions)

    # Build links RDD: (node, [neighbors])
    links = (
        lines
        .map(lambda line: line.strip().split())
        .filter(lambda parts: len(parts) == 2)
        .map(lambda parts: (parts[0], parts[1]))
        .distinct()
        .groupByKey()
        .cache()
    )

    # Initialize ranks: every node starts with rank 1.0
    ranks = links.mapValues(lambda _: 1.0)

    # Iterative PageRank computation
    for i in range(iterations):
        # Compute contributions: each node distributes rank equally to neighbors
        contribs = (
            links.join(ranks)
            .flatMap(lambda node_links_rank: [
                (neighbor, node_links_rank[1][1] / len(node_links_rank[1][0]))
                for neighbor in node_links_rank[1][0]
            ])
        )
        # Update ranks with damping factor 0.85
        ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(lambda rank: 0.15 + 0.85 * rank)

        if (i + 1) % CHECKPOINT_INTERVAL == 0:
            ranks.cache()
            ranks.checkpoint()
            ranks.count()  # force materialization so the checkpoint actually truncates lineage

    # Materialize — force execution
    result = ranks.collect()
    return len(result)
