"""Maps predicted execution time to Spark resource configurations."""


# Tier thresholds (seconds)
SMALL_THRESHOLD = 10.0
LARGE_THRESHOLD = 60.0


def predict_config(predicted_seconds: float) -> dict:
    """
    Return a dict of Spark configuration overrides based on predicted job size.

    Tiers:
      < 10s  → small  : minimal resources
      10–60s → medium : balanced resources
      > 60s  → large  : generous resources
    """
    if predicted_seconds < SMALL_THRESHOLD:
        return {
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.memory": "1g",
        }
    elif predicted_seconds <= LARGE_THRESHOLD:
        return {
            "spark.executor.memory": "1g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory": "1g",
        }
    else:
        return {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "16",
            "spark.driver.memory": "2g",
        }


def get_tier_label(predicted_seconds: float) -> str:
    """Return human-readable tier label."""
    if predicted_seconds < SMALL_THRESHOLD:
        return "small"
    elif predicted_seconds <= LARGE_THRESHOLD:
        return "medium"
    return "large"
