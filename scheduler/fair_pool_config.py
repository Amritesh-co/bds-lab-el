"""Dynamically generates fairscheduler.xml based on current job pool weights."""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAIRSCHEDULER_PATH = os.path.join(PROJECT_ROOT, "conf", "fairscheduler.xml")


def get_pool_name(predicted_seconds: float) -> str:
    """Return the FAIR scheduler pool name for the given predicted execution time."""
    if predicted_seconds < 10.0:
        return "small_pool"
    elif predicted_seconds <= 60.0:
        return "medium_pool"
    return "large_pool"


def generate_fairscheduler_xml(pool_weights: dict = None, output_path: str = None) -> str:
    """
    Generate fairscheduler.xml with given pool weights.

    pool_weights: dict like {"small_pool": 3, "medium_pool": 2, "large_pool": 1}
    Writes to output_path (default: conf/fairscheduler.xml).
    Returns the XML string.
    """
    if pool_weights is None:
        pool_weights = {
            "small_pool": 3,
            "medium_pool": 2,
            "large_pool": 1,
        }
    if output_path is None:
        output_path = FAIRSCHEDULER_PATH

    lines = ['<?xml version="1.0"?>', "<allocations>"]
    for pool_name, weight in pool_weights.items():
        lines += [
            f'  <pool name="{pool_name}">',
            "    <schedulingMode>FIFO</schedulingMode>",
            f"    <weight>{weight}</weight>",
            "    <minShare>1</minShare>",
            "  </pool>",
        ]
    lines.append("</allocations>")
    xml = "\n".join(lines) + "\n"

    with open(output_path, "w") as f:
        f.write(xml)

    return xml


def compute_adaptive_weights(job_specs: list) -> dict:
    """
    Compute pool weights adaptively based on the number of jobs in each tier.
    More small jobs → higher weight for small_pool (to clear them faster).
    """
    from scheduler.resource_policy import predict_config, SMALL_THRESHOLD, LARGE_THRESHOLD

    counts = {"small_pool": 0, "medium_pool": 0, "large_pool": 0}
    for spec in job_specs:
        pt = spec.get("predicted_time", 30.0)
        pool = get_pool_name(pt)
        counts[pool] += 1

    total = sum(counts.values()) or 1
    # Normalize to weights 1–5 (more jobs in a pool → higher weight to drain it)
    weights = {}
    for pool, count in counts.items():
        w = max(1, round(5 * count / total))
        weights[pool] = w

    return weights
