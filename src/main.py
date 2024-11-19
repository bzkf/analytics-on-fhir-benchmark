import datetime
from pathlib import Path
import sys
import time
from loguru import logger
import pandas as pd
import docker
import gc
import time

from pathling_benchmark import PathlingBenchmark
from pyrate_benchmark import PyrateBenchmark
from trino_benchmark import TrinoBenchmark

NUM_RUNS_PER_ENGINE: int = 5


def main() -> int:
    results = pd.DataFrame()

    docker_client = docker.from_env()

    logger.info("Setting up benchmarks")
    trino = TrinoBenchmark()
    pyrate = PyrateBenchmark()
    pathling = PathlingBenchmark()

    resources_to_count = ["Patient", "Observation", "Encounter", "Condition"]

    resource_count_total = trino.get_resource_counts_total(resources_to_count)

    resource_counts = trino.get_resource_counts(resources_to_count)

    logger.info("Resource counts: {resource_counts}", resource_counts=resource_counts)

    logger.info(
        "Running {n} rounds per engine against {resource_count_total} total resources",
        n=NUM_RUNS_PER_ENGINE,
        resource_count_total=resource_count_total,
    )

    benchmark_timestamp = datetime.datetime.now(datetime.UTC)

    failed_run_count = 0

    for i in range(NUM_RUNS_PER_ENGINE):
        logger.info(
            "Run {i} out of {total_runs}", i=i + 1, total_runs=NUM_RUNS_PER_ENGINE
        )

        # trino
        trino_results = trino.run_all_queries(run_id=str(i))
        results = pd.concat([results, pd.DataFrame(trino_results)])
        docker_client.containers.get("analytics-on-fhir-benchmark-minio-1").restart()
        docker_client.containers.get("analytics-on-fhir-benchmark-trino-1").restart()
        gc.collect()

        logger.info("Done with trino. Waiting for 30s")
        time.sleep(30)

        # pathling
        # we occasionally observe transient OOM issues, so add retries here
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                pathling_results = pathling.run_all_queries(run_id=str(i))
                results = pd.concat([results, pd.DataFrame(pathling_results)])
                pathling.reset()
                break
            except Exception as exc:
                logger.error(
                    "Pathling benchmark failed {error}. Attempt {retry_count} out of {max_retries}.",
                    retry_count=retry_count,
                    max_retries=max_retries,
                    error=exc,
                )
                failed_run_count += 1
                retry_count += 1

        logger.info("Done with pathling. Waiting for 30s")
        time.sleep(30)

        # pyrate
        pyrate_results = pyrate.run_all_queries(run_id=str(i))
        results = pd.concat([results, pd.DataFrame(pyrate_results)])
        docker_client.containers.get("analytics-on-fhir-benchmark-blaze-1").restart()
        gc.collect()

        logger.info("Done with pyrate. Waiting for 30s")
        time.sleep(30)

    logger.info(
        "All benchmarks completed. Failed runs: {failed_run_count}",
        failed_run_count=failed_run_count,
    )
    output_dir = Path.cwd() / "results" / "benchmark-runs"
    output_dir.mkdir(parents=True, exist_ok=True)

    results["benchmark_timestamp"] = benchmark_timestamp

    # append the resource_count_total as a fixed-value column. Makes it easier to later facet by it.
    results["resource_count_total"] = resource_count_total

    for resource_type in resource_counts.keys():
        results[f"resource_count_{resource_type.lower()}"] = resource_counts[
            resource_type
        ]

    results.to_csv(
        output_dir
        / f"{time.strftime("%Y%m%d-%H%M%S")}-{resource_count_total}-benchmark-results.csv",
        index=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
