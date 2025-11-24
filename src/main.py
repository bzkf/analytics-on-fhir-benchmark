import datetime
import os
from pathlib import Path
import sys
import time
from loguru import logger
import pandas as pd
import docker
import gc

from pathling_benchmark import PathlingBenchmark
from pyrate_benchmark import PyrateBenchmark
from trino_benchmark import TrinoBenchmark

NUM_RUNS_PER_ENGINE: int = 5


def main() -> int:
    results = pd.DataFrame()

    docker_client = docker.from_env()

    logger.info("Setting up benchmarks")
    trino = TrinoBenchmark()
    pyrate_hapi = PyrateBenchmark(
        fhir_server_base_url="http://localhost:8084/fhir/",
        fhir_server_name="hapi",
    )
    pyrate_blaze = PyrateBenchmark(
        fhir_server_base_url="http://localhost:8083/fhir/",
        fhir_server_name="blaze",
    )
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

    for cold_or_warm in ["cold", "warm"]:
        logger.info(
            "Running benchmarks in {cold_or_warm} state", cold_or_warm=cold_or_warm
        )

        runs_to_perform = NUM_RUNS_PER_ENGINE
        if cold_or_warm == "warm":
            runs_to_perform = runs_to_perform + 1

        for i in range(runs_to_perform):
            logger.info(
                "Run {i} out of {total_runs}", i=i + 1, total_runs=NUM_RUNS_PER_ENGINE
            )

            # trino
            trino_results = trino.run_all_queries(
                run_id=i,
                is_warmup=(cold_or_warm == "warm" and i == 0),
                cold_or_warm=cold_or_warm,
            )
            results = pd.concat([results, pd.DataFrame(trino_results)])

            if cold_or_warm == "cold":
                logger.info("Restarting trino and minio containers for cold run")
                docker_client.containers.get(
                    "analytics-on-fhir-benchmark-minio-1"
                ).restart()
                docker_client.containers.get(
                    "analytics-on-fhir-benchmark-trino-1"
                ).restart()
            gc.collect()

            logger.info("Done with trino. Waiting for 30s")
            time.sleep(30)

            # pathling
            # we occasionally observe transient OOM issues, so add retries here
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    pathling_results = pathling.run_all_queries(
                        run_id=i,
                        is_warmup=(cold_or_warm == "warm" and i == 0),
                        cold_or_warm=cold_or_warm,
                    )
                    results = pd.concat([results, pd.DataFrame(pathling_results)])
                    if cold_or_warm == "cold":
                        logger.info("Resetting pathling/spark for cold run")
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

            gc.collect()
            logger.info("Done with pathling. Waiting for 30s")
            time.sleep(30)

            # pyrate Blaze
            pyrate_blaze_results = pyrate_blaze.run_all_queries(
                run_id=i,
                is_warmup=(cold_or_warm == "warm" and i == 0),
                cold_or_warm=cold_or_warm,
            )
            results = pd.concat([results, pd.DataFrame(pyrate_blaze_results)])

            if cold_or_warm == "cold":
                logger.info("Restarting blaze for cold run")
                docker_client.containers.get(
                    "analytics-on-fhir-benchmark-blaze-1"
                ).restart()
            gc.collect()
            logger.info("Done with pyrate Blaze. Waiting for 30s")
            time.sleep(30)

            # pyrate HAPI
            pyrate_hapi_results = pyrate_hapi.run_all_queries(
                run_id=i,
                is_warmup=(cold_or_warm == "warm" and i == 0),
                cold_or_warm=cold_or_warm,
            )
            results = pd.concat([results, pd.DataFrame(pyrate_hapi_results)])

            if cold_or_warm == "cold":
                logger.info("Restarting HAPI Postgres for cold run")
                docker_client.containers.get(
                    "analytics-on-fhir-benchmark-hapi-fhir-postgres-1"
                ).restart()
                logger.info("Restarting HAPI Server for cold run")
                docker_client.containers.get(
                    "analytics-on-fhir-benchmark-hapi-fhir-1"
                ).restart()
            gc.collect()
            logger.info("Done with pyrate HAPI. Waiting for 30s")
            time.sleep(30)

        logger.info("{warm_or_cold} run completed.", warm_or_cold=cold_or_warm)

    logger.info(
        "All benchmarks completed. Failed runs: {failed_run_count}",
        failed_run_count=failed_run_count,
    )
    output_dir = Path.cwd() / "results" / "benchmark-runs"
    output_dir.mkdir(parents=True, exist_ok=True)

    results["benchmark_timestamp"] = benchmark_timestamp

    # append the resource_count_total as a fixed-value column. Makes it easier to later facet by it.
    results["resource_count_total"] = resource_count_total

    results["synthea_population_size"] = os.getenv("SYNTHEA_POPULATION_SIZE", "")

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
