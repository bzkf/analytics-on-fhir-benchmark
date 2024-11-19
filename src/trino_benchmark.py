import datetime
import trino
import pandas as pd
from pathlib import Path
from loguru import logger
import time

from benchmark import Benchmark, BenchmarkRunResult, QueryType


class TrinoBenchmark(Benchmark):
    def __init__(self):
        self.trino_connection = trino.dbapi.connect(
            host="localhost",
            port="8080",
            user="trino",
            catalog="fhir",
            schema="default",
        )
        logger.info("Completed initialization.")

    def run_all_queries(self, run_id: str) -> list[BenchmarkRunResult]:
        logger.info("Begin trino benchmarking")
        queries_base_path = Path.cwd() / "queries"

        results = []
        start_timestamp = datetime.datetime.now(datetime.UTC)

        for query_type in QueryType:
            queries_dir_path = queries_base_path / str(query_type)
            logger.info(
                "Looking for sql files in {queries_dir_path}",
                queries_dir_path=queries_dir_path,
            )

            for file in queries_dir_path.glob("*.sql"):
                query_name = file.stem
                logger.info(
                    "Running {query_type} query {query_name}",
                    query_type=query_type,
                    query_name=query_name,
                )

                output_file_name = f"{query_name}.csv"
                output_folder = Path.cwd() / "results" / "trino" / str(query_type)
                output_folder.mkdir(parents=True, exist_ok=True)
                output_file_path = output_folder / output_file_name

                logger.info(
                    "Output file path set to {output_file_path}",
                    output_file_path=output_file_path,
                )

                query = file.read_text()

                cursor = self.trino_connection.cursor()

                # technically, the query is likely first executed on fetchall
                timings_start = time.perf_counter()
                cursor.execute(query)

                # The DBAPI implementation in trino.dbapi provides methods to retrieve fewer rows for example Cursor.fetchone() or Cursor.fetchmany().
                # By default Cursor.fetchmany() fetches one row. Please set trino.dbapi.Cursor.arraysize accordingly.
                # So this could probably be optimized.
                rows = cursor.fetchall()

                fetch_done_timestamp = time.perf_counter()
                fetch_duration = fetch_done_timestamp - timings_start

                # TODO: try pd.read_sql_query
                df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

                df.to_csv(output_file_path, index=False)

                write_to_file_duration = time.perf_counter() - fetch_done_timestamp

                duration_total = time.perf_counter() - timings_start

                logger.info(
                    "Total duration: {duration_total:0.4f} s",
                    duration_total=duration_total,
                )

                cursor.close()

                result = BenchmarkRunResult(
                    run_id=run_id,
                    start_timestamp=start_timestamp,
                    engine="trino",
                    query=query_name,
                    query_type=query_type,
                    total_duration_seconds=duration_total,
                    write_to_file_duration_seconds=write_to_file_duration,
                    fetch_duration_seconds=fetch_duration,
                    post_process_duration_seconds=0,
                    trino_cpu_time_seconds=cursor.stats["cpuTimeMillis"] / 1000.0,
                    trino_wall_time_seconds=cursor.stats["wallTimeMillis"] / 1000.0,
                    trino_elapsed_time_seconds=cursor.stats["elapsedTimeMillis"]
                    / 1000.0,
                )

                results.append(result)

        return results

    def get_resource_counts_total(self, resource_types: list[str]) -> int:
        cursor = self.trino_connection.cursor()
        total = 0
        for resource_type in resource_types:
            query = (
                f"SELECT COUNT(DISTINCT(id)) AS count FROM fhir.default.{resource_type}"
            )

            cursor.execute(query)
            row = cursor.fetchone()

            total = total + row[0]
        cursor.close()
        return total

    def get_resource_counts(self, resource_types: list[str]) -> dict[str, int]:
        cursor = self.trino_connection.cursor()
        result = {}
        for resource_type in resource_types:
            query = (
                f"SELECT COUNT(DISTINCT(id)) AS count FROM fhir.default.{resource_type}"
            )

            cursor.execute(query)
            row = cursor.fetchone()

            result[resource_type] = row[0]

        cursor.close()

        return result
