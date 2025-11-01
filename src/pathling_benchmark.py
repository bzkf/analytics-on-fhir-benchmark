import datetime
import os
import time
from pathling import PathlingContext, Expression as exp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count_distinct
from loguru import logger
from pathlib import Path

from benchmark import Benchmark, BenchmarkRunResult, QueryType


class PathlingBenchmark(Benchmark):
    def __init__(self):
        self._init_pc()
        logger.info("Completed initialization.")

    def _init_pc(self):
        spark = (
            SparkSession.builder.config(
                "spark.jars.packages",
                # "au.csiro.pathling:library-runtime:7.2.0,io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4",
                "au.csiro.pathling:library-runtime:9.0.0,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1"
            )
            .config("fs.s3a.access.key", "admin")
            .config("fs.s3a.secret.key", "miniopass")
            .config("aws.accessKeyId", "admin")
            .config("aws.secretAccessKey", "miniopass")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.driver.memory",
                os.getenv("SPARK_DRIVER_MEMORY", "48g"),
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                "localhost:9000",
            )
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false",
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true",
            )
            .config("spark.local.dir", Path.cwd() / "spark-tmp")
            .getOrCreate()
        )

        self.pc = PathlingContext.create(
            spark, enable_delta=True, enable_terminology=False
        )

    def run_all_queries(self, run_id: str) -> list[BenchmarkRunResult]:
        output_folder_base = Path.cwd() / "results" / "pathling"

        data = self.pc.read.delta("s3a://fhir/default")

        results = []
        queries = {
            QueryType.EXTRACT: [
                {
                    "query_name": "gender-age",
                    "resource_type": "Patient",
                    "columns": [
                        {
                            "column": [
                                {
                                    "path": "Patient.id",
                                    "name": "patient_id",
                                },
                                {
                                    "path": "Patient.birthDate",
                                    "name": "patient_birthdate",
                                },
                                {
                                    "path": "Patient.gender",
                                    "name": "patient_gender",
                                },
                            ],
                        }
                    ],
                    "filters": [
                        {
                            "path": "Patient.gender = 'female' and Patient.birthDate >= @1970-01-01",
                        },
                    ],
                },
                {
                    "query_name": "diabetes",
                    "resource_type": "Condition",
                    "columns": [  # = select
                        {
                            "column": [
                                {
                                    "path": "Condition.id",
                                    "name": "condition_id",
                                },
                                {
                                    "path": "Condition.subject.reference",
                                    "name": "patient_id",
                                },
                                {
                                    "path": "Condition.code.coding.where(system='http://snomed.info/sct' and (code='73211009' or code='427089005' or code='44054006')).code",
                                    "name": "condition_snomed_code",
                                },
                                {
                                    "path": "onset.ofType(dateTime)",
                                    "name": "condition_onset",
                                },
                            ],
                        }
                    ],
                    "filters": [  # = where
                        {
                            "path": "Condition.code.coding.where(system='http://snomed.info/sct' and (code='73211009' or code='427089005' or code='44054006')).exists()",
                        },
                    ],
                },
                {
                    "query_name": "hemoglobin",
                    "resource_type": "Observation",
                    "columns": [
                        {
                            "column": [
                                {
                                    "path": "Observation.subject.reference",
                                    "name": "patient_id",
                                },
                                {
                                    "path": "Observation.id",
                                    "name": "observation_id",
                                },
                                {
                                    "path": "Observation.code.coding.where(system = 'http://loinc.org').code",
                                    "name": "loinc_code",
                                },
                                {
                                    "path": "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').code",
                                    "name": "value_quantity_ucum_code",
                                },
                                {
                                    "path": "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').value",
                                    "name": "value_quantity_value",
                                },
                                {
                                    "path": "Observation.effectiveDateTime",
                                    "name": "effective_datetime",
                                },
                            ],
                        }
                    ],
                    "filters": [  # = where
                        {
                            "path": "Observation.exists((code.coding.exists(system='http://loinc.org' and code='718-7') and valueQuantity.exists(system='http://unitsofmeasure.org' and code='g/dL') and valueQuantity.value > 25) "
                            + "or (code.coding.exists(system='http://loinc.org' and (code='17856-6' or code='4548-4' or code='4549-2')) and valueQuantity.exists(system='http://unitsofmeasure.org' and code='%') and valueQuantity.value > 5))",
                        },
                    ],
                },
            ],
            # QueryType.AGGREGATE: [
            #     {
            #         "query_name": "observations-by-code",
            #         "resource_type": "Observation",
            #         "aggregations": [exp("count()", "num_observations")],
            #         "groupings": [
            #             exp("code.coding", "coding"),
            #         ],
            #         "filters": [],
            #         "order_by": "num_observations",
            #     },
            # ],
            QueryType.COUNT: [
                {
                    "query_name": "gender-age",
                },
                {
                    "query_name": "diabetes",
                },
                {
                    "query_name": "hemoglobin",
                },
            ],
        }

        start_timestamp = datetime.datetime.now(datetime.UTC)

        for query_type in [QueryType.COUNT, QueryType.EXTRACT]:
            output_folder = output_folder_base / str(query_type)
            output_folder.mkdir(parents=True, exist_ok=True)

            queries_of_type = queries[query_type]
            start = run_id % len(queries_of_type)
            round_robin_queries = queries_of_type[start:] + queries_of_type[:start]

            for query in round_robin_queries:
                query_name = query["query_name"]
                logger.info(
                    "Running {query_type} query {query_name}",
                    query_type=query_type,
                    query_name=query_name,
                )
                timings_start = time.perf_counter()

                df: DataFrame = None

                if query_type == QueryType.AGGREGATE:
                    # df = data.aggregate(
                    #     resource_type=query["resource_type"],
                    #     aggregations=query["aggregations"],
                    #     groupings=query["groupings"],
                    #     filters=query["filters"],
                    # )
                    # df = df.orderBy(query["order_by"], ascending=False).select(
                    #     "coding.display",
                    #     "coding.code",
                    #     "coding.system",
                    #     "num_observations",
                    # )
                    print("query type AGGREGATE not implemented")
                else:
                    # re-use the query with the same name in the list of "extract" queries
                    if query_type == QueryType.COUNT:
                        query = [
                            q
                            for q in queries[QueryType.EXTRACT]
                            if q["query_name"] == query_name
                        ][0]

                    df = data.view(
                        resource=query["resource_type"],
                        select=query["columns"],
                        where=query["filters"],
                    )

                    if query_type == QueryType.COUNT:
                        # in case of the diabetes query, count the condition id, not the patient id
                        if query_name == "diabetes":
                            df = df.agg(count_distinct("condition_id"))
                        else:
                            df = df.agg(count_distinct("patient_id"))
                    else:
                        df = df.orderBy("patient_id", ascending=True)

                df.write.option("header", "true").format("csv").mode("overwrite").save(
                    (output_folder / f"{query_name}.csv").as_posix()
                )

                duration_total = time.perf_counter() - timings_start

                result = BenchmarkRunResult(
                    run_id=run_id,
                    start_timestamp=start_timestamp,
                    engine="pathling",
                    query=query_name,
                    query_type=query_type,
                    total_duration_seconds=duration_total,
                    write_to_file_duration_seconds=0,
                    fetch_duration_seconds=0,
                    post_process_duration_seconds=0,
                )
                results.append(result)

                df.unpersist(blocking=True)

        return results

    def reset(self):
        self.pc.spark.catalog.clearCache()
        self.pc.spark.sparkContext._jvm.System.gc()
        self.pc.spark.stop()
        self._init_pc()
