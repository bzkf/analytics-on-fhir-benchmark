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
                "au.csiro.pathling:library-runtime:7.0.1,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4",
            )
            .config("fs.s3a.access.key", "admin")
            .config("fs.s3a.secret.key", "miniopass")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.driver.memory",
                os.getenv("SPARK_DRIVER_MEMORY", "64g"),
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
                        exp("Patient.id", "patient_id"),
                        exp("Patient.birthDate", "patient_birthdate"),
                        exp("Patient.gender", "patient_gender"),
                    ],
                    "filters": [
                        "Patient.gender = 'female' and Patient.birthDate >= @1970-01-01"
                    ],
                },
                {
                    "query_name": "diabetes",
                    "resource_type": "Condition",
                    "columns": [
                        exp("Condition.id", "condition_id"),
                        exp(
                            "Condition.code.coding.where(system='http://snomed.info/sct' and (code='73211009' or code='427089005' or code='44054006')).code",
                            "condition_snomed_code",
                        ),
                        exp("Condition.onsetDateTime", "condition_onset"),
                        exp("Condition.encounter.resolve().id", "encounter_id"),
                        exp(
                            "Condition.encounter.resolve().period.start",
                            "encounter_period_start",
                        ),
                        exp(
                            "Condition.encounter.resolve().period.end",
                            "encounter_period_end",
                        ),
                        exp("Condition.encounter.resolve().status", "encounter_status"),
                        exp(
                            "Condition.subject.resolve().ofType(Patient).id",
                            "patient_id",
                        ),
                        exp(
                            "Condition.subject.resolve().ofType(Patient).birthDate",
                            "patient_birthdate",
                        ),
                    ],
                    "filters": [
                        "Condition.code.coding.where(system='http://snomed.info/sct' and (code='73211009' or code='427089005' or code='44054006')).exists()",
                        "Condition.encounter.resolve().period.start >= @2020-01-01",
                        "Condition.subject.resolve().ofType(Patient).birthDate >= @1970-01-01",
                    ],
                },
                {
                    "query_name": "hemoglobin",
                    "resource_type": "Observation",
                    "columns": [
                        exp(
                            "Observation.subject.resolve().ofType(Patient).id",
                            "patient_id",
                        ),
                        exp(
                            "Observation.subject.resolve().ofType(Patient).birthDate",
                            "patient_birthdate",
                        ),
                        exp("Observation.id", "observation_id"),
                        exp(
                            "Observation.code.coding.where(system = 'http://loinc.org').code",
                            "loinc_code",
                        ),
                        exp(
                            "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').code",
                            "value_quantity_ucum_code",
                        ),
                        exp(
                            "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').value",
                            "value_quantity_value",
                        ),
                        exp("Observation.effectiveDateTime", "effective_datetime"),
                        exp(
                            "Observation.subject.reference",
                            "observation_patient_reference",
                        ),
                    ],
                    "filters": [
                        "Observation.exists((code.coding.exists(system='http://loinc.org' and code='718-7') and valueQuantity.exists(system='http://unitsofmeasure.org' and code='g/dL') and valueQuantity.value > 25) "
                        + "or (code.coding.exists(system='http://loinc.org' and (code='17856-6' or code='4548-4' or code='4549-2')) and valueQuantity.exists(system='http://unitsofmeasure.org' and code='%') and valueQuantity.value > 5))",
                    ],
                },
            ],
            QueryType.AGGREGATE: [
                {
                    "query_name": "observations-by-code",
                    "resource_type": "Observation",
                    "aggregations": [exp("count()", "num_observations")],
                    "groupings": [
                        exp("code.coding", "coding"),
                    ],
                    "filters": [],
                    "order_by": "num_observations",
                },
            ],
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

        for query_type in QueryType:
            output_folder = output_folder_base / str(query_type)
            output_folder.mkdir(parents=True, exist_ok=True)

            for query in queries[query_type]:
                query_name = query["query_name"]
                logger.info(
                    "Running {query_type} query {query_name}",
                    query_type=query_type,
                    query_name=query_name,
                )
                timings_start = time.perf_counter()

                df: DataFrame = None

                if query_type == QueryType.AGGREGATE:
                    df = data.aggregate(
                        resource_type=query["resource_type"],
                        aggregations=query["aggregations"],
                        groupings=query["groupings"],
                        filters=query["filters"],
                    )
                    df = df.orderBy(query["order_by"], ascending=False).select(
                        "coding.display",
                        "coding.code",
                        "coding.system",
                        "num_observations",
                    )
                else:
                    # re-use the query with the same name in the list of "extract" queries
                    if query_type == QueryType.COUNT:
                        query = [
                            q
                            for q in queries[QueryType.EXTRACT]
                            if q["query_name"] == query_name
                        ][0]

                    df = data.extract(
                        resource_type=query["resource_type"],
                        columns=query["columns"],
                        filters=query["filters"],
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
