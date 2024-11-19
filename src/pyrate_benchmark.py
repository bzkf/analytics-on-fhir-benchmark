import datetime
import os
import time
from fhir_pyrate import Ahoy, Pirate
from pathlib import Path
from loguru import logger
from pandas import DataFrame
import pandas as pd

from benchmark import Benchmark, BenchmarkRunResult, QueryType

PAGE_SIZE: int = 10000


class PyrateBenchmark(Benchmark):
    def __init__(self):
        os.environ["FHIR_USER"] = "any"
        os.environ["FHIR_PASSWORD"] = "any"

        auth = Ahoy(auth_type="BasicAuth", auth_method="env")

        self.search = Pirate(
            auth=auth,
            base_url="http://localhost:8083/fhir/",
            print_request_url=False,  # TODO: useful for debugging
        )
        logger.info("Completed initialization.")

    def run_all_queries(self, run_id: str) -> list[BenchmarkRunResult]:
        output_folder_base = Path.cwd() / "results" / "pyrate"

        results = []
        queries = {
            QueryType.EXTRACT: [
                {
                    "query_name": "gender-age",
                    "resource_type": "Patient",
                    "request_params": {
                        "birthdate": "ge1970-01-01",
                        "gender": "female",
                        "_count": PAGE_SIZE,
                        "_sort": "_id",
                    },
                    "fhir_paths": [
                        ("patient_id", "Patient.id"),
                        ("patient_birthdate", "Patient.birthDate"),
                        ("patient_gender", "Patient.gender"),
                    ],
                    "post_process": None,
                },
                {
                    "query_name": "diabetes",
                    "resource_type": "Condition",
                    "request_params": {
                        "encounter.date": "ge2020-01-01",
                        "code": "http://snomed.info/sct|73211009,http://snomed.info/sct|427089005,http://snomed.info/sct|44054006",
                        "subject:Patient.birthdate": "ge1970-01-01",
                        "_include": "Condition:encounter",
                        "_include": "Condition:patient",
                        "_count": PAGE_SIZE,
                        "_sort": "_id",
                    },
                    "fhir_paths": [
                        ("condition_id", "Condition.id"),
                        (
                            "condition_snomed_code",
                            "Condition.code.coding.where(system = 'http://snomed.info/sct' and (code = '73211009' or code = '427089005' or code = '44054006')).code",
                        ),
                        ("condition_onset", "Condition.onsetDateTime"),
                        ("condition_patient_reference", "Condition.subject.reference"),
                        ("encounter_id", "Encounter.id"),
                        ("encounter_patient_reference", "Encounter.subject.reference"),
                        ("encounter_period_start", "Encounter.period.start"),
                        ("encounter_period_end", "Encounter.period.end"),
                        ("encounter_status", "Encounter.status"),
                        ("patient_id", "Patient.id"),
                        ("patient_birthdate", "Patient.birthDate"),
                    ],
                    "post_process": None,
                },
                {
                    "query_name": "hemoglobin",
                    "resource_type": "Observation",
                    "request_params": {
                        "code-value-quantity": "http://loinc.org|718-7$gt25|http://unitsofmeasure.org|g/dL,http://loinc.org|17856-6$gt5|http://unitsofmeasure.org|%,http://loinc.org|4548-4$gt5|http://unitsofmeasure.org|%,http://loinc.org|4549-2$gt5|http://unitsofmeasure.org|%",
                        "_include": "Observation:patient",
                        "_count": PAGE_SIZE,
                        "_sort": "_id",
                    },
                    "fhir_paths": [
                        ("patient_id", "Patient.id"),
                        ("patient_birthdate", "Patient.birthDate"),
                        ("observation_id", "Observation.id"),
                        (
                            "loinc_code",
                            "Observation.code.coding.where(system = 'http://loinc.org').code",
                        ),
                        (
                            "value_quantity_ucum_code",
                            "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').code",
                        ),
                        (
                            "value_quantity_value",
                            "Observation.valueQuantity.where(system = 'http://unitsofmeasure.org').value",
                        ),
                        ("effective_datetime", "Observation.effectiveDateTime"),
                        (
                            "observation_patient_reference",
                            "Observation.subject.reference",
                        ),
                    ],
                    "post_process": None,
                },
            ],
            QueryType.AGGREGATE: [
                {
                    "query_name": "observations-by-code",
                    "resource_type": "Observation",
                    "request_params": {
                        "_count": PAGE_SIZE,
                        "_sort": "_id",
                    },
                    "fhir_paths": [
                        (
                            "display",
                            "Observation.code.coding.display",
                        ),
                        (
                            "code",
                            "Observation.code.coding.code",
                        ),
                        (
                            "code_system",
                            "Observation.code.coding.system",
                        ),
                    ],
                    "post_process": lambda df: self._post_process_observations_by_code(
                        df
                    ),
                },
            ],
            QueryType.COUNT: [
                {
                    "query_name": "gender-age",
                    "resource_type": "Patient",
                    "request_params": {
                        "birthdate": "ge1970-01-01",
                        "gender": "female",
                        "_summary": "count",
                        "_sort": "_id",
                    },
                    "fhir_paths": [],
                    "post_process": None,
                },
                {
                    "query_name": "diabetes",
                    "resource_type": "Condition",
                    "request_params": {
                        "encounter.date": "ge2020-01-01",
                        "code": "http://snomed.info/sct|73211009,http://snomed.info/sct|427089005,http://snomed.info/sct|44054006",
                        "subject:Patient.birthdate": "ge1970-01-01",
                        "_summary": "count",
                        "_sort": "_id",
                    },
                    "fhir_paths": [],
                    "post_process": None,
                },
                {
                    "query_name": "hemoglobin",
                    "resource_type": "Patient",
                    "request_params": {
                        "_has:Observation:patient:code-value-quantity": "http://loinc.org|718-7$gt25|http://unitsofmeasure.org|g/dL,http://loinc.org|17856-6$gt5|http://unitsofmeasure.org|%,http://loinc.org|4548-4$gt5|http://unitsofmeasure.org|%,http://loinc.org|4549-2$gt5|http://unitsofmeasure.org|%",
                        "_summary": "count",
                        "_sort": "_id",
                    },
                    "fhir_paths": [],
                    "post_process": None,
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

                if query_type == QueryType.COUNT:
                    # special handling for the count case
                    count = self.search.get_bundle_total(
                        resource_type=query["resource_type"],
                        request_params=query["request_params"],
                    )
                    df = DataFrame(data={"count": [count]})
                else:
                    df = self.search.steal_bundles_to_dataframe(
                        resource_type=query["resource_type"],
                        request_params=query["request_params"],
                        fhir_paths=query["fhir_paths"],
                    )

                fetch_done_timestamp = time.perf_counter()
                fetch_duration = fetch_done_timestamp - timings_start

                post_process_duration = 0
                if query["post_process"] is not None:
                    post_process_start = time.perf_counter()
                    df = query["post_process"](df)
                    post_process_duration = time.perf_counter() - post_process_start

                write_to_file_start = time.perf_counter()
                if isinstance(df, DataFrame):
                    df.to_csv(output_folder / f"{query_name}.csv", index=False)
                else:
                    for resource_type in df.keys():
                        df[resource_type].to_csv(
                            output_folder / f"{query_name}-{resource_type}.csv",
                            index=False,
                        )

                write_to_file_duration = time.perf_counter() - write_to_file_start
                duration_total = time.perf_counter() - timings_start

                result = BenchmarkRunResult(
                    run_id=run_id,
                    start_timestamp=start_timestamp,
                    engine="pyrate",
                    query=query_name,
                    query_type=query_type,
                    total_duration_seconds=duration_total,
                    write_to_file_duration_seconds=write_to_file_duration,
                    fetch_duration_seconds=fetch_duration,
                    post_process_duration_seconds=post_process_duration,
                )
                results.append(result)

        return results

    def _post_process_observations_by_code(self, df: DataFrame):
        if not isinstance(df, DataFrame):
            logger.warning(
                "fetch result is not a DataFrame. Instead: {df_type}",
                df_type=type(df),
            )
            return df

        processed = (
            df.astype(str)
            .groupby(["display", "code", "code_system"])
            .size()
            .reset_index(name="num_observations")
            .sort_values(by="num_observations", ascending=False)
        )
        return processed
