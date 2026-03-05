import datetime
import os
import sys
import time
from fhir_pyrate import Ahoy, Pirate
from pathlib import Path
from loguru import logger
from pandas import DataFrame

from benchmark import Benchmark, BenchmarkRunResult, QueryType

PAGE_SIZE: int = 1_000

def main() -> int:
    os.environ["FHIR_USER"] = "any"
    os.environ["FHIR_PASSWORD"] = "any"

    auth = Ahoy(auth_type="BasicAuth", auth_method="env")

    search = Pirate(
        auth=auth,
        base_url="http://localhost:8084/fhir",
        print_request_url=True,
    )

    df = search.steal_bundles_to_dataframe(
        resource_type="Observation",
        request_params={
            "_count": PAGE_SIZE,
            "_sort": "_id",
        },
        fhir_paths=[
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
    )
    agg_df = df.astype(str).groupby(["display", "code", "code_system"]).size().reset_index(name="num_observations").sort_values(by="num_observations", ascending=False)
    print(agg_df)
    agg_df.to_csv("pyrate-hapi-agg.csv")
    total_count = agg_df["num_observations"].sum()
    print(f"Total count: {total_count}")
    return 0

if __name__ == "__main__":
    sys.exit(main())