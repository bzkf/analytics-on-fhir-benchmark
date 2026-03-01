from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from enum import Enum

class QueryType(Enum):
    AGGREGATE = "aggregate"
    COUNT = "count"
    EXTRACT = "extract"
    COUNT_SKEWED = "count-skewed"
    JOIN_COUNT_SKEWED = "join-count-skewed"

    def __str__(self):
        return str(self.value).lower()

# QUERY_TYPES_TO_RUN =  [QueryType.EXTRACT, QueryType.AGGREGATE, QueryType.COUNT]

QUERY_TYPES_TO_RUN = [QueryType.EXTRACT, QueryType.COUNT]

class QueryEngine(Enum):
    PATHLING = "pathling"
    PYRATE = "pyrate"
    TRINO = "trino"

    def __str__(self):
        return str(self.value).lower()


@dataclass
class BenchmarkRunResult:
    run_id: int
    start_timestamp: datetime.datetime
    engine: str
    query: str
    query_type: QueryType
    total_duration_seconds: float
    write_to_file_duration_seconds: float
    fetch_duration_seconds: float
    post_process_duration_seconds: float = 0
    trino_cpu_time_seconds: float = 0
    trino_wall_time_seconds: float = 0
    trino_elapsed_time_seconds: float = 0
    is_warmup: bool = False
    cold_or_warm: str = "cold"


class Benchmark(ABC):
    @abstractmethod
    def run_all_queries(self, run_id : int) -> list[BenchmarkRunResult]:
        pass
