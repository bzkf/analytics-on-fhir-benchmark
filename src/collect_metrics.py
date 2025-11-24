import argparse
import requests
import csv
import datetime

PROM_URL = "http://localhost:9090/api/v1/query_range"

# PromQL queries
queries = {
    "cpu": 'rate(container_cpu_usage_seconds_total{image!=""}[10s])',
    "memory_working_set_bytes": 'container_memory_working_set_bytes{image!=""}',
    "memory_rss": 'container_memory_rss{image!=""}',
    "fs_read_bytes": "rate(container_fs_reads_bytes_total[10s])",
    "fs_read_bytes": "rate(container_fs_writes_bytes_total[10s])",
}


def parse_time(t):
    """Allow multiple time formats."""
    try:
        return datetime.datetime.fromisoformat(t.replace("Z", ""))
    except ValueError:
        return datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="Export Prometheus metrics to CSV")
    parser.add_argument(
        "--start", required=True, help="Start time (ISO8601), e.g. 2025-01-23T10:00:00Z"
    )
    parser.add_argument(
        "--end", required=False, help="End time (ISO8601). Default = now."
    )
    parser.add_argument(
        "--step", default="5s", help="Query resolution step, e.g. 5s or 1m"
    )
    parser.add_argument(
        "--output", default="container_metrics.csv", help="CSV output file"
    )
    parser.add_argument(
        "--population-size", required=True, help="The population size for the run"
    )

    args = parser.parse_args()

    start_dt = parse_time(args.start)
    end_dt = (
        parse_time(args.end)
        if args.end
        else datetime.datetime.now(datetime.timezone.utc)
    )

    params = {
        "start": start_dt.timestamp(),
        "end": end_dt.timestamp(),
        "step": args.step,
        "population_size": args.population_size,
    }

    rows = []

    for metric_name, query in queries.items():
        params["query"] = query
        r = requests.get(PROM_URL, params=params).json()

        if r["status"] != "success":
            print(f"Query failed for metric {metric_name}: {r}")
            continue

        for result in r["data"]["result"]:
            container = result["metric"].get(
                "name", result["metric"].get("container", "unknown")
            )
            for ts, value in result["values"]:
                rows.append(
                    {
                        "timestamp": ts,
                        "container": container,
                        "metric": metric_name,
                        "value": value,
                        "synthea_population_size": args.population_size,
                    }
                )

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "container",
                "metric",
                "value",
                "synthea_population_size",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {args.output}")


if __name__ == "__main__":
    main()
