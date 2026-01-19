from pathlib import Path
import pandas as pd
import seaborn as sns
from loguru import logger
import matplotlib.pyplot as plt

df = pd.DataFrame()

metrics_dir_path = Path.cwd() / "results" / "import-resource-metrics"

for file in metrics_dir_path.glob("*.csv"):
    if file.name.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    file_df = pd.read_csv(file)
    file_df["synthea_population_size"] = file_df["synthea_population_size"].astype(str)
    df = pd.concat([df, file_df])

df = df[
    df["container"].isin(
        [
            "analytics-on-fhir-benchmark-minio-1",
            "analytics-on-fhir-benchmark-hapi-fhir-1",
            "analytics-on-fhir-benchmark-blaze-1",
            "analytics-on-fhir-benchmark-hapi-fhir-postgres-1",
            # "analytics-on-fhir-benchmark-hive-metastore-1",
            # "analytics-on-fhir-benchmark-metastore-db-1",
            "analytics-on-fhir-benchmark-pathling-1",
            "analytics-on-fhir-benchmark-warehousekeeper-1",
        ]
    )
]

rename_map = {
    "minio": "MinIO",
    "hapi-fhir": "HAPI FHIR Server",
    "hapi-fhir-postgres": "HAPI FHIR PostgreSQL DB",
    "pathling": "Pathling Server",
    "blaze": "Blaze Server",
    "warehousekeeper": "Delta Lake OPTIMIZE & VACUUM",
}

df["container"] = (
    df["container"]
    .str.replace("analytics-on-fhir-benchmark-", "", regex=False)
    .str.replace("-1", "", regex=False)
    .replace(rename_map)
)

# the pathling import and the fhir server import were started manually, so there's
# a random gap between the absolute timestamps for the two kinds of import measurements.
# the code below stitches them together using the "start" column of the population_size
# groups (only 1 run per population size per pathling and fhir servers).
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["start"] = pd.to_datetime(df["start"])
df = df.sort_values(["synthea_population_size", "start", "timestamp"])

df["run_index"] = (
    df.groupby("synthea_population_size")["start"].rank(method="dense").astype(int)
)
df["t_rel"] = df["timestamp"] - df.groupby(["synthea_population_size", "run_index"])[
    "timestamp"
].transform("min")

df["t_rel_sec"] = df["t_rel"].dt.total_seconds()

run_durations = (
    df.groupby(["synthea_population_size", "run_index"])["t_rel_sec"]
    .max()
    .reset_index(name="run_duration")
)

run_durations["offset"] = (
    run_durations.groupby("synthea_population_size")["run_duration"].cumsum()
    - run_durations["run_duration"]
)

df = df.merge(
    run_durations[["synthea_population_size", "run_index", "offset"]],
    on=["synthea_population_size", "run_index"],
    how="left",
)

df["t_stitched"] = df["t_rel_sec"] + df["offset"]

df["t_stitched_norm"] = df.groupby("synthea_population_size")["t_stitched"].transform(
    lambda x: x / x.max()
)

# df["t_rel"] = (
#     df["timestamp"]
#     - df.groupby("synthea_population_size")["timestamp"].transform("min")
# )
# df["t_rel_sec"] = df["t_rel"].dt.total_seconds()
# df["t_norm"] = (
#     df.groupby("synthea_population_size")["t_rel_sec"]
#       .transform(lambda x: x / x.max())
# )

metrics = [
    {"metric": "cpu", "y_axis_label": "CPU Usage (cores)", "factor": 1},
    {
        "metric": "memory_working_set_bytes",
        "y_axis_label": "Memory Usage (GB)",
        "factor": 1 / (1024**3),
    },
]

for metric in metrics:
    plot_df = df[df["metric"] == metric["metric"]].copy()
    plot_df.loc[:, "value_factored"] = plot_df["value"] * metric["factor"]

    order = sorted(plot_df["synthea_population_size"].unique(), key=int)

    g = sns.relplot(
        data=plot_df,
        x="t_stitched_norm",
        y="value_factored",
        hue="container",
        col="synthea_population_size",
        col_order=order,
        col_wrap=2,  # wrap into multiple rows
        kind="line",
        height=4,
        aspect=1.3,
    )

    g.set_titles("Synthea record count: {col_name}")
    g.set_axis_labels("Normalized runtime", metric["y_axis_label"])
    g._legend.set_title("Container")

    sns.move_legend(
        g,
        loc="upper center",
        bbox_to_anchor=(0.43, 1.07),
        bbox_transform=g.figure.transFigure,
        ncol=3,
        frameon=True,
        columnspacing=1,
        handletextpad=0,
    )

    results_dir = Path.cwd() / "results" / "plots" / "import-resource-metrics"
    results_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(results_dir /f"{metric['metric']}-plot.png", dpi=300, bbox_inches="tight")
