import os
from pathlib import Path
import pandas as pd
import seaborn as sns
from loguru import logger

from benchmark import QueryType

BENCHMARK_CATEGORY = "skewed"

FACETS = [
    "gender-age",
    "diabetes",
    "hemoglobin"
]


def compute_relative_duration(group):
    # Find the duration for the FHIR-PYrate engine as the reference
    reference_row = group[group["engine"] == "FHIR-PYrate (Blaze)"]
    if not reference_row.empty:
        reference_duration = reference_row["total_duration_seconds"].values[0]
    else:
        raise ValueError("reference row not found!")

    # Calculate relative duration by dividing each duration by the reference
    group["relative_duration"] = reference_duration / group["total_duration_seconds"]
    return group


df = pd.DataFrame()

results_dir_path = Path.cwd() / "results" / "benchmark-runs" / BENCHMARK_CATEGORY

for file in results_dir_path.glob("*.csv"):
    if file.stem.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    df = pd.concat([df, pd.read_csv(file)])

df["resource_count_total_categorical"] = df["resource_count_total"].astype("category")
df["engine"] = (
    df["engine"]
    .astype("category")
    .cat.rename_categories(
        {
            "pyrate-hapi": "FHIR-PYrate (HAPI)",
            "pyrate-blaze": "FHIR-PYrate (Blaze)",
            "pathling": "Pathling",
            "trino": "Trino",
        }
    )
)
df["query_type"] = df["query_type"].astype("category")

df["record_count_categorical"] = df["synthea_population_size"].astype("category")

# exclude any warmup runs
df = df[~df["is_warmup"]]


logger.info(df)
logger.info(df.dtypes)

results_dir = Path.cwd() / "results"
results_dir.mkdir(parents=True, exist_ok=True)

# added to make it work in the GitHub workflow that only uses a much smaller record size
if not os.getenv("SKIP_RELATIVE_PERFORMANCE_COMPARISON", False):
    # this results in some NA values in the means since `observations-by-code` is
    # only used in the `aggregate` scenario and in turn all other scenarios
    # are not ran against the `observations-by-code` query.
    df[df["record_count_categorical"] == 100000].groupby(
        ["engine", "query_type", "query", "record_count_categorical"]
    )["total_duration_seconds"].mean().dropna().drop(
        columns=("record_count_categorical")
    ).to_csv(
        results_dir / "summary-largest-record-count.csv"
    )

    means_largest_record_count = pd.read_csv(
        results_dir / "summary-largest-record-count.csv"
    )

    logger.info(means_largest_record_count)

    # Apply the function to each (query, query_type) group
    relative_data = means_largest_record_count.groupby(["query", "query_type"]).apply(
        compute_relative_duration
    )

    # Sort and display
    relative_data = relative_data.sort_values(by=["query", "query_type", "engine"])

    logger.info(relative_data)

    mean_relative_performance = (
        relative_data.groupby(["engine", "query_type"])["relative_duration"]
        .mean()
        .dropna()
    )

    logger.info(mean_relative_performance)

output_dir = results_dir / "plots" / BENCHMARK_CATEGORY
output_dir.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid", font="sans-serif", context="paper")

df_original = df

for query_type in [QueryType.COUNT_SKEWED]: # QueryType.EXTRACT, QueryType.COUNT, QueryType.AGGREGATE,
    query_type_str = str(query_type)

    (output_dir / query_type_str).mkdir(parents=True, exist_ok=True)

    df = df_original[df_original["query_type"] == query_type_str]

    p95 = (
        df.groupby(["engine", "query", "record_count_categorical"])[
            "total_duration_seconds"
        ]
        .quantile(0.95)
        .reset_index(name="p95_duration_seconds")
    )

    col_order = FACETS
    titles = "{col_name}"
    col_wrap = 2

    if query_type == QueryType.AGGREGATE:
        col_order = ["observations-by-code"]
        titles = ""
        col_wrap = None

    if query_type == QueryType.COUNT_SKEWED:
        col_order = [
            "skewed-rare-codes",
            "skewed-mixed-codes",
            "skewed-hot-codes",
            # "skewed-mixed-group-by",
        ]
        col_wrap = None

    g = sns.catplot(
        data=df,
        kind="bar",
        x="record_count_categorical",
        y="total_duration_seconds",
        hue="engine",
        col="query",
        palette="Set2",
        errorbar=("ci", 95),
        col_order=col_order,
        err_kws={"color": ".1", "linewidth": 1},
        capsize=0.3,
    )

    g.legend.set_title("Query Engine")
    g.set_titles(titles)
    g.set_axis_labels("Record Count", "Mean duration (seconds)")

    # Pick any axis (e.g., first facet)
    ax = g.axes.flat[0]

    # Create a dummy scatter for the legend
    scatter_for_legend = ax.scatter(
        [], [], facecolors="red", marker="+", label="p95 latency"
    )

    g.figure.legend(
        handles=[scatter_for_legend],
        labels=["P95 Duration"],
        title="Auxiliary Metrics",
        loc="center right",  # relative to the figure
        bbox_to_anchor=(1, 0.35),  # move outside the figure (x > 1)
        frameon=False,
    )

    for facet in g.axes_dict.keys():
        ax = g.axes_dict[facet]
        logger.info("{facet}, {ax}", facet=facet, ax=ax)
        ax.set_yscale("log")

        d = p95[p95["query"] == facet]

        handles, labels = ax.get_legend_handles_labels()

        for p in ax.patches:
            height_multiplier = 1.5 if query_type == QueryType.COUNT else 2
            ax.text(
                p.get_x() + 0.01,
                p.get_height() * height_multiplier,  # 0.015,
                "{0:.2f}".format(p.get_height()),
                color="black",
                rotation="horizontal",
                size="small",
            )

        for bar, error_line in zip(ax.patches, ax.lines):
            # Extract bar and error bar positions
            x = bar.get_x() + bar.get_width() / 2
            y = bar.get_height()

            # Extract CI from error bars
            error_y = error_line.get_ydata()
            ci_lower, ci_upper = error_y[0], error_y[-1]

            # Format the CI label
            ci_label = f"({ci_lower:.1f}, {ci_upper:.1f})"
            # ax.text(
            #     x,
            #     y * 1.05,
            #     ci_label,
            #     ha="center",
            #     va="bottom",
            #     color="black",
            #     size="small",
            # )

        for bar, (_, row) in zip(ax.patches, d.iterrows()):
            x = bar.get_x() + bar.get_width() / 2  # center of the bar
            y = row["p95_duration_seconds"]
            #           ax.scatter(x, y, color="red", s=80, zorder=10)
            ax.scatter(x, y, facecolors="red", marker="+", linewidths=1)

    g.figure.savefig(
        output_dir
        / query_type_str
        / "duration-by-resource-count-facetted-by-query.png",
        dpi=300,
    )
