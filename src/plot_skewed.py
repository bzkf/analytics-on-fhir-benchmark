import os
from pathlib import Path
import pandas as pd
import seaborn as sns
from loguru import logger

from benchmark import QueryType

BENCHMARK_CATEGORY = "skewed"


df = pd.DataFrame()

results_dir_path = Path.cwd() / "results" / "benchmark-runs" / BENCHMARK_CATEGORY

for file in results_dir_path.glob("*.csv"):
    if file.stem.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    df = pd.concat([df, pd.read_csv(file)])

df = df[df["query"] != "skewed-mixed-group-by"]
df = df[~df["is_warmup"]]

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

df["query"] = (
    df["query"]
    .astype("category")
    .cat.rename_categories(
        {
            "skewed-rare-codes": "Rare Codes",
            "skewed-hot-codes": "Hot Codes",
            "skewed-mixed-codes": "Mixed Codes",
        }
    )
)

df["record_count_categorical"] = df["synthea_population_size"].astype("category")


logger.info(df)
logger.info(df.dtypes)

results_dir = Path.cwd() / "results"
results_dir.mkdir(parents=True, exist_ok=True)

output_dir = results_dir / "plots" / BENCHMARK_CATEGORY
output_dir.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid", font="sans-serif", context="paper")

df_original = df

query_type = QueryType.COUNT_SKEWED
query_type_str = str(query_type)

(output_dir / query_type_str).mkdir(parents=True, exist_ok=True)

df = df_original[df_original["query_type"] == query_type_str]

print(df)
p95 = (
    df.groupby(["engine", "query"])["total_duration_seconds"]
    .quantile(0.95)
    .reset_index(name="p95_duration_seconds")
)

titles = "{col_name}"
col_order = [
    "Rare Codes",
    "Hot Codes",
    "Mixed Codes",
]
col_wrap = None

g = sns.catplot(
    data=df,
    kind="bar",
    x="engine",
    y="total_duration_seconds",
    col_order=col_order,
    hue="query",
    palette="Set1",
    errorbar=("ci", 95),
    err_kws={"color": ".1", "linewidth": 1},
    capsize=0.3,
    height=6,
    aspect=1.6,
)

g.legend.set_title("Query Type")
g.set_axis_labels("Engine", "Mean duration (seconds)")

# Single axis now
ax = g.ax


# Dummy handle for legend
scatter_for_legend = ax.scatter(
    [], [], facecolors="red", marker="+", label="p95 latency"
)

g.figure.legend(
    handles=[scatter_for_legend],
    labels=["P95 Duration"],
    title="Auxiliary Metrics",
    loc="center right",
    bbox_to_anchor=(1, 0.35),
    frameon=False,
)

for p in ax.patches:
    print(p)
    height_multiplier = 1.95
    ax.text(
        p.get_x() + p.get_width() / 2,
        p.get_height() * height_multiplier,
        f"{p.get_height():.2f}",
        ha="center",
        va="bottom",
        fontsize="small",
        color="black",
    )

print(p95)

# p95["query_type"] = "count_skewed"
all_red_palette = {q: "red" for q in p95["query"].unique()}
sns.stripplot(
    data=p95,
    x="engine",
    y="p95_duration_seconds",
    hue="query",
    dodge=True,  # aligns with bars
    marker="+",
    size=5,
    linewidth=1,
    palette=all_red_palette,
    # color="red",
    ax=ax,
    legend=False,  # avoid duplicate legend
    zorder=10,
)

# Log scale
ax.set_yscale("log")

g.figure.savefig(
    output_dir / query_type_str / "duration-by-resource-count-facetted-by-query.png",
    dpi=300,
)
