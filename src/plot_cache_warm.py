import os
from pathlib import Path
import pandas as pd
import seaborn as sns
from loguru import logger

from benchmark import QueryType

BENCHMARK_CATEGORY = "skewed"


df = pd.DataFrame()

all_results_dir_path = Path.cwd() / "results" / "benchmark-runs" / "all-engines"

for file in all_results_dir_path.glob("*.csv"):
    if file.stem.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    df_to_add = pd.read_csv(file)
    df_to_add["is_cache_warm"] = False
    df = pd.concat([df, df_to_add])
    df = df[~df["is_warmup"]]

df = df[df["synthea_population_size"] == 100000]

warmed_results_dir_path = Path.cwd() / "results" / "benchmark-runs" / "warmed"

for file in warmed_results_dir_path.glob("*.csv"):
    if file.stem.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    df_to_add = pd.read_csv(file)
    df_to_add["is_cache_warm"] = True
    df = pd.concat([df, df_to_add])
    df = df[~df["is_warmup"]]

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

df_original = df[df["query"] != "hemoglobin-simple"]

for query_type in [QueryType.EXTRACT, QueryType.COUNT]:
    query_type_str = str(query_type)

    output_dir = (
        Path.cwd() / "results" / "plots" / "cache-warm-vs-cold" / query_type_str
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    df = df_original[df_original["query_type"] == query_type_str]

    logger.info(df)

    sns.set_theme(style="whitegrid", font="sans-serif", context="paper")

    p95 = (
        df.groupby(["engine", "query", "is_cache_warm"])["total_duration_seconds"]
        .quantile(0.95)
        .reset_index(name="p95_duration_seconds")
    )

    logger.info(p95)

    g = sns.catplot(
        data=df,
        kind="bar",
        x="engine",
        y="total_duration_seconds",
        hue="is_cache_warm",
        col="query",
        errorbar=("ci", 95),
        err_kws={"color": ".1", "linewidth": 1},
        col_order=["gender-age", "diabetes", "hemoglobin"],
        capsize=0.3,
        height=4,
        aspect=1,
    )

    g.legend.set_title("Cache warmed")
    g.set_axis_labels("Engine", "Mean duration (seconds)")
    g.set_titles("{col_name}")

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
        ax.set_yscale("log")

        for p in ax.patches:
            height_multiplier = 1.32
            ax.text(
                p.get_x() + p.get_width() / 2,
                p.get_height() * height_multiplier,
                f"{p.get_height():.2f}",
                ha="center",
                va="bottom",
                fontsize="small",
                color="black",
            )

        d = p95[p95["query"] == facet]

        all_red_palette = {q: "red" for q in d["is_cache_warm"].unique()}
        sns.stripplot(
            data=d,
            x="engine",
            y="p95_duration_seconds",
            hue="is_cache_warm",
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

    g.figure.savefig(
        output_dir / "duration-by-resource-count-facetted-by-query.png",
        dpi=300,
    )
