import os
from pathlib import Path
import pandas as pd
import seaborn as sns
from loguru import logger

from benchmark import QueryType

df = pd.DataFrame()

results_dir_path = Path.cwd() / "results" / "import-resource-metrics"

for file in results_dir_path.glob("*.csv"):
    if file.stem.startswith("_"):
        logger.info("Skipping {file}", file=file)
        continue

    logger.info("Adding {file} to dataset", file=file)
    df = pd.concat([df, pd.read_csv(file)])

df = df[df["metric"] == "cpu"]

# Plot the responses for different events and regions
sns.lineplot(x="timestamp", y="value", hue="container", data=df)
