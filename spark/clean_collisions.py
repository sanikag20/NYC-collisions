"""
Stage 1: clean the raw NYC Motor Vehicle Collisions CSV and write it as Parquet.

    spark-submit spark/clean_collisions.py

Paths are resolved from the PROJECT_DIR environment variable (defaults to the repo root):
    input : $PROJECT_DIR/data/raw/collisions.csv
    output: $PROJECT_DIR/data/processed/cleaned   (Parquet)
"""
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = os.environ.get(
    "PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
INPUT_PATH = os.path.join(PROJECT_DIR, "data", "raw", "collisions.csv")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed", "cleaned")

NUMERIC_COLUMNS = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]


def standardize(name: str) -> str:
    """'CONTRIBUTING FACTOR VEHICLE 1' -> 'contributing_factor_vehicle_1'."""
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def main():
    spark = SparkSession.builder.appName("NYCCollisionCleaning").getOrCreate()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )
    print(f"Raw rows: {df.count()}")

    # 1. Column standardization
    df = df.toDF(*[standardize(c) for c in df.columns])
    df = df.dropna(how="all")

    # 2. Date processing: malformed dates become null instead of failing the job
    df = df.withColumn(
        "crash_date",
        F.expr("try_to_date(cast(crash_date as string), 'MM/dd/yyyy')"),
    )
    df = df.filter(F.col("crash_date").isNotNull())

    # 3. Numeric injury/fatality columns: cast and fill missing with 0
    for c in NUMERIC_COLUMNS:
        if c in df.columns:
            df = df.withColumn(
                c, F.coalesce(F.col(c).cast("double"), F.lit(0.0))
            )

    # 4. Missing boroughs are kept and labelled explicitly
    df = df.withColumn(
        "borough",
        F.when(
            F.col("borough").isNull() | (F.trim(F.col("borough")) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(F.upper(F.trim(F.col("borough")))),
    )

    # 5. Derived features
    df = (
        df.withColumn("crash_year", F.year("crash_date"))
        .withColumn("crash_month", F.month("crash_date"))
        .withColumn("total_injured", F.col("number_of_persons_injured"))
        .withColumn(
            "severity",
            F.when(F.col("number_of_persons_killed") > 0, "FATAL")
            .when(F.col("total_injured") >= 3, "SEVERE")
            .when(F.col("total_injured") >= 1, "MODERATE")
            .otherwise("MINOR"),
        )
    )

    print(f"Total cleaned rows: {df.count()}")
    df.printSchema()

    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"Cleaned data written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
