"""
Stage 2: build the five analytical outputs from the cleaned Parquet data.

    spark-submit spark/transform_collisions.py

Reads  $PROJECT_DIR/data/processed/cleaned
Writes $PROJECT_DIR/output/{borough_summary, monthly_summary, severity_summary,
                            factor_summary, borough_ranking}   (Parquet)
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_DIR = os.environ.get(
    "PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
INPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed", "cleaned")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")


def save(df, name):
    path = os.path.join(OUTPUT_DIR, name)
    df.write.mode("overwrite").parquet(path)
    print(f"Wrote {name} -> {path}")


def main():
    spark = SparkSession.builder.appName("NYCCollisionAnalytics").getOrCreate()
    df = spark.read.parquet(INPUT_PATH).cache()

    totals = [
        F.count("*").alias("total_crashes"),
        F.sum("total_injured").alias("total_injured"),
        F.sum("number_of_persons_killed").alias("total_killed"),
    ]

    # 1. Borough summary
    borough_summary = (
        df.groupBy("borough")
        .agg(*totals, F.avg("total_injured").alias("average_injured_per_crash"))
        .orderBy(F.desc("total_crashes"))
    )
    borough_summary.show(truncate=False)
    save(borough_summary, "borough_summary")

    # 2. Monthly collision trends
    monthly_summary = (
        df.groupBy("crash_year", "crash_month")
        .agg(*totals)
        .orderBy("crash_year", "crash_month")
    )
    monthly_summary.show(30, truncate=False)
    save(monthly_summary, "monthly_summary")

    # 3. Severity analysis
    severity_summary = (
        df.groupBy("severity").agg(*totals).orderBy(F.desc("total_crashes"))
    )
    severity_summary.show(truncate=False)
    save(severity_summary, "severity_summary")

    # 4. Contributing factors (first recorded factor per collision)
    factor_summary = (
        df.filter(F.col("contributing_factor_vehicle_1").isNotNull())
        .groupBy("contributing_factor_vehicle_1")
        .agg(*totals)
        .orderBy(F.desc("total_crashes"))
    )
    factor_summary.show(20, truncate=False)
    save(factor_summary, "factor_summary")

    # 5. Borough ranking with a window function
    rank_window = Window.orderBy(F.desc("total_crashes"))
    borough_ranking = borough_summary.withColumn(
        "borough_rank", F.row_number().over(rank_window)
    )
    borough_ranking.show(truncate=False)
    save(borough_ranking, "borough_ranking")

    spark.stop()


if __name__ == "__main__":
    main()
