"""SparkSession factory and small DataFrame helpers shared by the jobs."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import SILVER_DIR


def get_spark(app_name: str, packages: str = "") -> SparkSession:
    """Return a local SparkSession configured for the project."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions",
                os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.adaptive.enabled", "true")
        # Only used when the script is started with plain `python`
        # (spark-submit sets the driver memory itself).
        .config("spark.driver.memory",
                os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
    )
    if packages:  # ignored by spark-submit, which uses --packages
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    """Read a cleaned table produced by 02_data_quality.py."""
    return spark.read.parquet(str(SILVER_DIR / table))


def feature_movies(spark: SparkSession, min_runtime: int) -> DataFrame:
    """Feature-length, non-adult movies joined with their rating (if any).

    Columns: tconst, primaryTitle, originalTitle, year, runtimeMinutes,
    genres (array), averageRating, numVotes.
    """
    basics = read_silver(spark, "title_basics")
    ratings = read_silver(spark, "title_ratings").select(
        "tconst", "averageRating", "numVotes"
    )
    return (
        basics.filter(
            (F.col("titleType") == "movie")
            & (F.col("isAdult") == 0)
            & (F.col("runtimeMinutes") > min_runtime)
        )
        .select(
            "tconst", "primaryTitle", "originalTitle",
            F.col("startYear").alias("year"), "runtimeMinutes", "genres",
        )
        .join(ratings, "tconst", "left")
    )


def normalize_text(col):
    """Lower-case, strip accents-insensitive punctuation and collapse spaces.

    Used to match titles / names across sources (IMDb, Wikipedia).
    """
    c = F.lower(F.trim(col))
    c = F.regexp_replace(c, r"[’']", "'")
    c = F.regexp_replace(c, r"[^\p{L}\p{N}' &]+", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)
