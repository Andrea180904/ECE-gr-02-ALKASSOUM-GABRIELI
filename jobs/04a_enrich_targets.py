"""Part 3 (step 1/3) - Select the records to enrich (Spark).

Targets:
  * films  : the top-N feature films (by number of votes) of every genre
             and every year in [year-min, year-max]
  * people : the first K principals (IMDb billing order) of each target film

Outputs (Parquet + plain id lists consumed by collectors/enrich_collect.py):
  data/gold/enrich_targets_films, data/gold/enrich_targets_people
  data/enrichment/targets/films/part-*.txt, .../people/part-*.txt

Nothing is hard-coded to the 1980-2025 scope: running with
--year-min 1874 --year-max 2026 --top-n 1000000 targets the whole dataset.

Run with:  ./run.sh spark jobs/04a_enrich_targets.py
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import (  # noqa: E402
    ENRICH_DIR, FEATURE_MIN_RUNTIME, GOLD_DIR, YEAR_MAX, YEAR_MIN,
)
from common.output import save_result  # noqa: E402
from common.spark import feature_movies, get_spark, read_silver  # noqa: E402

EXCLUDED_CATEGORIES = ["archive_footage", "archive_sound"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-min", type=int, default=YEAR_MIN)
    parser.add_argument("--year-max", type=int, default=YEAR_MAX)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--principals-per-film", type=int, default=3)
    args = parser.parse_args()

    spark = get_spark("04a_enrich_targets")
    movies = feature_movies(spark, FEATURE_MIN_RUNTIME).filter(
        F.col("year").between(args.year_min, args.year_max)
        & F.col("numVotes").isNotNull()
    )
    w = Window.partitionBy("year", "genre").orderBy(
        F.desc("numVotes"), "tconst")
    films = (
        movies.select("tconst", "primaryTitle", "year", "numVotes",
                      F.explode("genres").alias("genre"))
        .withColumn("rank_in_genre_year", F.row_number().over(w))
        .filter(F.col("rank_in_genre_year") <= args.top_n)
    )
    films.write.mode("overwrite").parquet(
        str(GOLD_DIR / "enrich_targets_films"))
    films = spark.read.parquet(str(GOLD_DIR / "enrich_targets_films"))
    unique_films = films.select("tconst").distinct()

    principals = read_silver(spark, "title_principals").filter(
        ~F.col("category").isin(*EXCLUDED_CATEGORIES))
    wp = Window.partitionBy("tconst").orderBy("ordering", "nconst")
    people = (
        principals.join(unique_films, "tconst")
        .withColumn("rk", F.row_number().over(wp))
        .filter(F.col("rk") <= args.principals_per_film)
        .select("tconst", "nconst", "ordering", "category")
    )
    people.write.mode("overwrite").parquet(
        str(GOLD_DIR / "enrich_targets_people"))
    people = spark.read.parquet(str(GOLD_DIR / "enrich_targets_people"))

    unique_films.coalesce(1).write.mode("overwrite").text(
        str(ENRICH_DIR / "targets" / "films"))
    people.select("nconst").distinct().coalesce(1).write.mode(
        "overwrite").text(str(ENRICH_DIR / "targets" / "people"))

    summary = spark.createDataFrame(
        [
            ("film x genre x year slots", films.count()),
            ("distinct films", unique_films.count()),
            ("film principals", people.count()),
            ("distinct people", people.select("nconst").distinct().count()),
            ("films with < %d principals" % args.principals_per_film,
             people.groupBy("tconst").count()
             .filter(F.col("count") < args.principals_per_film).count()),
        ],
        "item string, n long",
    )
    save_result(summary, "03_enrichment_targets",
                f"Enrichment scope: top {args.top_n} films per genre per "
                f"year ({args.year_min}-{args.year_max}) and their first "
                f"{args.principals_per_film} principals")
    spark.stop()


if __name__ == "__main__":
    main()
