"""Part 5 - Open question.

Question: do recurring director-actor duos (>= 3 feature films together,
the actor among the top-billed cast) make better films together than
each of them makes apart? Does the effect hold across decades, and does
it fade as the collaboration goes on?

For each duo:
  joint_avg      average rating of their films together
  director_apart average rating of the director's other films
  actor_apart    average rating of the actor's other (lead) films
  lift           joint_avg - (director_apart + actor_apart) / 2

Tables joined: title_basics, title_ratings, title_directors (from
title_crew), title_principals, name_basics.

Run with:  ./run.sh spark jobs/06_open_question.py
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import FEATURE_MIN_RUNTIME  # noqa: E402
from common.output import save_result, save_text  # noqa: E402
from common.spark import feature_movies, get_spark, read_silver  # noqa: E402

MIN_VOTES = 1000      # ratings with fewer votes are too noisy
LEAD_BILLING = 4      # actor must be among the first 4 credited principals
MIN_JOINT = 3         # films together to call it a duo
MIN_APART = 2         # films apart needed (each side) for a baseline


def main() -> None:
    spark = get_spark("06_open_question")
    movies = feature_movies(spark, FEATURE_MIN_RUNTIME).filter(
        F.col("numVotes") >= MIN_VOTES).select(
        "tconst", "primaryTitle", "year", "averageRating", "numVotes")
    names = read_silver(spark, "name_basics").select("nconst", "primaryName")

    directed = read_silver(spark, "title_directors").join(movies, "tconst") \
        .select("tconst", F.col("nconst").alias("director"), "year",
                "averageRating", "numVotes")
    leads = (
        read_silver(spark, "title_principals")
        .filter(F.col("category").isin("actor", "actress")
                & (F.col("ordering") <= LEAD_BILLING))
        .select("tconst", F.col("nconst").alias("actor")).distinct()
        .join(movies, "tconst")
        .select("tconst", "actor", "year", "averageRating")
    )
    joint = directed.join(leads.select("tconst", "actor"), "tconst").filter(
        F.col("director") != F.col("actor")).cache()

    dir_tot = directed.groupBy("director").agg(
        F.count("*").alias("dir_n"), F.sum("averageRating").alias("dir_sum"))
    act_tot = leads.groupBy("actor").agg(
        F.count("*").alias("act_n"), F.sum("averageRating").alias("act_sum"))
    pairs = joint.groupBy("director", "actor").agg(
        F.count("*").alias("n_joint"),
        F.sum("averageRating").alias("joint_sum"),
        F.min("year").alias("first_year"), F.max("year").alias("last_year"),
        F.sum("numVotes").alias("joint_votes"),
    ).filter(F.col("n_joint") >= MIN_JOINT)

    duos = (
        pairs.join(dir_tot, "director").join(act_tot, "actor")
        .withColumn("dir_apart_n", F.col("dir_n") - F.col("n_joint"))
        .withColumn("act_apart_n", F.col("act_n") - F.col("n_joint"))
        .filter((F.col("dir_apart_n") >= MIN_APART)
                & (F.col("act_apart_n") >= MIN_APART))
        .withColumn("joint_avg", F.col("joint_sum") / F.col("n_joint"))
        .withColumn("director_apart",
                    (F.col("dir_sum") - F.col("joint_sum"))
                    / F.col("dir_apart_n"))
        .withColumn("actor_apart",
                    (F.col("act_sum") - F.col("joint_sum"))
                    / F.col("act_apart_n"))
        .withColumn("baseline",
                    (F.col("director_apart") + F.col("actor_apart")) / 2)
        .withColumn("lift", F.col("joint_avg") - F.col("baseline"))
        .withColumn("decade", (F.floor(F.col("first_year") / 10) * 10)
                    .cast("int"))
        .join(names.select(F.col("nconst").alias("director"),
                           F.col("primaryName").alias("director_name")),
              "director")
        .join(names.select(F.col("nconst").alias("actor"),
                           F.col("primaryName").alias("actor_name")),
              "actor")
        .cache()
    )

    # ---- 1. overall effect + sign test ---------------------------------
    s = duos.agg(
        F.count("*").alias("n"),
        F.avg("lift").alias("mean"),
        F.stddev_samp("lift").alias("sd"),
        F.percentile_approx("lift", 0.5).alias("median"),
        F.sum((F.col("lift") > 0).cast("int")).alias("pos"),
        F.avg("joint_avg").alias("joint"), F.avg("baseline").alias("base"),
    ).collect()[0]
    n = s["n"] or 0
    if n:
        se = (s["sd"] or 0) / math.sqrt(n) if n > 1 else float("nan")
        z_sign = (s["pos"] - n / 2) / math.sqrt(n / 4)
        text = (
            "**Open question - overall result**\n\n"
            "| metric | value |\n| --- | --- |\n"
            f"| duos analysed | {n:,} |\n"
            f"| avg rating together | {s['joint']:.3f} |\n"
            f"| avg baseline (apart) | {s['base']:.3f} |\n"
            f"| mean lift | {s['mean']:+.3f} (std. error {se:.3f}) |\n"
            f"| median lift | {s['median']:+.3f} |\n"
            f"| duos with positive lift | {s['pos']:,} "
            f"({100 * s['pos'] / n:.1f}%) |\n"
            f"| sign test z-score (H0: 50% positive) | {z_sign:+.2f} |\n"
        )
    else:
        text = "**Open question - overall result**\n\nNo duo found."
    save_text("05_overall", text)

    # ---- 2. by decade of first collaboration ----------------------------
    save_result(
        duos.groupBy("decade").agg(
            F.count("*").alias("duos"),
            F.round(F.avg("joint_avg"), 3).alias("avg_joint_rating"),
            F.round(F.avg("baseline"), 3).alias("avg_baseline"),
            F.round(F.avg("lift"), 3).alias("mean_lift"),
            F.round(100 * F.avg((F.col("lift") > 0).cast("int")), 1)
            .alias("pct_positive"))
        .orderBy("decade"),
        "05_by_decade", "Lift by decade of the first collaboration")

    # ---- 3. does the effect fade along the collaboration? ---------------
    w = Window.partitionBy("director", "actor").orderBy("year", "tconst")
    seq = (
        joint.join(duos.select("director", "actor", "baseline"),
                   ["director", "actor"])
        .withColumn("collab_index", F.row_number().over(w))
        .withColumn("collab_bucket",
                    F.when(F.col("collab_index") >= 5, "5+")
                    .otherwise(F.col("collab_index").cast("string")))
    )
    save_result(
        seq.groupBy("collab_bucket").agg(
            F.count("*").alias("films"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.avg(F.col("averageRating") - F.col("baseline")), 3)
            .alias("avg_lift"))
        .orderBy("collab_bucket"),
        "05_by_collaboration_rank",
        "Rating of the n-th film made together (relative to the duo's "
        "baseline)")

    # ---- 4. strongest and weakest duos -----------------------------------
    cols = ["director_name", "actor_name", "n_joint", "first_year",
            "last_year", F.round("joint_avg", 2).alias("joint_avg"),
            F.round("director_apart", 2).alias("director_apart"),
            F.round("actor_apart", 2).alias("actor_apart"),
            F.round("lift", 2).alias("lift")]
    save_result(duos.orderBy(F.desc("lift")).select(*cols),
                "05_top_duos", "Duos with the highest lift",
                max_md_rows=15)
    save_result(duos.orderBy("lift").select(*cols),
                "05_bottom_duos", "Duos with the lowest lift",
                max_md_rows=10)
    save_result(duos.orderBy(F.desc("n_joint"), F.desc("joint_votes"))
                .select(*cols),
                "05_most_frequent_duos", "Most frequent duos",
                max_md_rows=15)
    spark.stop()


if __name__ == "__main__":
    main()
